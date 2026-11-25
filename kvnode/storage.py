
"""Per-node storage: in-memory dict + sorted key index + write-ahead log.

Every accepted write is appended to the WAL and flushed before the
in-memory state is updated.

Conflict rule: last-write-wins on (timestamp, origin node id).
Deletes are stored as tombstones.
The sorted key list supports efficient prefix scans.
"""

import bisect
import json
import os
import threading


class Store:
    def __init__(self, path: str, fsync: bool = True):
        self.path = path
        self.fsync = fsync

        self.data = {}
        self.keys = []
        self.lock = threading.Lock()

        self._wal = None

        os.makedirs(
            os.path.dirname(os.path.abspath(path)),
            exist_ok=True
        )

        # Replay existing records and detect an incomplete final record.
        lines, torn = self._replay()

        # Rebuild the WAL if it contains a torn tail or redundant records.
        if torn or lines > len(self.data):
            self._compact()

        # Open the repaired WAL for future writes.
        self._wal = open(path, "a", encoding="utf-8")

    # ---------------------------------------------------------
    # WAL RECOVERY
    # ---------------------------------------------------------

    def _replay(self):
        if not os.path.exists(self.path):
            return 0, False

        count = 0
        torn = False

        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    # Ignore the incomplete record and rebuild the WAL.
                    torn = True
                    break

                self._apply(rec["key"], rec)
                count += 1

        return count, torn

    def _compact(self):
        """Rewrite the WAL using only the latest record for each key."""

        tmp = self.path + ".tmp"

        try:
            with open(tmp, "w", encoding="utf-8") as f:
                for key in self.keys:
                    record = {
                        "key": key,
                        **self.data[key]
                    }

                    f.write(json.dumps(record) + "\n")

                f.flush()
                os.fsync(f.fileno())

            # Atomic replacement after the temporary file is complete.
            os.replace(tmp, self.path)

        finally:
            # Remove a leftover temporary file if replacement failed.
            if os.path.exists(tmp):
                os.remove(tmp)

    # ---------------------------------------------------------
    # INTERNAL STATE MANAGEMENT
    # ---------------------------------------------------------

    def _apply(self, key, rec):
        current = self.data.get(key)

        if current and (
            current["ts"],
            current["origin"]
        ) >= (
            rec["ts"],
            rec["origin"]
        ):
            return False

        if current is None:
            bisect.insort(self.keys, key)

        self.data[key] = {
            "value": rec["value"],
            "ts": rec["ts"],
            "origin": rec["origin"],
            "deleted": rec.get("deleted", False),
        }

        return True

    # ---------------------------------------------------------
    # WRITE OPERATIONS
    # ---------------------------------------------------------

    def put(self, key, value, ts, origin, deleted=False):
        with self.lock:
            if self._wal is None or self._wal.closed:
                raise ValueError("Store is closed")

            current = self.data.get(key)

            # Reject stale writes before touching the WAL.
            if current and (
                current["ts"],
                current["origin"]
            ) >= (ts, origin):
                return False

            record = {
                "key": key,
                "value": value,
                "ts": ts,
                "origin": origin,
                "deleted": deleted,
            }

            # Save the original WAL position so a failed write can
            # be rolled back rather than leaving a broken tail.
            self._wal.seek(0, os.SEEK_END)
            start = self._wal.tell()

            try:
                self._wal.write(json.dumps(record) + "\n")
                self._wal.flush()

                if self.fsync:
                    os.fsync(self._wal.fileno())

            except Exception:
                try:
                    self._wal.seek(start)
                    self._wal.truncate(start)
                    self._wal.flush()

                    if self.fsync:
                        os.fsync(self._wal.fileno())

                except Exception:
                    # Preserve the original write error.
                    pass

                raise

            # Update memory only after the WAL write succeeds.
            self._apply(key, record)

            return True

    # ---------------------------------------------------------
    # READ OPERATIONS
    # ---------------------------------------------------------

    def get(self, key):
        with self.lock:
            record = self.data.get(key)

            if record is None:
                return None

            # Return a copy so callers cannot mutate internal state.
            return dict(record)

    def scan(self, prefix: str, limit: int):
        with self.lock:
            index = bisect.bisect_left(self.keys, prefix)
            result = []

            while (
                index < len(self.keys)
                and self.keys[index].startswith(prefix)
                and len(result) < limit
            ):
                key = self.keys[index]

                result.append({
                    "key": key,
                    **self.data[key]
                })

                index += 1

            return result

    # ---------------------------------------------------------
    # FILE CLEANUP
    # ---------------------------------------------------------

    def close(self):
        """Flush and close the WAL safely."""

        with self.lock:
            if self._wal is not None and not self._wal.closed:
                self._wal.flush()
                self._wal.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
