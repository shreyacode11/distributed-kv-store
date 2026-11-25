"""Per-node storage: in-memory dict + sorted key index + write-ahead log."""

import bisect
import json
import os
import threading


class Store:
    def __init__(
        self,
        path: str,
        fsync: bool = True,
        compact_threshold: int = 1000,
    ):
        if compact_threshold < 1:
            raise ValueError("compact_threshold must be at least 1")

        self.path = path
        self.fsync = fsync
        self.compact_threshold = compact_threshold

        self.data = {}
        self.keys = []
        self.lock = threading.Lock()
        self._wal = None
        self._wal_records = 0

        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

        lines, torn = self._replay()

        if torn or lines > len(self.data):
            self._compact()

        self._wal_records = len(self.data)
        self._wal = open(path, "a", encoding="utf-8")

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
                    self._apply(rec["key"], rec)
                    count += 1
                except (json.JSONDecodeError, KeyError, TypeError):
                    torn = True
                    break

        return count, torn

    def _compact(self):
        """Rewrite the WAL with the latest record for each key."""
        tmp = self.path + ".tmp"

        try:
            with open(tmp, "w", encoding="utf-8") as f:
                for key in self.keys:
                    record = {"key": key, **self.data[key]}
                    f.write(json.dumps(record) + "\n")

                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def _compact_runtime(self):
        """Compact safely while the store is running."""
        self._wal.flush()

        if self.fsync:
            os.fsync(self._wal.fileno())

        self._wal.close()
        self._wal = None

        try:
            self._compact()
        finally:
            # Reopen the WAL even if compaction fails.
            self._wal = open(self.path, "a", encoding="utf-8")

        self._wal_records = len(self.data)

    def _apply(self, key, rec):
        current = self.data.get(key)

        if current and (
            current["ts"], current["origin"]
        ) >= (
            rec["ts"], rec["origin"]
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

    def put(self, key, value, ts, origin, deleted=False):
        with self.lock:
            if self._wal is None or self._wal.closed:
                raise ValueError("Store is closed")

            current = self.data.get(key)

            if current and (
                current["ts"], current["origin"]
            ) >= (ts, origin):
                return False

            record = {
                "key": key,
                "value": value,
                "ts": ts,
                "origin": origin,
                "deleted": deleted,
            }

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
                    pass
                raise

            self._apply(key, record)
            self._wal_records += 1

            if self._wal_records >= self.compact_threshold:
                try:
                    self._compact_runtime()
                except OSError:
                    # The write is already durable. Leave the WAL intact
                    # and retry compaction after a future successful write.
                    pass

            return True

    def get(self, key):
        with self.lock:
            record = self.data.get(key)
            return dict(record) if record is not None else None

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
                result.append({"key": key, **self.data[key]})
                index += 1

            return result

    def close(self):
        with self.lock:
            if self._wal is not None and not self._wal.closed:
                self._wal.flush()
                self._wal.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False