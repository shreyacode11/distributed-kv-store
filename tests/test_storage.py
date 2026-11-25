
import json

from kvnode.storage import Store


def mk(tmp_path):
    return Store(str(tmp_path / "n.wal"), fsync=False)


def test_last_write_wins_and_ignores_stale(tmp_path):
    with mk(tmp_path) as s:
        assert s.put("a", "v1", 10, "n1")
        assert s.put("a", "v2", 20, "n1")
        assert not s.put("a", "old", 15, "n1")

        assert s.get("a")["value"] == "v2"


def test_tie_broken_by_origin(tmp_path):
    with mk(tmp_path) as s:
        assert s.put("a", "x", 10, "n1")
        assert s.put("a", "y", 10, "n2")

        assert s.get("a")["value"] == "y"


def test_tombstone_blocks_stale_resurrection(tmp_path):
    with mk(tmp_path) as s:
        s.put("a", "v", 10, "n1")
        s.put("a", None, 20, "n1", deleted=True)
        s.put("a", "v", 10, "n1")

        assert s.get("a")["deleted"] is True


def test_recovers_from_wal(tmp_path):
    with mk(tmp_path) as s:
        s.put("a", "1", 1, "n1")
        s.put("b", "2", 2, "n1")
        s.put("a", "3", 3, "n1")

    with mk(tmp_path) as s2:
        assert s2.get("a")["value"] == "3"
        assert s2.get("b")["value"] == "2"


def test_survives_torn_tail(tmp_path):
    with mk(tmp_path) as s:
        s.put("a", "1", 1, "n1")

    with open(tmp_path / "n.wal", "a", encoding="utf-8") as f:
        f.write('{"key": "b", "val')

    with mk(tmp_path) as s2:
        assert s2.get("a")["value"] == "1"
        assert s2.get("b") is None


def test_write_after_torn_tail_survives_restart(tmp_path):
    # First, create a valid WAL.
    with mk(tmp_path) as s:
        s.put("a", "1", 1, "n1")

    # Simulate a crash during the next record.
    with open(tmp_path / "n.wal", "a", encoding="utf-8") as f:
        f.write('{"key": "broken')

    # Recovery must repair the WAL before accepting new writes.
    with mk(tmp_path) as s:
        assert s.get("a")["value"] == "1"
        assert s.put("b", "2", 2, "n1")

    # A second restart must recover both valid records.
    with mk(tmp_path) as s:
        assert s.get("a")["value"] == "1"
        assert s.get("b")["value"] == "2"


def test_compaction_shrinks_log(tmp_path):
    with mk(tmp_path) as s:
        for i in range(50):
            s.put("a", str(i), i + 1, "n1")

    # Reopening triggers compaction.
    with mk(tmp_path):
        pass

    lines = (tmp_path / "n.wal").read_text(
        encoding="utf-8"
    ).strip().splitlines()

    assert len(lines) == 1
    assert json.loads(lines[0])["value"] == "49"


def test_prefix_scan_sorted_and_limited(tmp_path):
    with mk(tmp_path) as s:
        for key in ["user:3", "user:1", "order:1", "user:2", "usez"]:
            s.put(key, "v", 1, "n1")

        assert [
            record["key"] for record in s.scan("user:", 10)
        ] == ["user:1", "user:2", "user:3"]

        assert len(s.scan("user:", 2)) == 2


def test_get_returns_copy(tmp_path):
    with mk(tmp_path) as s:
        s.put("a", "original", 1, "n1")

        record = s.get("a")
        record["value"] = "modified"

        assert s.get("a")["value"] == "original"
