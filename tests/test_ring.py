from collections import Counter
from kvnode.ring import HashRing

NODES = [f"node{i}" for i in range(1, 6)]


def test_preference_list_is_distinct_and_sized():
    ring = HashRing(NODES)
    for k in range(500):
        prefs = ring.preference_list(f"key-{k}", 3)
        assert len(prefs) == 3 and len(set(prefs)) == 3


def test_deterministic():
    a, b = HashRing(NODES), HashRing(NODES)
    assert all(a.preference_list(f"k{i}", 3) == b.preference_list(f"k{i}", 3) for i in range(200))


def test_even_distribution_of_primaries():
    ring = HashRing(NODES)
    counts = Counter(ring.preference_list(f"key-{i}", 1)[0] for i in range(20000))
    mean = 20000 / len(NODES)
    assert all(abs(c - mean) / mean < 0.30 for c in counts.values()), counts


def test_removing_a_node_only_moves_its_keys():
    full, reduced = HashRing(NODES), HashRing(NODES[:-1])
    moved = 0
    for i in range(10000):
        k = f"key-{i}"
        before, after = full.preference_list(k, 1)[0], reduced.preference_list(k, 1)[0]
        if before != "node5":
            assert before == after  # keys of surviving nodes never move
        else:
            moved += 1
    assert moved < 10000 * 0.35
