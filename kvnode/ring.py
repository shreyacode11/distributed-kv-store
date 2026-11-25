"""Consistent-hash ring with virtual nodes.

Each physical node is placed on the ring `vnodes` times so keys spread evenly,
and adding/removing a node only moves ~1/N of the keys.
"""
import bisect
import hashlib


def _hash(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:8], "big")


class HashRing:
    def __init__(self, nodes, vnodes: int = 128):
        self._points = sorted(
            (_hash(f"{node}#{i}"), node) for node in nodes for i in range(vnodes)
        )
        self._hashes = [h for h, _ in self._points]

    def preference_list(self, key: str, n: int):
        """First `n` distinct physical nodes clockwise from the key's position."""
        start = bisect.bisect(self._hashes, _hash(key))
        out = []
        for i in range(len(self._points)):
            node = self._points[(start + i) % len(self._points)][1]
            if node not in out:
                out.append(node)
                if len(out) == n:
                    break
        return out
