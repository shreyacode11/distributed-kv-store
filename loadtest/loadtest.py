"""Tiny async load generator: throughput + latency percentiles.

  python loadtest/loadtest.py --nodes http://localhost:8001,http://localhost:8002 \
      --duration 15 --concurrency 32 --write-ratio 0.3

Spreads requests across the given coordinator nodes. To test with a node down,
stop it and pass only the nodes that are still up (like a real client would).
"""
import argparse
import asyncio
import random
import time

import httpx


def pct(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))] if sorted_vals else float("nan")


async def worker(client, urls, deadline, write_ratio, keyspace, lat, errors):
    while time.perf_counter() < deadline:
        url, key = random.choice(urls), f"load:{random.randrange(keyspace)}"
        t = time.perf_counter()
        try:
            if random.random() < write_ratio:
                r = await client.put(f"{url}/kv/{key}", json={"value": "x" * 64})
            else:
                r = await client.get(f"{url}/kv/{key}")
            ok = r.status_code in (200, 404)
        except httpx.HTTPError:
            ok = False
        if ok:
            lat.append((time.perf_counter() - t) * 1000)
        else:
            errors.append(1)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", default="http://localhost:8001,http://localhost:8002,http://localhost:8003")
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--write-ratio", type=float, default=0.3)
    ap.add_argument("--keyspace", type=int, default=2000)
    a = ap.parse_args()
    urls = a.nodes.split(",")

    limits = httpx.Limits(max_connections=a.concurrency * 2)
    async with httpx.AsyncClient(timeout=5, limits=limits) as client:
        # preload so reads hit real data
        sem = asyncio.Semaphore(32)

        async def load(i):
            async with sem:
                await client.put(f"{urls[0]}/kv/load:{i}", json={"value": "x" * 64})

        await asyncio.gather(*(load(i) for i in range(a.keyspace)))

        lat, errors = [], []
        start = time.perf_counter()
        deadline = start + a.duration
        await asyncio.gather(*(worker(client, urls, deadline, a.write_ratio, a.keyspace, lat, errors)
                               for _ in range(a.concurrency)))
        elapsed = time.perf_counter() - start

    lat.sort()
    total = len(lat) + len(errors)
    print(f"nodes={len(urls)} concurrency={a.concurrency} write_ratio={a.write_ratio} duration={elapsed:.1f}s")
    print(f"requests={total}  ok={len(lat)}  errors={len(errors)}  throughput={len(lat) / elapsed:.0f} ops/s")
    print(f"latency ms: p50={pct(lat, .50):.1f}  p95={pct(lat, .95):.1f}  p99={pct(lat, .99):.1f}")


if __name__ == "__main__":
    asyncio.run(main())
