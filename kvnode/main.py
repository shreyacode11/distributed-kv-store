"""One node of the distributed key-value store.

Any node can coordinate any request (Dynamo-style, leaderless):
  * the key is hashed onto the ring -> an ordered preference list of N replicas
  * WRITE: send to all N replicas in parallel, succeed once W acknowledge
  * READ : ask all N replicas, return once R answer, pick the newest version,
           then repair stale replicas in the background (read repair)
With N=3, W=2, R=2 we have R + W > N, so every read quorum overlaps every
write quorum, and the cluster tolerates one replica being down per key.
"""
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .ring import HashRing
from .storage import Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("kv")

NODE_ID = os.environ.get("NODE_ID", "node1")
PEERS = dict(
    p.split("=", 1)
    for p in os.environ.get("PEERS", f"{NODE_ID}=http://127.0.0.1:8000").split(",")
)
N = int(os.getenv("N", "3"))
W = int(os.getenv("W", "2"))
R = int(os.getenv("R", "2"))
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT", "0.8"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2"))
MAX_KEY_LEN = 256

if not (1 <= W <= N and 1 <= R <= N and N <= len(PEERS)):
    raise ValueError(f"invalid config: N={N} W={W} R={R} peers={len(PEERS)}")

store = Store(
    os.path.join(os.getenv("DATA_DIR", "./data"), f"{NODE_ID}.wal"),
    fsync=os.getenv("FSYNC", "1") == "1",
)
ring = HashRing(list(PEERS))
alive = {p: True for p in PEERS}
client: httpx.AsyncClient | None = None
_bg: set = set()


def spawn(coro):
    """Run a coroutine in the background, keeping a reference and swallowing errors."""
    t = asyncio.create_task(coro)
    _bg.add(t)
    t.add_done_callback(_bg.discard)
    t.add_done_callback(lambda t: t.cancelled() or t.exception())
    return t


async def heartbeat():
    async def ping(node, url):
        try:
            r = await client.get(f"{url}/health", timeout=0.5)
            up = r.status_code == 200
        except Exception:
            up = False
        if alive.get(node) != up:
            log.info("peer %s is now %s", node, "UP" if up else "DOWN")
        alive[node] = up

    while True:
        await asyncio.gather(*(ping(n, u) for n, u in PEERS.items() if n != NODE_ID))
        await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    global client
    client = httpx.AsyncClient(timeout=RPC_TIMEOUT)
    hb = asyncio.create_task(heartbeat())
    log.info("%s up: N=%d W=%d R=%d peers=%s", NODE_ID, N, W, R, list(PEERS))
    yield
    hb.cancel()
    await client.aclose()


app = FastAPI(title=f"distkv {NODE_ID}", lifespan=lifespan)


# --------------------------------------------------------------------------
# replica RPCs (local call if the replica is this node, HTTP otherwise)
# --------------------------------------------------------------------------
async def replica_put(node, key, value, ts, origin, deleted):
    if node == NODE_ID:
        await asyncio.to_thread(store.put, key, value, ts, origin, deleted)
        return node, True
    r = await client.put(
        f"{PEERS[node]}/internal/kv/{key}",
        json={"value": value, "ts": ts, "origin": origin, "deleted": deleted},
    )
    r.raise_for_status()
    return node, True


async def replica_get(node, key):
    if node == NODE_ID:
        return node, {"rec": store.get(key)}
    r = await client.get(f"{PEERS[node]}/internal/kv/{key}")
    r.raise_for_status()
    return node, r.json()


async def wait_for(tasks, need):
    """Wait until `need` tasks succeed (or all finish). Returns (results, still_pending)."""
    pending, ok = set(tasks), []
    while pending and len(ok) < need:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for d in done:
            if d.exception() is None:
                ok.append(d.result())
    return ok, pending


def version(rec):
    return (rec["ts"], rec["origin"])


async def read_repair(key, best, responses, pending):
    late = await asyncio.gather(*pending, return_exceptions=True)
    responses = responses + [x for x in late if not isinstance(x, BaseException)]
    for node, resp in responses:
        rec = resp["rec"]
        if rec is None or version(rec) < version(best):
            try:
                await replica_put(node, key, best["value"], best["ts"], best["origin"], best["deleted"])
                log.info("read-repair %s -> %s", key, node)
            except Exception:
                pass


def check_key(key):
    if not key or len(key) > MAX_KEY_LEN:
        raise HTTPException(400, f"key must be 1..{MAX_KEY_LEN} chars")


# --------------------------------------------------------------------------
# public API
# --------------------------------------------------------------------------
class PutBody(BaseModel):
    value: str


async def write(key, value, deleted):
    check_key(key)
    ts = time.time_ns()
    prefs = ring.preference_list(key, N)
    tasks = [spawn(replica_put(n, key, value, ts, NODE_ID, deleted)) for n in prefs]
    ok, _ = await wait_for(tasks, W)
    if len(ok) < W:
        raise HTTPException(503, f"write quorum not met ({len(ok)}/{W} acks)")
    return {"key": key, "ts": ts, "coordinator": NODE_ID, "replicas": prefs, "acks": len(ok)}


@app.put("/kv/{key:path}")
async def put_key(key: str, body: PutBody):
    return await write(key, body.value, False)


@app.delete("/kv/{key:path}")
async def delete_key(key: str):
    return await write(key, None, True)


@app.get("/kv/{key:path}")
async def get_key(key: str):
    check_key(key)
    prefs = ring.preference_list(key, N)
    tasks = [spawn(replica_get(n, key)) for n in prefs]
    ok, pending = await wait_for(tasks, R)
    if len(ok) < R:
        raise HTTPException(503, f"read quorum not met ({len(ok)}/{R} replies)")
    recs = [resp["rec"] for _, resp in ok if resp["rec"] is not None]
    if not recs:
        raise HTTPException(404, "key not found")
    best = max(recs, key=version)
    spawn(read_repair(key, best, list(ok), pending))
    if best["deleted"]:
        raise HTTPException(404, "key not found")
    return {"key": key, "value": best["value"], "ts": best["ts"],
            "coordinator": NODE_ID, "replicas": prefs}


@app.get("/scan")
async def scan(prefix: str = "", limit: int = 100):
    """Prefix/range query: scatter to every node, merge by newest version."""
    limit = max(1, min(limit, 1000))

    async def one(node):
        if node == NODE_ID:
            return store.scan(prefix, limit)
        r = await client.get(f"{PEERS[node]}/internal/scan", params={"prefix": prefix, "limit": limit})
        r.raise_for_status()
        return r.json()["items"]

    results = await asyncio.gather(*(one(n) for n in PEERS), return_exceptions=True)
    failed = sum(isinstance(r, BaseException) for r in results)
    merged = {}
    for items in results:
        if isinstance(items, BaseException):
            continue
        for it in items:
            cur = merged.get(it["key"])
            if cur is None or version(it) > version(cur):
                merged[it["key"]] = it
    live = sorted((v for v in merged.values() if not v["deleted"]), key=lambda v: v["key"])[:limit]
    return {
        "prefix": prefix,
        "count": len(live),
        "items": [{"key": v["key"], "value": v["value"]} for v in live],
        "nodes_queried": len(PEERS),
        "nodes_failed": failed,
        # every key lives on N nodes, so fewer than N failures still sees every key
        "complete": failed < N,
    }


# --------------------------------------------------------------------------
# internal + ops endpoints
# --------------------------------------------------------------------------
class Record(BaseModel):
    value: str | None = None
    ts: int
    origin: str
    deleted: bool = False


@app.put("/internal/kv/{key:path}")
def internal_put(key: str, rec: Record):
    return {"applied": store.put(key, rec.value, rec.ts, rec.origin, rec.deleted)}


@app.get("/internal/kv/{key:path}")
def internal_get(key: str):
    return {"rec": store.get(key)}


@app.get("/internal/scan")
def internal_scan(prefix: str = "", limit: int = 1000):
    return {"items": store.scan(prefix, limit)}


@app.get("/health")
def health():
    return {"node": NODE_ID, "status": "ok"}


@app.get("/cluster/status")
def cluster_status():
    return {"node": NODE_ID, "N": N, "W": W, "R": R, "keys_stored": len(store.data),
            "peers": {n: {"url": u, "alive": alive[n] if n != NODE_ID else True} for n, u in PEERS.items()}}


@app.get("/cluster/locate/{key:path}")
def locate(key: str):
    return {"key": key, "replicas": ring.preference_list(key, N)}
