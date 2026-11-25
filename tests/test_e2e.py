"""End-to-end: boots a real 5-node cluster (separate OS processes) and kills nodes."""
import os
import subprocess
import sys
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e
BASE = 18000
IDS = [f"node{i}" for i in range(1, 6)]
URL = {n: f"http://127.0.0.1:{BASE + i}" for i, n in enumerate(IDS, 1)}


class Cluster:
    def __init__(self, data_dir):
        self.data_dir, self.procs = str(data_dir), {}
        self.http = httpx.Client(timeout=3)

    def start(self, node):
        env = {**os.environ, "NODE_ID": node, "DATA_DIR": self.data_dir, "FSYNC": "0",
               "HEARTBEAT_INTERVAL": "1", "RPC_TIMEOUT": "0.5",
               "PEERS": ",".join(f"{n}={u}" for n, u in URL.items())}
        port = URL[node].rsplit(":", 1)[1]
        self.procs[node] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "kvnode.main:app", "--port", port, "--log-level", "warning"],
            env=env)
        for _ in range(100):
            try:
                if self.http.get(f"{URL[node]}/health").status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
        raise RuntimeError(f"{node} did not start")

    def kill(self, node):
        self.procs[node].kill()
        self.procs[node].wait()

    def stop_all(self):
        for p in self.procs.values():
            p.kill()
            p.wait()


@pytest.fixture()
def cluster(tmp_path):
    c = Cluster(tmp_path)
    for n in IDS:
        c.start(n)
    yield c
    c.stop_all()


def replicas(c, key):
    return c.http.get(f"{URL['node1']}/cluster/locate/{key}").json()["replicas"]


def test_any_node_can_coordinate(cluster):
    http = cluster.http
    assert http.put(f"{URL['node1']}/kv/color", json={"value": "blue"}).status_code == 200
    for n in IDS:
        r = http.get(f"{URL[n]}/kv/color")
        assert r.status_code == 200 and r.json()["value"] == "blue"
    assert http.get(f"{URL['node2']}/kv/missing").status_code == 404


def test_delete(cluster):
    http = cluster.http
    http.put(f"{URL['node1']}/kv/tmp", json={"value": "x"})
    assert http.delete(f"{URL['node3']}/kv/tmp").status_code == 200
    assert http.get(f"{URL['node4']}/kv/tmp").status_code == 404


def test_survives_one_replica_down_and_repairs_it(cluster):
    http, key = cluster.http, "fault-key"
    a, b, c = replicas(cluster, key)
    http.put(f"{URL[b]}/kv/{key}", json={"value": "v1"})

    cluster.kill(a)  # one of the three replicas dies
    assert http.put(f"{URL[b]}/kv/{key}", json={"value": "v2"}).status_code == 200
    assert http.get(f"{URL[c]}/kv/{key}").json()["value"] == "v2"

    cluster.start(a)  # comes back with only its WAL (has v1, missed v2)
    assert http.get(f"{URL[a]}/internal/kv/{key}").json()["rec"]["value"] == "v1"
    assert http.get(f"{URL[b]}/kv/{key}").json()["value"] == "v2"  # triggers read repair
    for _ in range(50):
        if http.get(f"{URL[a]}/internal/kv/{key}").json()["rec"]["value"] == "v2":
            break
        time.sleep(0.1)
    else:
        pytest.fail("read repair did not update the restarted replica")


def test_quorum_lost_returns_503_not_stale_data(cluster):
    http, key = cluster.http, "quorum-key"
    a, b, c = replicas(cluster, key)
    http.put(f"{URL[c]}/kv/{key}", json={"value": "v1"})
    cluster.kill(a)
    cluster.kill(b)
    assert http.put(f"{URL[c]}/kv/{key}", json={"value": "v2"}).status_code == 503
    assert http.get(f"{URL[c]}/kv/{key}").status_code == 503


def test_prefix_scan_with_a_node_down(cluster):
    http = cluster.http
    for i in range(20):
        http.put(f"{URL['node1']}/kv/user:{i:02d}", json={"value": str(i)})
    http.put(f"{URL['node1']}/kv/order:1", json={"value": "o"})
    cluster.kill("node5")
    r = http.get(f"{URL['node1']}/scan", params={"prefix": "user:", "limit": 100}).json()
    assert r["count"] == 20 and r["complete"] is True and r["nodes_failed"] == 1
    assert [i["key"] for i in r["items"]] == sorted(i["key"] for i in r["items"])
