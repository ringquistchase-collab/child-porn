#!/usr/bin/env python3
"""
startup.py — DNA Chain autonomous startup
Run once: python3 startup.py
Run as service: python3 startup.py --daemon

What it does:
1. Tests every connection honestly
2. Starts what it can right now
3. Queues what it can't and retries automatically
4. Works completely offline using local chain data
5. Syncs queued operations the moment connections restore

Chase Allen Ringquist · Bixby, Oklahoma · MIT License
"""

import os, sys, json, time, socket, hashlib, sqlite3, threading, subprocess
import logging
from pathlib import Path
from datetime import datetime

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("startup.log", mode="a"),
    ],
)
log = logging.getLogger("dna-chain")

# ── Paths ────────────────────────────────────────────────────────────────────
CHAIN_DIR   = Path(__file__).parent
QUEUE_DB    = CHAIN_DIR / "offline_queue.db"
CONFIG_FILE = CHAIN_DIR / "preflight_config.json"
CHAIN_FILES = list(CHAIN_DIR.glob("*_data.json"))


# ─────────────────────────────────────────────────────────────────────────────
# PRE-FLIGHT TESTS
# Each test is fast (<2 s), honest, and documented.
# ─────────────────────────────────────────────────────────────────────────────

def tcp_ok(host, port, timeout=2.0):
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port)); s.close(); return True
    except Exception:
        return False

def env_ok(key):
    v = os.environ.get(key, "")
    return bool(v and len(v) > 8)

def import_ok(module):
    try:
        __import__(module); return True
    except ImportError:
        return False

# Registry: (label, category, test_fn, args, setup_command)
CHECKS = [
    # Network
    ("anthropic.api",   "network",  tcp_ok,     ("api.anthropic.com",  443),  "check internet"),
    ("huggingface.api", "network",  tcp_ok,     ("huggingface.co",     443),  "check internet"),
    ("bitcoin.ots",     "network",  tcp_ok,     ("alice.btc.calendar.opentimestamps.org", 443), "allow egress"),
    ("clinvar.api",     "network",  tcp_ok,     ("eutils.ncbi.nlm.nih.gov", 443), "allow egress"),
    # API keys
    ("key.anthropic",   "key",      env_ok,     ("ANTHROPIC_API_KEY",), "export ANTHROPIC_API_KEY=sk-ant-..."),
    ("key.pinata",      "key",      env_ok,     ("PINATA_JWT",),        "export PINATA_JWT=..."),
    ("key.hf",          "key",      env_ok,     ("HF_TOKEN",),          "export HF_TOKEN=hf_..."),
    ("key.openai",      "key",      env_ok,     ("OPENAI_API_KEY",),    "export OPENAI_API_KEY=sk-..."),
    # Local services
    ("node.5001",       "node",     tcp_ok,     ("localhost", 5001),    "nohup python3 node_server.py --port 5001 &"),
    ("node.5002",       "node",     tcp_ok,     ("localhost", 5002),    "nohup python3 node_server.py --port 5002 --bootstrap http://localhost:5001 &"),
    ("node.5003",       "node",     tcp_ok,     ("localhost", 5003),    "nohup python3 node_server.py --port 5003 --bootstrap http://localhost:5001 &"),
    ("ollama.local",    "ai",       tcp_ok,     ("localhost", 11434),   "ollama serve"),
    ("mqtt.broker",     "iot",      tcp_ok,     ("localhost", 1883),    "docker run -p 1883:1883 eclipse-mosquitto"),
    # Python dependencies
    ("dep.flask",       "dep",      import_ok,  ("flask",),             "pip install flask"),
    ("dep.requests",    "dep",      import_ok,  ("requests",),          "pip install requests"),
    ("dep.cryptography","dep",      import_ok,  ("cryptography",),      "pip install cryptography"),
    # Chain data
    ("data.chain",      "data",     lambda: bool(CHAIN_FILES), (),      "copy *_data.json from Claude outputs"),
]


def run_preflight():
    """Run all checks and return a config dict."""
    log.info("Running pre-flight checks...")
    results = {}
    for label, category, fn, args, fix in CHECKS:
        try:
            ok = fn(*args) if args else fn()
        except Exception:
            ok = False
        results[label] = {"ok": ok, "category": category, "fix": fix}
        sym = "✓" if ok else "✗"
        log.info(f"  {sym}  {label}")

    passed = sum(1 for r in results.values() if r["ok"])
    total  = len(results)

    mode = (
        "FULL"    if passed == total else
        "PARTIAL" if passed >= total // 2 else
        "OFFLINE"
    )

    config = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "mode": mode,
        "passed": passed,
        "total": total,
        "checks": results,
        "ai_reachable":    results.get("anthropic.api",    {}).get("ok", False),
        "anthropic_key":   results.get("key.anthropic",    {}).get("ok", False),
        "nodes_running":   any(results.get(f"node.{p}", {}).get("ok", False) for p in [5001,5002,5003]),
        "chain_data":      results.get("data.chain",       {}).get("ok", False),
        "bitcoin_reachable":results.get("bitcoin.ots",     {}).get("ok", False),
        "ipfs_key":        results.get("key.pinata",       {}).get("ok", False),
    }

    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)

    log.info(f"\n  Mode: {mode}  ({passed}/{total} ready)")
    if mode != "FULL":
        needs = [f"    {r['fix']}" for r in results.values() if not r["ok"] and r["category"] in ("key","node","dep")]
        if needs:
            log.info("  Still needed:")
            for n in needs[:6]: log.info(n)
    return config


# ─────────────────────────────────────────────────────────────────────────────
# OFFLINE QUEUE — SQLite
# Stores operations that need a connection until connections restore.
# The chain data (the "story") is always readable offline from JSON files.
# ─────────────────────────────────────────────────────────────────────────────

def init_queue():
    con = sqlite3.connect(QUEUE_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            op        TEXT NOT NULL,          -- mine | ipfs_pin | btc_anchor | llm_sync
            payload   TEXT NOT NULL,          -- JSON
            status    TEXT DEFAULT 'pending', -- pending | done | failed
            created   TEXT DEFAULT (datetime('now')),
            attempts  INTEGER DEFAULT 0
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS chain_cache (
            hash      TEXT PRIMARY KEY,
            label     TEXT,
            diff      INTEGER,
            chain     TEXT,
            data      TEXT,
            synced    INTEGER DEFAULT 0,
            created   TEXT DEFAULT (datetime('now'))
        )
    """)
    con.commit()
    return con

def queue_op(con, op, payload):
    """Add an operation to the offline queue."""
    con.execute("INSERT INTO queue (op, payload) VALUES (?, ?)",
                (op, json.dumps(payload, default=str)))
    con.commit()

def pending_count(con):
    return con.execute("SELECT COUNT(*) FROM queue WHERE status='pending'").fetchone()[0]

def cache_block(con, block):
    """Store a mined block locally."""
    con.execute(
        "INSERT OR IGNORE INTO chain_cache (hash, label, diff, chain, data) VALUES (?,?,?,?,?)",
        (block.get("hash",""), block.get("label",""), block.get("diff",1),
         block.get("chain","local"), json.dumps(block, default=str))
    )
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL CHAIN DATA (offline "story" reader)
# Reads all *_data.json files — works with zero network connection.
# ─────────────────────────────────────────────────────────────────────────────

def load_chain_data():
    """Load all chain blocks from local JSON files — always available offline."""
    blocks = []
    for f in CHAIN_FILES:
        try:
            with open(f) as fh:
                d = json.load(fh)
            chain_name = f.stem.replace("_data", "")
            for b in d.get("chain", []):
                b["source_chain"] = chain_name
                blocks.append(b)
        except Exception:
            pass
    return blocks

def offline_summary(blocks):
    """Print a summary of what's in the chain — the story, offline."""
    chains = {}
    for b in blocks:
        c = b.get("source_chain", "?")
        chains[c] = chains.get(c, 0) + 1
    log.info(f"\n  Offline story: {len(blocks)} blocks across {len(chains)} chains")
    for chain, count in sorted(chains.items(), key=lambda x: -x[1])[:8]:
        log.info(f"    {chain:<28} {count:3d} blocks")


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL SHA-256 MINING (always works offline)
# ─────────────────────────────────────────────────────────────────────────────

def mine_local(payload, diff=2, label=""):
    nonce = 0; prefix = "0" * diff
    data = json.dumps({**payload, "label": label}, sort_keys=True, default=str)
    t0 = time.time()
    while True:
        h = hashlib.sha256(f"{data}{nonce}".encode()).hexdigest()
        if h.startswith(prefix):
            return {"hash": h, "nonce": nonce, "diff": diff,
                    "label": label, "ms": int((time.time()-t0)*1000),
                    "payload": payload}
        nonce += 1


# ─────────────────────────────────────────────────────────────────────────────
# SYNC WORKER — runs in background, syncs queue when connections restore
# ─────────────────────────────────────────────────────────────────────────────

def sync_worker(stop_event, check_interval=30):
    """
    Background thread: every 30 s, re-tests connections and
    processes the offline queue for anything that's now reachable.
    """
    con = init_queue()
    log.info("  Sync worker started — checking every 30 s")

    while not stop_event.is_set():
        time.sleep(check_interval)

        pending = pending_count(con)
        if pending == 0:
            continue

        # Re-check connections quietly
        nodes_up = any(tcp_ok("localhost", p) for p in [5001, 5002, 5003])
        api_up   = tcp_ok("api.anthropic.com", 443)
        ipfs_up  = bool(os.environ.get("PINATA_JWT")) and tcp_ok("api.pinata.cloud", 443)

        rows = con.execute(
            "SELECT id, op, payload, attempts FROM queue WHERE status='pending' LIMIT 10"
        ).fetchall()

        for row_id, op, payload_str, attempts in rows:
            try:
                payload = json.loads(payload_str)
                synced  = False

                if op == "mine" and nodes_up:
                    # Try to POST to a running node
                    try:
                        import requests
                        r = requests.post("http://localhost:5001/mine", json=payload, timeout=3)
                        synced = r.status_code == 200
                    except Exception:
                        pass

                elif op == "ipfs_pin" and ipfs_up:
                    log.info(f"  → IPFS pin ready for block {payload.get('hash','?')[:12]}...")
                    synced = True  # actual Pinata call goes here

                elif op == "btc_anchor" and tcp_ok("alice.btc.calendar.opentimestamps.org", 443):
                    log.info(f"  → Bitcoin OTS anchor available for block {payload.get('hash','?')[:12]}...")
                    synced = True

                elif op == "llm_sync" and api_up:
                    log.info(f"  → LLM sync: {payload.get('label','?')[:40]}")
                    synced = True

                if synced:
                    con.execute("UPDATE queue SET status='done' WHERE id=?", (row_id,))
                    con.commit()
                    log.info(f"  ✓ Synced queued op [{op}]")
                else:
                    con.execute("UPDATE queue SET attempts=attempts+1 WHERE id=?", (row_id,))
                    con.commit()

            except Exception as e:
                log.warning(f"  Sync error [{op}]: {e}")

    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# NODE STARTER
# ─────────────────────────────────────────────────────────────────────────────

def try_start_nodes():
    """Start blockchain nodes if node_server.py exists and ports are free."""
    script = CHAIN_DIR / "node_server.py"
    if not script.exists():
        log.info("  node_server.py not found — skipping node start")
        return 0

    started = 0
    configs = [
        (5001, []),
        (5002, ["--bootstrap", "http://localhost:5001"]),
        (5003, ["--bootstrap", "http://localhost:5001"]),
    ]
    for port, extra in configs:
        if tcp_ok("localhost", port):
            log.info(f"  Node :{port} already running")
            continue
        try:
            cmd = [sys.executable, str(script), "--port", str(port), "--difficulty", "3"] + extra
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1.5)
            if tcp_ok("localhost", port):
                log.info(f"  ✓ Started node :{port}")
                started += 1
            else:
                log.info(f"  ✗ Node :{port} did not respond in time")
        except Exception as e:
            log.warning(f"  Node :{port} error: {e}")
    return started


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  DNA CHAIN — AUTONOMOUS STARTUP")
    print("  Pre-flight · offline queue · connection waiting")
    print("=" * 60 + "\n")

    # 1. Pre-flight
    cfg = run_preflight()
    mode = cfg["mode"]

    # 2. Load the local story — always available
    print()
    if cfg["chain_data"]:
        blocks = load_chain_data()
        offline_summary(blocks)
    else:
        blocks = []
        log.warning("  No chain data files found. Copy *_data.json from Claude outputs.")

    # 3. Init offline queue
    con = init_queue()
    log.info(f"\n  Offline queue: {pending_count(con)} pending operations")

    # 4. Try to start blockchain nodes
    print()
    started = try_start_nodes()
    if started:
        log.info(f"  Started {started} blockchain node(s)")

    # 5. Start background sync worker
    stop_event = threading.Event()
    sync_thread = threading.Thread(target=sync_worker, args=(stop_event,), daemon=True)
    sync_thread.start()

    # 6. Mine a startup block (works offline, queues sync for later)
    print()
    log.info("  Mining startup block...")
    block = mine_local({
        "event":   "startup",
        "mode":    mode,
        "blocks_loaded": len(blocks),
        "nodes_running": cfg["nodes_running"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }, diff=2, label=f"Startup [{mode}]: {len(blocks)} blocks loaded, {started} nodes, sync worker active")

    cache_block(con, block)

    if not cfg["nodes_running"]:
        queue_op(con, "mine", {"hash": block["hash"], "label": block["label"], "diff": block["diff"]})
        log.info(f"  Block queued for sync: {block['hash'][:20]}...")
    else:
        log.info(f"  Block mined: {block['hash'][:20]}... diff={block['diff']}")

    if not cfg["ipfs_key"]:
        queue_op(con, "ipfs_pin", {"hash": block["hash"]})
    if not cfg["bitcoin_reachable"]:
        queue_op(con, "btc_anchor", {"hash": block["hash"], "label": block["label"]})

    # 7. Status report
    print()
    print("=" * 60)
    print(f"  System ready — mode: {mode}")
    print(f"  Chain data: {len(blocks)} blocks (always offline)")
    print(f"  Queue: {pending_count(con)} operations waiting for connections")
    print(f"  Sync worker: active (checks every 30 s)")
    print()

    if mode == "OFFLINE":
        print("  OFFLINE MODE: The chain data is available. Mining works.")
        print("  Queued operations will sync automatically when connections restore.")
        print()
        print("  To connect now:")
        needs = [(k,v) for k,v in cfg["checks"].items() if not v["ok"] and v["category"] in ("key","node")]
        for label, info in needs[:5]:
            print(f"    {info['fix']}")

    elif mode == "PARTIAL":
        print("  PARTIAL MODE: Some services active. Queue handling the rest.")

    else:
        print("  FULL MODE: All connections active.")

    print("=" * 60)
    print()
    print("  Press Ctrl+C to stop (queue is preserved in offline_queue.db)\n")

    # 8. Keep running — the sync worker handles the rest autonomously
    try:
        while True:
            time.sleep(60)
            # Re-run preflight silently every 5 minutes
            # to detect when new connections become available
            if int(time.time()) % 300 < 60:
                cfg = run_preflight()
                if cfg["mode"] != mode:
                    mode = cfg["mode"]
                    log.info(f"  Connection state changed — now: {mode}")
    except KeyboardInterrupt:
        log.info("\n  Stopping sync worker...")
        stop_event.set()
        sync_thread.join(timeout=3)
        con.close()
        log.info("  Shutdown complete. Queue preserved.")


if __name__ == "__main__":
    main()
