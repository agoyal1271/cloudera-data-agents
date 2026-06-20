"""
Unified launcher for Cloudera AI Agents.
On Cloudera AI (CML): serves FastAPI + prebuilt React on CDSW_APP_PORT.
Local dev: runs FastAPI (port 8000) + Vite (port 5173) in parallel threads.
"""
import os
import subprocess
import sys
import time
import threading

IS_CLOUDERA = bool(os.getenv("CDSW_APP_PORT") or os.getenv("CDSW_PROJECT_ID"))
APP_PORT = int(os.getenv("CDSW_APP_PORT", "8000"))

# On CML the project always lives at /home/cdsw.
# Locally, derive from __file__ (defined when run as a real script).
if IS_CLOUDERA:
    BASE_DIR = "/home/cdsw"
    PYTHON = "/usr/local/bin/python3"
else:
    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        BASE_DIR = os.getcwd()
    # Prefer the system python3; fall back to whatever is on PATH
    PYTHON = os.getenv("PYTHON_BIN", sys.executable or "python3")

BACKEND_DIR = os.path.join(BASE_DIR, "02_backend")
FRONTEND_DIR = os.path.join(BASE_DIR, "03_frontend")


def _describe_port_holder(port):
    """Log which process currently LISTENs on `port`, and on which address,
    so we can see whether it's the CML proxy (PID 1) and which interface it
    bound. Pure /proc parsing — no external tools."""
    hexport = f"{port:04X}"
    inodes = {}
    for proc_net in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(proc_net) as f:
                lines = f.readlines()[1:]
        except FileNotFoundError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 10:
                continue
            local, state, inode = parts[1], parts[3], parts[9]
            if local.split(":")[-1].upper() == hexport and state == "0A":
                inodes[inode] = local  # local is HEXIP:HEXPORT
    if not inodes:
        print(f"[launch] :{port} has no current LISTEN socket (loopback should be free)")
        return
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            fds = os.listdir(f"/proc/{pid}/fd")
        except OSError:
            continue
        for fd in fds:
            try:
                link = os.readlink(f"/proc/{pid}/fd/{fd}")
            except OSError:
                continue
            if link.startswith("socket:[") and link[8:-1] in inodes:
                try:
                    with open(f"/proc/{pid}/cmdline", "rb") as cf:
                        cmd = cf.read().replace(b"\0", b" ").decode(errors="replace").strip()
                except OSError:
                    cmd = "?"
                print(f"[launch] :{port} held by pid={pid} addr={inodes[link[8:-1]]} cmd={cmd!r}")
                break


def run_backend():
    """
    Start uvicorn. On CML the runtime's app-proxy (PID 1) already owns the
    pod-IP side of CDSW_APP_PORT and forwards to the app on loopback, so we
    bind 127.0.0.1 (not 0.0.0.0, which collides with the proxy's bind).
    Override with UVICORN_HOST if needed.
    """
    env = {**os.environ, "PYTHONPATH": BACKEND_DIR}
    host = os.getenv("UVICORN_HOST", "127.0.0.1" if IS_CLOUDERA else "0.0.0.0")
    cmd = [
        PYTHON, "-m", "uvicorn", "app:app",
        "--host", host,
        "--port", str(APP_PORT),
        "--log-level", "info",
    ]
    if not IS_CLOUDERA:
        cmd.append("--reload")
    if IS_CLOUDERA:
        _describe_port_holder(APP_PORT)
    print(f"[launch] starting uvicorn on {host}:{APP_PORT} cwd={BACKEND_DIR}")
    subprocess.run(cmd, cwd=BACKEND_DIR, env=env)


def run_frontend():
    if IS_CLOUDERA:
        req = os.path.join(BACKEND_DIR, "requirements.txt")
        if os.path.exists(req):
            print("[launch] installing Python deps...")
            subprocess.run([PYTHON, "-m", "pip", "install", "-r", req, "-q"], check=False)
        else:
            print(f"[launch] requirements.txt not found at {req}")

        dist = os.path.join(FRONTEND_DIR, "dist")
        if os.path.isdir(dist):
            print(f"[launch] frontend dist/ found at {dist} — will be served by FastAPI")
        else:
            print(f"[launch] no dist/ at {dist} — API-only mode")

        print(f"[launch] frontend ready on port {APP_PORT}")
    else:
        time.sleep(1.5)
        subprocess.run(["npm", "run", "dev"], cwd=FRONTEND_DIR)


# CML PBJ kernel doesn't set __name__ == "__main__", so guard with True.
if True:
    print(f"[launch] Starting Cloudera AI Agents ({'CML' if IS_CLOUDERA else 'local dev'})")
    print(f"[launch] BASE_DIR={BASE_DIR}  BACKEND_DIR={BACKEND_DIR}  PYTHON={PYTHON}")
    print(f"[launch] 02_backend exists: {os.path.isdir(BACKEND_DIR)}")

    if IS_CLOUDERA:
        run_frontend()
        run_backend()
    else:
        backend_thread = threading.Thread(target=run_backend, daemon=True)
        backend_thread.start()

        frontend_thread = threading.Thread(target=run_frontend, daemon=False)
        frontend_thread.start()

        try:
            frontend_thread.join()
        except KeyboardInterrupt:
            print("\n[launch] shutting down...")
            sys.exit(0)
