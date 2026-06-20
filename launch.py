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


def _kill_port_holder(port):
    """
    Find and SIGKILL whatever process is LISTENing on `port`, using pure
    /proc parsing (no fuser/pkill needed). On CML the previous uvicorn gets
    orphaned across PBJ-kernel restarts and keeps holding the port; this
    clears it so the new uvicorn can bind.
    """
    hexport = f"{port:04X}"
    inodes = set()
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
            # state 0A == TCP_LISTEN
            if local.split(":")[-1].upper() == hexport and state == "0A":
                inodes.add(inode)
    if not inodes:
        print(f"[launch] no existing listener on :{port}")
        return

    me = os.getpid()
    killed = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) == me:
            continue
        fd_dir = f"/proc/{pid}/fd"
        try:
            fds = os.listdir(fd_dir)
        except OSError:
            continue
        for fd in fds:
            try:
                link = os.readlink(f"{fd_dir}/{fd}")
            except OSError:
                continue
            if link.startswith("socket:[") and link[8:-1] in inodes:
                try:
                    os.kill(int(pid), 9)
                    killed.append(pid)
                except OSError:
                    pass
                break
    print(f"[launch] killed stale listener(s) on :{port}: {killed or 'none'}")
    time.sleep(2)  # let the OS release the socket


def run_backend():
    """
    Start uvicorn as a subprocess. Before binding, clear any orphaned
    listener on APP_PORT (left by a prior PBJ-kernel restart) so we never
    crash with 'address already in use'. Loops so the app process never
    exits (which CML would treat as the app stopping).
    """
    env = {**os.environ, "PYTHONPATH": BACKEND_DIR}
    cmd = [
        PYTHON, "-m", "uvicorn", "app:app",
        "--host", "0.0.0.0",
        "--port", str(APP_PORT),
        "--log-level", "info",
    ]
    if not IS_CLOUDERA:
        cmd.append("--reload")
        subprocess.run(cmd, cwd=BACKEND_DIR, env=env)
        return

    while True:
        _kill_port_holder(APP_PORT)
        print(f"[launch] starting uvicorn on :{APP_PORT} cwd={BACKEND_DIR}")
        subprocess.run(cmd, cwd=BACKEND_DIR, env=env)
        print("[launch] uvicorn exited; clearing port and retrying in 3s")
        time.sleep(3)


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
