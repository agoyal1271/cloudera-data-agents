"""
Unified launcher for Cloudera AI Agents.
Starts FastAPI backend (port 8000) and Vite frontend (port 5173).
On Cloudera AI: frontend binds to CDSW_APP_PORT and serves the built static files.
"""
import os
import subprocess
import sys
import time
import threading

# Use Anaconda Python if available (has all dependencies installed)
PYTHON = os.getenv("PYTHON_BIN", "/opt/anaconda3/bin/python")

# CML PBJ runtime executes via an IPython kernel where __file__ is not defined.
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.environ.get("CDSW_PROJECT_DIR", os.getcwd())

BACKEND_DIR = os.path.join(BASE_DIR, "02_backend")
FRONTEND_DIR = os.path.join(BASE_DIR, "03_frontend")

IS_CLOUDERA = bool(os.getenv("CDSW_APP_PORT") or os.getenv("CDSW_PROJECT_ID"))
APP_PORT = int(os.getenv("CDSW_APP_PORT", "8000"))


def run_backend():
    env = {**os.environ, "PYTHONPATH": BACKEND_DIR}
    workers = int(os.getenv("UVICORN_WORKERS", "2")) if IS_CLOUDERA else 1
    cmd = [
        PYTHON, "-m", "uvicorn", "app:app",
        "--host", "0.0.0.0",
        "--port", str(APP_PORT),
        "--workers", str(workers),
        "--log-level", "info",
    ]
    # --reload only in local dev (incompatible with multiple workers)
    if not IS_CLOUDERA:
        cmd.append("--reload")
    print(f"Starting uvicorn on port {APP_PORT} with {workers} worker(s)")
    subprocess.run(cmd, cwd=BACKEND_DIR, env=env)


def run_frontend():
    if IS_CLOUDERA:
        dist = os.path.join(FRONTEND_DIR, "dist")
        node_modules = os.path.join(FRONTEND_DIR, "node_modules")
        # Install Python deps
        req = os.path.join(BASE_DIR, "02_backend", "requirements.txt")
        if os.path.exists(req):
            print("Installing Python dependencies...")
            subprocess.run([PYTHON, "-m", "pip", "install", "-r", req, "-q"], check=False)

        # Find npm — try common locations
        npm = None
        for candidate in [os.getenv("NPM_BIN", ""), "/usr/bin/npm", "/usr/local/bin/npm",
                          "/opt/homebrew/bin/npm", "npm"]:
            if not candidate:
                continue
            try:
                if subprocess.run([candidate, "--version"], capture_output=True).returncode == 0:
                    npm = candidate
                    break
            except FileNotFoundError:
                continue

        if npm:
            if not os.path.isdir(node_modules):
                print(f"Installing npm deps with {npm}...")
                subprocess.run([npm, "install", "--silent"], cwd=FRONTEND_DIR, check=False)
            if not os.path.isdir(dist):
                print("Building React frontend...")
                subprocess.run([npm, "run", "build"], cwd=FRONTEND_DIR, check=False)
            else:
                print("Frontend dist/ already built.")
        else:
            print("npm not found — skipping frontend build. API-only mode.")

        print(f"Frontend served by FastAPI on port {APP_PORT}")
    else:
        time.sleep(1.5)
        subprocess.run(["npm", "run", "dev"], cwd=FRONTEND_DIR)


# Run unconditionally — CML PBJ kernel doesn't set __name__ == "__main__"
if True:
    print(f"Starting Cloudera AI Agents {'(Cloudera CAI mode)' if IS_CLOUDERA else '(local dev mode)'}")

    if IS_CLOUDERA:
        # On CAI: build frontend then start backend (single process)
        run_frontend()
        run_backend()
    else:
        # Local: run both in parallel threads
        backend_thread = threading.Thread(target=run_backend, daemon=True)
        backend_thread.start()

        frontend_thread = threading.Thread(target=run_frontend, daemon=False)
        frontend_thread.start()

        try:
            frontend_thread.join()
        except KeyboardInterrupt:
            print("\nShutting down...")
            sys.exit(0)
