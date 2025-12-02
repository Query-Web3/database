#!/usr/bin/env python3
import subprocess
import sys
import time
from pathlib import Path

# Base directory where all the scripts live
BASE_DIR = Path(__file__).resolve().parent

# Script paths
BIFROST_SCRIPT = BASE_DIR / "Bifrost_Data_fetching.py"
HYDRATION_SCRIPT = BASE_DIR / "Hydration_Data_fetching.py"
ASSET_PRICES_SCRIPT = BASE_DIR / "fetch_asset_prices.py"
MERGE_SCRIPT = BASE_DIR / "combine_tables.py"


def start_long_running_scripts():
    """
    Start the three fetch scripts that run their own internal loops.
    Returns a list of Popen processes.
    """
    processes = []

    commands = [
        [sys.executable, str(BIFROST_SCRIPT)],
        [sys.executable, str(HYDRATION_SCRIPT)],
        [sys.executable, str(ASSET_PRICES_SCRIPT)],
    ]

    for cmd in commands:
        print(f"Starting: {' '.join(cmd)}")
        p = subprocess.Popen(cmd, cwd=str(BASE_DIR))
        processes.append(p)

    return processes


def run_merge_script():
    """
    Run merge_multiple_tables.py once (blocking).
    """
    print("\n[MERGE] Running merge_multiple_tables.py ...")
    try:
        subprocess.run(
            [sys.executable, str(MERGE_SCRIPT)],
            cwd=str(BASE_DIR),
            check=True
        )
        print("[MERGE] Done.\n")
    except subprocess.CalledProcessError as e:
        print(f"[MERGE] Failed with exit code {e.returncode}\n")


def main():
    # Start the three long-running fetch scripts
    processes = start_long_running_scripts()

    # Run merge_multiple_tables.py every hour
    MERGE_INTERVAL_SEC = 60 * 60  # 1 hour
    next_merge_time = time.time()  # run once immediately on start

    print("Scheduler started. Press Ctrl+C to stop.\n")

    try:
        while True:
            now = time.time()

            # Run merge script when it's time
            if now >= next_merge_time:
                run_merge_script()
                next_merge_time = now + MERGE_INTERVAL_SEC

            # Optional: check if any fetch process died
            for p in processes:
                if p.poll() is not None:
                    print(f"[WARN] Process {p.args} exited with code {p.returncode}")
                    # If you want auto-restart, you could restart it here.

            time.sleep(10)  # avoid busy loop

    except KeyboardInterrupt:
        print("\nReceived Ctrl+C, stopping all processes...")

    finally:
        # Try to gracefully stop child processes
        for p in processes:
            if p.poll() is None:
                try:
                    p.terminate()
                except Exception:
                    pass

        time.sleep(2)
        for p in processes:
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass

        print("All processes stopped.")


if __name__ == "__main__":
    main()

