#!/usr/bin/env python3
import time
import sys
import signal
import os

def signal_handler(sig, frame):
    print(f"Mock runner received signal {sig}. Exiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    run_id = sys.argv[1] if len(sys.argv) > 1 else "default"
    print(f"Mock runner {run_id} started. Pid: {os.getpid()}")
    
    try:
        while True:
            print(f"Mock runner {run_id} heartbeat...")
            sys.stdout.flush()
            time.sleep(1)
    except Exception as e:
        print(f"Mock runner {run_id} error: {e}")

if __name__ == "__main__":
    main()
