#!/usr/bin/env python3
import os
import sys
import json
import argparse
from dotenv import load_dotenv
from utils import HealthMonitor

def main():
    parser = argparse.ArgumentParser(description="QueryWeb3 Health Check Utility")
    parser.add_argument("--service", type=str, choices=["bifrost", "hydration", "prices", "stellaswap", "database"], 
                        help="Specific service to check health for")
    parser.add_argument("--max-age", type=int, default=7200, 
                        help="Maximum age of heartbeat in seconds (default: 7200s / 2h)")
    
    args = parser.parse_args()
    load_dotenv()
    
    db_config = {
        'user': os.getenv("DB_USERNAME"),
        'password': os.getenv("DB_PASSWORD"),
        'database': os.getenv("DB_NAME"),
        'port': int(os.getenv("DB_PORT", 3306)),
        'host': os.getenv("DB_HOST", "127.0.0.1")
    }
    
    if args.service:
        # Specific service mode
        if args.service == "database":
            is_healthy = HealthMonitor.check_db_connection(db_config)
        else:
            is_healthy = HealthMonitor.check_script_health(args.service, max_age_seconds=args.max_age)
        
        status_str = "healthy" if is_healthy else "unhealthy"
        print(f"Service '{args.service}' is {status_str}")
        sys.exit(0) if is_healthy else sys.exit(1)

    # Full report mode (default)
    health_status = {
        "status": "ok",
        "checks": {}
    }
    
    # 1. Check Database
    db_check = HealthMonitor.check_db_connection(db_config)
    health_status["checks"]["database"] = "ok" if db_check else "failed"
    if not db_check:
        health_status["status"] = "error"
        
    # 2. Check Services
    for svc in ["orchestrator", "bifrost", "hydration", "prices", "stellaswap"]:
        svc_check = HealthMonitor.check_script_health(svc, max_age_seconds=args.max_age)
        health_status["checks"][svc] = "ok" if svc_check else "failed"
        if not svc_check:
            # We don't mark the whole report as error if one fetcher fails, 
            # as it might just be the container starting up or sleeping.
            # But we flag it in the checks.
            pass

    # 3. Check Disk Space
    disk_check = HealthMonitor.check_disk_space()
    health_status["checks"]["disk_space"] = "ok" if disk_check else "warning"

    print(json.dumps(health_status, indent=2))
    
    if health_status["status"] != "ok":
        sys.exit(1)
        
    sys.exit(0)

if __name__ == "__main__":
    main()
