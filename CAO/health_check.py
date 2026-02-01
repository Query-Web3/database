#!/usr/bin/env python3
import os
import sys
import json
from dotenv import load_dotenv
from utils import HealthMonitor

def main():
    load_dotenv()
    
    db_config = {
        'user': os.getenv("DB_USERNAME"),
        'password': os.getenv("DB_PASSWORD"),
        'database': os.getenv("DB_NAME"),
        'port':  os.getenv("DB_PORT",3306),
        'host': os.getenv("DB_HOST", "127.0.0.1")
    }
    
    health_status = {
        "status": "ok",
        "checks": {}
    }
    
    # 1. Check Database
    db_check = HealthMonitor.check_db_connection(db_config)
    health_status["checks"]["database"] = "ok" if db_check else "failed"
    if not db_check:
        health_status["status"] = "error"
        
    # 2. Check Disk Space
    disk_check = HealthMonitor.check_disk_space()
    health_status["checks"]["disk_space"] = "ok" if disk_check else "warning"
    if not disk_check:
        # Warning doesn't necessarily mean error for the whole system, but let's flag it
        pass 

    print(json.dumps(health_status, indent=2))
    
    if health_status["status"] != "ok":
        sys.exit(1)
        
    sys.exit(0)

if __name__ == "__main__":
    main()
