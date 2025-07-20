#!/usr/bin/env python3
"""
Start Dagster Web Server
Launches the Dagster UI for monitoring and running the data pipeline
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Start the Dagster web server"""
    print("Starting Dagster Web Server")
    print("===========================")
    print("This will start the Dagster UI at http://localhost:3000")
    print("You can monitor and trigger your data pipeline from there.")
    print()
    
    try:
        # Start Dagster web server
        cmd = [
            "poetry", "run", "dagster", "dev", 
            "--module-name", "dagster_pipeline.definitions",
            "--port", "3000"
        ]
        
        print("Starting Dagster webserver...")
        print("Access the UI at: http://localhost:3000")
        print("Press Ctrl+C to stop")
        print()
        
        # Run in foreground so user can see logs and stop with Ctrl+C
        subprocess.run(cmd, check=True)
        
    except KeyboardInterrupt:
        print("\nDagster webserver stopped by user")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to start Dagster webserver: {e}")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 