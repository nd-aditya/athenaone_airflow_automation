"""
Test script: connect VPN → test Snowflake → disconnect VPN.
Run from project root:
    python test_vpn_snowflake.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from services.vpn_service import vpn_connect, vpn_disconnect, vpn_is_connected
from services.config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE,
)


def test_snowflake_connection() -> bool:
    from sqlalchemy import create_engine, text
    print("\n[snowflake] Connecting...")
    engine = create_engine(
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
        f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}"
        f"?warehouse={SNOWFLAKE_WAREHOUSE}",
        connect_args={"insecure_mode": True},
    )
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()"))
            row = result.fetchone()
            print(f"[snowflake] Connected successfully!")
            print(f"  User      : {row[0]}")
            print(f"  Database  : {row[1]}")
            print(f"  Warehouse : {row[2]}")
        return True
    except Exception as e:
        print(f"[snowflake] Connection failed: {e}")
        return False
    finally:
        engine.dispose()


if __name__ == "__main__":
    print("=" * 50)
    print("Step 1 — Checking VPN status...")
    if vpn_is_connected():
        print("[vpn] Already connected — skipping connect step.")
    else:
        print("[vpn] Not connected. Connecting...")
        try:
            vpn_connect()
        except Exception as e:
            print(f"[vpn] Failed to connect: {e}")
            sys.exit(1)

    print("\nStep 2 — Testing Snowflake connection...")
    success = test_snowflake_connection()

    print("\nStep 3 — Disconnecting VPN...")
    vpn_disconnect()

    print("\n" + "=" * 50)
    print("RESULT:", "PASS" if success else "FAIL")
    sys.exit(0 if success else 1)
