import asyncio
from whitelist_system import build_store_from_env, LuarmorSyncError
from pathlib import Path

async def main():
    store = build_store_from_env(Path("tmp_test.db"))
    # mock luarmor to pass checks
    store.luarmor.api_key = "test_key"
    store.luarmor.project_id = "test_project"
    store.key_provider = "luarmor"
    
    await store.ensure_initialized()
    print("Initialized OK")

    # try generating a key directly without luarmor
    store.key_provider = "local"
    key = await store.create_key(123)
    print("Created local key:", key)
    
    # redeem key
    success = await store.redeem_key(999, key)
    print("Redeem local key:", success)

    stats = await store.get_stats()
    print("Stats:", stats)

if __name__ == "__main__":
    asyncio.run(main())
