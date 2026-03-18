from __future__ import annotations

import asyncio
import logging
import os
import secrets
import sqlite3
import string
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib import error as urllib_error

import asyncpg


LOGGER = logging.getLogger("theseus-bot.whitelist")
KEY_PREFIX = "ZyphraxHub"
LUARMOR_API_BASE_URL = "https://api.luarmor.net"


class LuarmorSyncError(RuntimeError):
    pass


class LuarmorClient:
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
        base_url: str = LUARMOR_API_BASE_URL,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.project_id = (project_id or "").strip()
        self.base_url = base_url.rstrip("/")

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.project_id)

    def _project_url(self, suffix: str) -> str:
        return f"{self.base_url}/v3/projects/{self.project_id}{suffix}"

    @staticmethod
    def _extract_users(result: Any) -> list[dict[str, Any]]:
        if isinstance(result, list):
            return [dict(item) for item in result if isinstance(item, dict)]
        if isinstance(result, dict):
            users = result.get("users")
            if isinstance(users, list):
                return [dict(item) for item in users if isinstance(item, dict)]
        return []

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[dict[str, str]] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> Any:
        if not self.enabled:
            raise LuarmorSyncError("Luarmor credentials are not configured.")

        url = self._project_url(path)
        if query:
            url = f"{url}?{urllib_parse.urlencode(query)}"

        body = None
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

        request = urllib_request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib_request.urlopen(request, timeout=20) as response:
                raw_body = response.read().decode("utf-8", errors="replace").strip()
        except urllib_error.HTTPError as exc:
            response_text = exc.read().decode("utf-8", errors="replace").strip()
            raise LuarmorSyncError(
                f"Luarmor {method} {path} failed with HTTP {exc.code}: {response_text or exc.reason}"
            ) from exc
        except urllib_error.URLError as exc:
            raise LuarmorSyncError(f"Luarmor request failed: {exc.reason}") from exc

        if not raw_body:
            return None
        try:
            return json.loads(raw_body)
        except json.JSONDecodeError:
            return {"raw": raw_body}

    async def get_users(self, **query: str) -> list[dict[str, Any]]:
        result = await asyncio.to_thread(
            self._request,
            "GET",
            "/users",
            query={key: value for key, value in query.items() if str(value).strip()},
        )
        return self._extract_users(result)

    async def get_user_by_discord_id(self, discord_id: int | str) -> Optional[dict[str, Any]]:
        users = await self.get_users(discord_id=str(discord_id))
        return users[0] if users else None

    async def get_user_by_key(self, user_key: str) -> Optional[dict[str, Any]]:
        users = await self.get_users(user_key=user_key)
        return users[0] if users else None

    async def create_user(
        self,
        *,
        discord_id: int | str | None = None,
        note: str,
        identifier: Optional[str] = None,
        key_days: Optional[int] = None,
        auth_expire: Optional[int] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"note": note}
        if discord_id not in (None, ""):
            payload["discord_id"] = str(discord_id)
        if identifier not in (None, ""):
            payload["identifier"] = str(identifier)
        if key_days is not None:
            payload["key_days"] = int(key_days)
        if auth_expire is not None:
            payload["auth_expire"] = int(auth_expire)
        result = await asyncio.to_thread(
            self._request,
            "POST",
            "/users",
            payload=payload,
        )
        if isinstance(result, dict):
            return result
        raise LuarmorSyncError("Luarmor create user returned an unexpected response.")

    async def update_user(
        self,
        *,
        user_key: str,
        discord_id: int | str | None = None,
        note: Optional[str] = None,
        identifier: Optional[str] = None,
        auth_expire: Optional[int] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"user_key": user_key}
        if discord_id not in (None, ""):
            payload["discord_id"] = str(discord_id)
        if note is not None:
            payload["note"] = note
        if identifier is not None:
            payload["identifier"] = identifier
        if auth_expire is not None:
            payload["auth_expire"] = int(auth_expire)
        result = await asyncio.to_thread(
            self._request,
            "PATCH",
            "/users",
            payload=payload,
        )
        if isinstance(result, dict):
            return result
        raise LuarmorSyncError("Luarmor update user returned an unexpected response.")

    async def delete_user(self, *, user_key: str) -> None:
        await asyncio.to_thread(
            self._request,
            "DELETE",
            "/users",
            query={"user_key": user_key},
        )

    async def reset_hwid(self, *, user_key: str, force: bool = False) -> Any:
        return await asyncio.to_thread(
            self._request,
            "POST",
            "/users/resethwid",
            payload={"user_key": user_key, "force": force},
        )

    async def link_discord(self, *, user_key: str, discord_id: int | str, force: bool = False) -> Any:
        return await asyncio.to_thread(
            self._request,
            "POST",
            "/users/linkdiscord",
            payload={
                "user_key": user_key,
                "discord_id": str(discord_id),
                "force": force,
            },
        )

    async def blacklist_user(
        self,
        *,
        user_key: str,
        ban_reason: str,
        ban_expire: Optional[int] = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "user_key": user_key,
            "ban_reason": ban_reason,
        }
        if ban_expire is not None:
            payload["ban_expire"] = int(ban_expire)
        return await asyncio.to_thread(
            self._request,
            "POST",
            "/users/blacklist",
            payload=payload,
        )

    async def unblacklist_user(self, *, unban_token: str) -> Any:
        return await asyncio.to_thread(
            self._request,
            "GET",
            "/users/unban",
            query={"unban_token": unban_token},
        )


class WhitelistStore:
    def __init__(
        self,
        sqlite_path: Path,
        database_url: Optional[str] = None,
        *,
        luarmor_api_key: Optional[str] = None,
        luarmor_project_id: Optional[str] = None,
        key_provider: Optional[str] = None,
        luarmor_key_days: Optional[int] = None,
    ) -> None:
        self.sqlite_path = sqlite_path
        self.database_url = (database_url or "").strip() or None
        self._pool: Optional[asyncpg.Pool] = None
        self._initialized = False
        self._init_lock = asyncio.Lock()
        provider = (key_provider or "local").strip().lower()
        self.key_provider = provider if provider in {"local", "luarmor"} else "local"
        self.luarmor_key_days = luarmor_key_days if luarmor_key_days and luarmor_key_days > 0 else None
        self.luarmor = LuarmorClient(
            api_key=luarmor_api_key,
            project_id=luarmor_project_id,
        )

    async def ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            if self.database_url:
                try:
                    await self._init_postgres()
                    self._initialized = True
                    return
                except Exception as exc:
                    LOGGER.warning(
                        "Postgres whitelist backend unavailable, falling back to SQLite: %s",
                        exc,
                    )
                    self.database_url = None

            await asyncio.to_thread(self._init_sqlite)
            self._initialized = True

    def generate_key(self) -> str:
        alphabet = string.ascii_uppercase + string.digits
        parts = ["".join(secrets.choice(alphabet) for _ in range(3)) for _ in range(3)]
        return f"{KEY_PREFIX}-" + "-".join(parts)

    @property
    def uses_luarmor_keys(self) -> bool:
        return self.key_provider == "luarmor" and self.luarmor.enabled

    async def log_event(
        self, event_type: str, discord_id: int | str | None, details: str = ""
    ) -> None:
        await self.ensure_initialized()
        discord_id_str = None if discord_id is None else str(discord_id)
        timestamp = datetime.now(UTC).isoformat()

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                await connection.execute(
                    """
                    INSERT INTO analytics (event_type, discord_id, details, timestamp)
                    VALUES ($1, $2, $3, $4)
                    """,
                    event_type,
                    discord_id_str,
                    details,
                    timestamp,
                )
            return

        def _run() -> None:
            with self._sqlite_connect() as connection:
                connection.execute(
                    """
                    INSERT INTO analytics (event_type, discord_id, details, timestamp)
                    VALUES (?, ?, ?, ?)
                    """,
                    (event_type, discord_id_str, details, timestamp),
                )
                connection.commit()

        await asyncio.to_thread(_run)

    async def create_key(self, created_by: int | str | None) -> str:
        await self.ensure_initialized()
        if self.uses_luarmor_keys:
            created_by_str = None if created_by is None else str(created_by)
            timestamp = datetime.now(UTC).isoformat()
            created = await self.luarmor.create_user(
                note=f"{KEY_PREFIX} generated by Discord bot",
                key_days=self.luarmor_key_days,
            )
            user_key = str(created.get("user_key") or "").strip()
            if not user_key:
                raise LuarmorSyncError("Luarmor did not return a generated key.")

            if self._pool is not None:
                await self._pool.execute(
                    """
                    INSERT INTO keys (key, created_by, created_at, used)
                    VALUES ($1, $2, $3, 0)
                    ON CONFLICT (key) DO NOTHING
                    """,
                    user_key,
                    created_by_str,
                    timestamp,
                )
            else:
                def _run() -> None:
                    with self._sqlite_connect() as connection:
                        connection.execute(
                            """
                            INSERT OR IGNORE INTO keys (key, created_by, created_at, used)
                            VALUES (?, ?, ?, 0)
                            """,
                            (user_key, created_by_str, timestamp),
                        )
                        connection.commit()

                await asyncio.to_thread(_run)
            await self.log_event("key_generated", created_by, user_key)
            return user_key

        while True:
            key = self.generate_key()
            timestamp = datetime.now(UTC).isoformat()
            created_by_str = None if created_by is None else str(created_by)
            try:
                if self._pool is not None:
                    async with self._pool.acquire() as connection:
                        await connection.execute(
                            """
                            INSERT INTO keys (key, created_by, created_at, used)
                            VALUES ($1, $2, $3, 0)
                            """,
                            key,
                            created_by_str,
                            timestamp,
                        )
                else:
                    def _run() -> None:
                        with self._sqlite_connect() as connection:
                            connection.execute(
                                """
                                INSERT INTO keys (key, created_by, created_at, used)
                                VALUES (?, ?, ?, 0)
                                """,
                                (key, created_by_str, timestamp),
                            )
                            connection.commit()

                    await asyncio.to_thread(_run)
                await self.log_event("key_generated", created_by, key)
                return key
            except (sqlite3.IntegrityError, asyncpg.UniqueViolationError):
                continue

    async def create_keys(self, count: int, created_by: int | str | None) -> list[str]:
        return [await self.create_key(created_by) for _ in range(max(1, count))]

    async def get_user(self, discord_id: int | str) -> Optional[dict[str, Any]]:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(
                    "SELECT * FROM users WHERE discord_id = $1",
                    discord_id_str,
                )
                return dict(row) if row is not None else None

        def _run() -> Optional[dict[str, Any]]:
            with self._sqlite_connect() as connection:
                row = connection.execute(
                    "SELECT * FROM users WHERE discord_id = ?",
                    (discord_id_str,),
                ).fetchone()
                return dict(row) if row is not None else None

        return await asyncio.to_thread(_run)

    async def get_user_with_stats(self, discord_id: int | str) -> Optional[dict[str, Any]]:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(
                    "SELECT * FROM users WHERE discord_id = $1",
                    discord_id_str,
                )
                if row is None:
                    return None
                login_count = await connection.fetchval(
                    "SELECT COUNT(*) FROM analytics WHERE discord_id = $1 AND event_type = 'login'",
                    discord_id_str,
                )
                redeem_count = await connection.fetchval(
                    "SELECT COUNT(*) FROM analytics WHERE discord_id = $1 AND event_type = 'redeem'",
                    discord_id_str,
                )
                banned = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM blacklist WHERE discord_id = $1)",
                    discord_id_str,
                )
                user = dict(row)
                user["login_count"] = int(login_count or 0)
                user["redeem_count"] = int(redeem_count or 0)
                user["banned"] = bool(banned)
                return user

        def _run() -> Optional[dict[str, Any]]:
            with self._sqlite_connect() as connection:
                row = connection.execute(
                    "SELECT * FROM users WHERE discord_id = ?",
                    (discord_id_str,),
                ).fetchone()
                if row is None:
                    return None
                login_count = connection.execute(
                    "SELECT COUNT(*) FROM analytics WHERE discord_id = ? AND event_type = 'login'",
                    (discord_id_str,),
                ).fetchone()[0]
                redeem_count = connection.execute(
                    "SELECT COUNT(*) FROM analytics WHERE discord_id = ? AND event_type = 'redeem'",
                    (discord_id_str,),
                ).fetchone()[0]
                banned = connection.execute(
                    "SELECT 1 FROM blacklist WHERE discord_id = ?",
                    (discord_id_str,),
                ).fetchone() is not None
                user = dict(row)
                user["login_count"] = int(login_count)
                user["redeem_count"] = int(redeem_count)
                user["banned"] = banned
                return user

        return await asyncio.to_thread(_run)

    async def key_exists_and_unused(self, key: str) -> bool:
        await self.ensure_initialized()
        normalized_key = key.strip()

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(
                    "SELECT used FROM keys WHERE key = $1",
                    normalized_key,
                )
                return row is not None and int(row["used"]) == 0

        def _run() -> bool:
            with self._sqlite_connect() as connection:
                row = connection.execute(
                    "SELECT used FROM keys WHERE key = ?",
                    (normalized_key,),
                ).fetchone()
                return row is not None and int(row["used"]) == 0

        return await asyncio.to_thread(_run)

    async def redeem_key(self, discord_id: int | str, key: str) -> bool:
        await self.ensure_initialized()
        normalized_key = key.strip()
        discord_id_str = str(discord_id)
        now = datetime.now(UTC).isoformat()
        previous_user = await self.get_user(discord_id)
        previous_key = None if previous_user is None else previous_user.get("key")

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    row = await connection.fetchrow(
                        "SELECT used FROM keys WHERE key = $1 FOR UPDATE",
                        normalized_key,
                    )
                    if row is None or int(row["used"]) != 0:
                        return False
                    await connection.execute(
                        """
                        INSERT INTO users (discord_id, key, joined_at)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (discord_id) DO UPDATE SET key = EXCLUDED.key
                        """,
                        discord_id_str,
                        normalized_key,
                        now,
                    )
                    await connection.execute(
                        """
                        UPDATE keys
                        SET used = 1, used_by = $1, used_at = $2
                        WHERE key = $3
                        """,
                        discord_id_str,
                        now,
                        normalized_key,
                    )
            try:
                await self._sync_luarmor_assignment(
                    discord_id=discord_id,
                    local_key=normalized_key,
                )
            except LuarmorSyncError as exc:
                await self._rollback_redeem(discord_id, normalized_key, previous_key)
                LOGGER.warning("Luarmor sync failed after redeem for %s: %s", discord_id, exc)
                return False
            await self.log_event("redeem", discord_id, normalized_key)
            await self.log_event("login", discord_id, "Redeemed key")
            return True

        def _run() -> bool:
            with self._sqlite_connect() as connection:
                row = connection.execute(
                    "SELECT used FROM keys WHERE key = ?",
                    (normalized_key,),
                ).fetchone()
                if row is None or int(row["used"]) != 0:
                    return False
                connection.execute(
                    """
                    INSERT INTO users (discord_id, key, joined_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(discord_id) DO UPDATE SET key = excluded.key
                    """,
                    (discord_id_str, normalized_key, now),
                )
                connection.execute(
                    "UPDATE keys SET used = 1, used_by = ?, used_at = ? WHERE key = ?",
                    (discord_id_str, now, normalized_key),
                )
                connection.commit()
                return True

        success = await asyncio.to_thread(_run)
        if success:
            try:
                await self._sync_luarmor_assignment(
                    discord_id=discord_id,
                    local_key=normalized_key,
                )
            except LuarmorSyncError as exc:
                await self._rollback_redeem(discord_id, normalized_key, previous_key)
                LOGGER.warning("Luarmor sync failed after redeem for %s: %s", discord_id, exc)
                return False
            await self.log_event("redeem", discord_id, normalized_key)
            await self.log_event("login", discord_id, "Redeemed key")
        return success

    async def whitelist_user(self, discord_id: int | str, *, created_by: int | str | None) -> str:
        existing = await self.get_user(discord_id)
        if existing is not None and existing.get("key"):
            return str(existing["key"])

        key = await self.create_key(created_by)
        if not await self.redeem_key(discord_id, key):
            raise RuntimeError("Failed to assign generated key.")
        await self.log_event("whitelist", discord_id, f"Assigned by {created_by}")
        return key

    async def unwhitelist_user(self, discord_id: int | str) -> Optional[str]:
        await self.ensure_initialized()
        user = await self.get_user(discord_id)
        if user is None or not user.get("key"):
            return None

        key = str(user["key"])
        discord_id_str = str(discord_id)
        luarmor_user_key = (
            str(user["luarmor_user_key"])
            if user.get("luarmor_user_key")
            else None
        )

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        "UPDATE users SET key = NULL WHERE discord_id = $1",
                        discord_id_str,
                    )
                    await connection.execute(
                        "UPDATE keys SET used = 0, used_by = NULL, used_at = NULL WHERE key = $1",
                        key,
                    )
                    await connection.execute(
                        """
                        UPDATE users
                        SET luarmor_user_key = NULL,
                            luarmor_status = NULL,
                            luarmor_synced_at = $2,
                            luarmor_unban_token = NULL,
                            luarmor_ban_reason = NULL
                        WHERE discord_id = $1
                        """,
                        discord_id_str,
                        datetime.now(UTC).isoformat(),
                    )
            try:
                await self._delete_luarmor_assignment(discord_id=discord_id, user_key=luarmor_user_key)
            except LuarmorSyncError as exc:
                await self._rollback_unwhitelist(discord_id, key, user)
                LOGGER.warning("Luarmor sync failed after unwhitelist for %s: %s", discord_id, exc)
                return None
            await self.log_event("unwhitelist", discord_id, key)
            return key

        def _run() -> None:
            with self._sqlite_connect() as connection:
                connection.execute(
                    "UPDATE users SET key = NULL WHERE discord_id = ?",
                    (discord_id_str,),
                )
                connection.execute(
                    "UPDATE keys SET used = 0, used_by = NULL, used_at = NULL WHERE key = ?",
                    (key,),
                )
                connection.execute(
                    """
                    UPDATE users
                    SET luarmor_user_key = NULL,
                        luarmor_status = NULL,
                        luarmor_synced_at = ?,
                        luarmor_unban_token = NULL,
                        luarmor_ban_reason = NULL
                    WHERE discord_id = ?
                    """,
                    (datetime.now(UTC).isoformat(), discord_id_str),
                )
                connection.commit()

        await asyncio.to_thread(_run)
        try:
            await self._delete_luarmor_assignment(discord_id=discord_id, user_key=luarmor_user_key)
        except LuarmorSyncError as exc:
            await self._rollback_unwhitelist(discord_id, key, user)
            LOGGER.warning("Luarmor sync failed after unwhitelist for %s: %s", discord_id, exc)
            return None
        await self.log_event("unwhitelist", discord_id, key)
        return key

    async def get_all_keys(self, *, include_used: bool = True) -> list[dict[str, Any]]:
        await self.ensure_initialized()

        if self._pool is not None:
            query = "SELECT * FROM keys ORDER BY created_at DESC"
            if not include_used:
                query = "SELECT * FROM keys WHERE used = 0 ORDER BY created_at DESC"
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(query)
                return [dict(row) for row in rows]

        def _run() -> list[dict[str, Any]]:
            with self._sqlite_connect() as connection:
                query = "SELECT * FROM keys ORDER BY created_at DESC"
                if not include_used:
                    query = "SELECT * FROM keys WHERE used = 0 ORDER BY created_at DESC"
                rows = connection.execute(query).fetchall()
                return [dict(row) for row in rows]

        return await asyncio.to_thread(_run)

    async def get_all_users(self) -> list[dict[str, Any]]:
        await self.ensure_initialized()

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                rows = await connection.fetch("SELECT * FROM users ORDER BY joined_at DESC")
                return [dict(row) for row in rows]

        def _run() -> list[dict[str, Any]]:
            with self._sqlite_connect() as connection:
                rows = connection.execute("SELECT * FROM users ORDER BY joined_at DESC").fetchall()
                return [dict(row) for row in rows]

        return await asyncio.to_thread(_run)

    async def purge_unused_keys(self) -> int:
        await self.ensure_initialized()

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                result = await connection.execute("DELETE FROM keys WHERE used = 0")
            deleted = int(result.split()[-1])
            await self.log_event("purge_keys", None, f"Deleted {deleted} unused keys")
            return deleted

        def _run() -> int:
            with self._sqlite_connect() as connection:
                cursor = connection.execute("DELETE FROM keys WHERE used = 0")
                connection.commit()
                return int(cursor.rowcount or 0)

        deleted = await asyncio.to_thread(_run)
        await self.log_event("purge_keys", None, f"Deleted {deleted} unused keys")
        return deleted

    async def is_blacklisted(self, discord_id: int | str) -> bool:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                return bool(
                    await connection.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM blacklist WHERE discord_id = $1)",
                        discord_id_str,
                    )
                )

        def _run() -> bool:
            with self._sqlite_connect() as connection:
                row = connection.execute(
                    "SELECT 1 FROM blacklist WHERE discord_id = ?",
                    (discord_id_str,),
                ).fetchone()
                return row is not None

        return await asyncio.to_thread(_run)

    async def get_blacklist_reason(self, discord_id: int | str) -> Optional[str]:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                reason = await connection.fetchval(
                    "SELECT reason FROM blacklist WHERE discord_id = $1",
                    discord_id_str,
                )
                return str(reason) if reason not in (None, "") else None

        def _run() -> Optional[str]:
            with self._sqlite_connect() as connection:
                row = connection.execute(
                    "SELECT reason FROM blacklist WHERE discord_id = ?",
                    (discord_id_str,),
                ).fetchone()
                if row is None or row["reason"] in (None, ""):
                    return None
                return str(row["reason"])

        return await asyncio.to_thread(_run)

    async def blacklist_user(
        self,
        discord_id: int | str,
        *,
        reason: str,
        ban_expire: Optional[int] = None,
    ) -> bool:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)
        now = datetime.now(UTC).isoformat()
        normalized_reason = reason.strip() or "Blacklisted by staff."

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                await connection.execute(
                    """
                    INSERT INTO blacklist (discord_id, reason, banned_at)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (discord_id) DO UPDATE
                    SET reason = EXCLUDED.reason, banned_at = EXCLUDED.banned_at
                    """,
                    discord_id_str,
                    normalized_reason,
                    now,
                )
        else:
            def _run() -> None:
                with self._sqlite_connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO blacklist (discord_id, reason, banned_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(discord_id) DO UPDATE
                        SET reason = excluded.reason, banned_at = excluded.banned_at
                        """,
                        (discord_id_str, normalized_reason, now),
                    )
                    connection.commit()

            await asyncio.to_thread(_run)

        user = await self.get_user(discord_id)
        remote_key = await self._resolve_remote_user_key(discord_id, user=user)
        if remote_key and self.luarmor.enabled:
            await self.luarmor.blacklist_user(
                user_key=remote_key,
                ban_reason=normalized_reason,
                ban_expire=ban_expire,
            )
            remote_user = await self.luarmor.get_user_by_key(remote_key)
            if remote_user is not None:
                await self._apply_remote_user_snapshot(discord_id, remote_user)

        await self.log_event("blacklist", discord_id, normalized_reason)
        return True

    async def unblacklist_user(self, discord_id: int | str) -> bool:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)
        user = await self.get_user(discord_id)

        remote_user: Optional[dict[str, Any]] = None
        if self.luarmor.enabled:
            remote_key = await self._resolve_remote_user_key(discord_id, user=user)
            if remote_key:
                remote_user = await self.luarmor.get_user_by_key(remote_key)
                if remote_user is not None and remote_user.get("banned"):
                    unban_token = str(remote_user.get("unban_token") or "").strip()
                    if not unban_token:
                        raise LuarmorSyncError("Luarmor user is banned but has no unban token.")
                    await self.luarmor.unblacklist_user(unban_token=unban_token)
                    remote_user = await self.luarmor.get_user_by_key(remote_key)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                await connection.execute(
                    "DELETE FROM blacklist WHERE discord_id = $1",
                    discord_id_str,
                )
        else:
            def _run() -> None:
                with self._sqlite_connect() as connection:
                    connection.execute(
                        "DELETE FROM blacklist WHERE discord_id = ?",
                        (discord_id_str,),
                    )
                    connection.commit()

            await asyncio.to_thread(_run)

        if remote_user is not None:
            await self._apply_remote_user_snapshot(discord_id, remote_user)
        else:
            await self._set_luarmor_fields(
                discord_id,
                user_key=None if user is None else user.get("luarmor_user_key"),
                status=None if user is None else user.get("luarmor_status"),
                identifier=None,
                unban_token=None,
                ban_reason=None,
            )

        await self.log_event("unblacklist", discord_id, "")
        return True

    async def reset_hwid(self, discord_id: int | str, *, force: bool = False) -> bool:
        await self.ensure_initialized()
        user = await self.get_user(discord_id)
        if user is None or not user.get("key"):
            return False

        remote_key = await self._resolve_remote_user_key(discord_id, user=user)
        if remote_key and self.luarmor.enabled:
            await self.luarmor.reset_hwid(user_key=remote_key, force=force)
            remote_user = await self.luarmor.get_user_by_key(remote_key)
            if remote_user is not None:
                await self._apply_remote_user_snapshot(discord_id, remote_user)
        else:
            await self._clear_local_hwid(discord_id)
            await self._set_luarmor_fields(
                discord_id,
                user_key=user.get("luarmor_user_key"),
                status="reset",
                identifier="",
                unban_token=user.get("luarmor_unban_token"),
                ban_reason=user.get("luarmor_ban_reason"),
            )

        await self.log_event("reset_hwid", discord_id, f"force={force}")
        return True

    async def resync_user_to_luarmor(self, discord_id: int | str) -> Optional[dict[str, Any]]:
        await self.ensure_initialized()
        if not self.luarmor.enabled:
            raise LuarmorSyncError("Luarmor credentials are not configured.")

        user = await self.get_user(discord_id)
        if user is None:
            remote_user = await self.luarmor.get_user_by_discord_id(discord_id)
            if remote_user is not None and remote_user.get("user_key"):
                await self.luarmor.delete_user(user_key=str(remote_user["user_key"]))
            return None

        local_key = str(user.get("key") or "").strip()
        if local_key:
            await self._sync_luarmor_assignment(discord_id=discord_id, local_key=local_key)
            remote_user = await self.luarmor.get_user_by_discord_id(discord_id)
            if remote_user is not None and await self.is_blacklisted(discord_id):
                await self.luarmor.blacklist_user(
                    user_key=str(remote_user["user_key"]),
                    ban_reason=(await self.get_blacklist_reason(discord_id)) or "Blacklisted by staff.",
                    ban_expire=-1,
                )
                remote_user = await self.luarmor.get_user_by_key(str(remote_user["user_key"]))
            if remote_user is not None:
                await self._apply_remote_user_snapshot(discord_id, remote_user)
            await self.log_event("luarmor_resync", discord_id, local_key)
            return remote_user

        remote_key = await self._resolve_remote_user_key(discord_id, user=user)
        if remote_key:
            await self.luarmor.delete_user(user_key=remote_key)
        await self._set_luarmor_fields(
            discord_id,
            user_key=None,
            status=None,
            identifier=None,
            unban_token=None,
            ban_reason=None,
        )
        await self.log_event("luarmor_resync", discord_id, "removed_remote")
        return None

    async def audit_luarmor(self) -> dict[str, Any]:
        await self.ensure_initialized()
        if not self.luarmor.enabled:
            raise LuarmorSyncError("Luarmor credentials are not configured.")

        local_users = await self.get_all_users()
        remote_users = await self.luarmor.get_users()

        local_by_discord = {
            str(user["discord_id"]): user
            for user in local_users
            if str(user.get("discord_id") or "").strip()
        }
        remote_by_discord = {
            str(user["discord_id"]): user
            for user in remote_users
            if str(user.get("discord_id") or "").strip()
        }

        missing_remote: list[str] = []
        mismatched_keys: list[str] = []
        ban_mismatches: list[str] = []

        for discord_id, local_user in local_by_discord.items():
            local_key = str(local_user.get("key") or "").strip()
            if not local_key:
                continue
            remote_user = remote_by_discord.get(discord_id)
            if remote_user is None:
                missing_remote.append(discord_id)
                continue
            if str(remote_user.get("user_key") or "").strip() != local_key:
                mismatched_keys.append(discord_id)
            local_banned = await self.is_blacklisted(discord_id)
            remote_banned = bool(remote_user.get("banned"))
            if local_banned != remote_banned:
                ban_mismatches.append(discord_id)

        remote_only = [
            str(user.get("discord_id"))
            for user in remote_users
            if str(user.get("discord_id") or "").strip()
            and str(user.get("discord_id")) not in local_by_discord
        ]

        return {
            "local_users": len([user for user in local_users if user.get("key")]),
            "remote_users": len(remote_users),
            "missing_remote": missing_remote,
            "remote_only": remote_only,
            "mismatched_keys": mismatched_keys,
            "ban_mismatches": ban_mismatches,
        }

    async def get_stats(self) -> dict[str, int]:
        await self.ensure_initialized()

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                total_users = int(await connection.fetchval("SELECT COUNT(*) FROM users") or 0)
                available_keys = int(
                    await connection.fetchval("SELECT COUNT(*) FROM keys WHERE used = 0") or 0
                )
                used_keys = int(
                    await connection.fetchval("SELECT COUNT(*) FROM keys WHERE used = 1") or 0
                )
                total_logins = int(
                    await connection.fetchval(
                        "SELECT COUNT(*) FROM analytics WHERE event_type = 'login'"
                    )
                    or 0
                )
            return {
                "total_users": total_users,
                "available_keys": available_keys,
                "used_keys": used_keys,
                "total_keys": available_keys + used_keys,
                "total_logins": total_logins,
            }

        def _run() -> dict[str, int]:
            with self._sqlite_connect() as connection:
                total_users = int(connection.execute("SELECT COUNT(*) FROM users").fetchone()[0])
                available_keys = int(
                    connection.execute("SELECT COUNT(*) FROM keys WHERE used = 0").fetchone()[0]
                )
                used_keys = int(
                    connection.execute("SELECT COUNT(*) FROM keys WHERE used = 1").fetchone()[0]
                )
                total_logins = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM analytics WHERE event_type = 'login'"
                    ).fetchone()[0]
                )
                return {
                    "total_users": total_users,
                    "available_keys": available_keys,
                    "used_keys": used_keys,
                    "total_keys": available_keys + used_keys,
                    "total_logins": total_logins,
                }

        return await asyncio.to_thread(_run)

    async def _init_postgres(self) -> None:
        self._pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=5)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    discord_id TEXT PRIMARY KEY,
                    key TEXT,
                    hwid TEXT,
                    joined_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_login TEXT,
                    luarmor_user_key TEXT,
                    luarmor_status TEXT,
                    luarmor_synced_at TEXT,
                    luarmor_unban_token TEXT,
                    luarmor_ban_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY,
                    created_by TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    used INTEGER DEFAULT 0,
                    used_by TEXT,
                    used_at TEXT
                );

                CREATE TABLE IF NOT EXISTS blacklist (
                    discord_id TEXT PRIMARY KEY,
                    reason TEXT,
                    banned_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS analytics (
                    id BIGSERIAL PRIMARY KEY,
                    event_type TEXT,
                    discord_id TEXT,
                    details TEXT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_whitelist_users_key ON users(key);
                CREATE INDEX IF NOT EXISTS idx_whitelist_keys_used ON keys(used);
                CREATE INDEX IF NOT EXISTS idx_whitelist_analytics_user ON analytics(discord_id);
                CREATE INDEX IF NOT EXISTS idx_whitelist_analytics_event ON analytics(event_type);
                """
            )
            await connection.execute(
                """
                ALTER TABLE users ADD COLUMN IF NOT EXISTS luarmor_user_key TEXT;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS luarmor_status TEXT;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS luarmor_synced_at TEXT;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS luarmor_unban_token TEXT;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS luarmor_ban_reason TEXT;
                """
            )

    def _init_sqlite(self) -> None:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        with self._sqlite_connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    discord_id TEXT PRIMARY KEY,
                    key TEXT,
                    hwid TEXT,
                    joined_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_login TEXT,
                    luarmor_user_key TEXT,
                    luarmor_status TEXT,
                    luarmor_synced_at TEXT,
                    luarmor_unban_token TEXT,
                    luarmor_ban_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY,
                    created_by TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    used INTEGER DEFAULT 0,
                    used_by TEXT,
                    used_at TEXT
                );

                CREATE TABLE IF NOT EXISTS blacklist (
                    discord_id TEXT PRIMARY KEY,
                    reason TEXT,
                    banned_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT,
                    discord_id TEXT,
                    details TEXT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_whitelist_users_key ON users(key);
                CREATE INDEX IF NOT EXISTS idx_whitelist_keys_used ON keys(used);
                CREATE INDEX IF NOT EXISTS idx_whitelist_analytics_user ON analytics(discord_id);
                CREATE INDEX IF NOT EXISTS idx_whitelist_analytics_event ON analytics(event_type);
                """
            )
            self._ensure_sqlite_user_columns(connection)
            connection.commit()

    def _sqlite_connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.sqlite_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_sqlite_user_columns(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(users)").fetchall()
        }
        missing_columns = {
            "luarmor_user_key": "TEXT",
            "luarmor_status": "TEXT",
            "luarmor_synced_at": "TEXT",
            "luarmor_unban_token": "TEXT",
            "luarmor_ban_reason": "TEXT",
        }
        for column_name, column_type in missing_columns.items():
            if column_name not in existing_columns:
                connection.execute(f"ALTER TABLE users ADD COLUMN {column_name} {column_type}")

    async def _set_luarmor_fields(
        self,
        discord_id: int | str,
        *,
        user_key: Optional[str],
        status: Optional[str],
        identifier: Optional[str],
        unban_token: Optional[str],
        ban_reason: Optional[str],
    ) -> None:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)
        synced_at = datetime.now(UTC).isoformat()

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE users
                    SET luarmor_user_key = $2,
                        luarmor_status = $3,
                        luarmor_synced_at = $4,
                        hwid = CASE WHEN $5 IS NULL THEN hwid ELSE NULLIF($5, '') END,
                        luarmor_unban_token = CASE WHEN $6 IS NULL THEN luarmor_unban_token ELSE NULLIF($6, '') END,
                        luarmor_ban_reason = CASE WHEN $7 IS NULL THEN luarmor_ban_reason ELSE NULLIF($7, '') END
                    WHERE discord_id = $1
                    """,
                    discord_id_str,
                    user_key,
                    status,
                    synced_at,
                    identifier,
                    unban_token,
                    ban_reason,
                )
            return

        def _run() -> None:
            with self._sqlite_connect() as connection:
                connection.execute(
                    """
                    UPDATE users
                    SET luarmor_user_key = ?,
                        luarmor_status = ?,
                        luarmor_synced_at = ?,
                        hwid = CASE WHEN ? IS NULL THEN hwid ELSE NULLIF(?, '') END,
                        luarmor_unban_token = CASE WHEN ? IS NULL THEN luarmor_unban_token ELSE NULLIF(?, '') END,
                        luarmor_ban_reason = CASE WHEN ? IS NULL THEN luarmor_ban_reason ELSE NULLIF(?, '') END
                    WHERE discord_id = ?
                    """,
                    (
                        user_key,
                        status,
                        synced_at,
                        identifier,
                        identifier,
                        unban_token,
                        unban_token,
                        ban_reason,
                        ban_reason,
                        discord_id_str,
                    ),
                )
                connection.commit()

        await asyncio.to_thread(_run)

    async def _apply_remote_user_snapshot(
        self,
        discord_id: int | str,
        remote_user: dict[str, Any],
    ) -> None:
        await self._set_luarmor_fields(
            discord_id,
            user_key=None if remote_user.get("user_key") in (None, "") else str(remote_user.get("user_key")),
            status=None if remote_user.get("status") in (None, "") else str(remote_user.get("status")),
            identifier=None if remote_user.get("identifier") is None else str(remote_user.get("identifier")),
            unban_token=None if remote_user.get("unban_token") is None else str(remote_user.get("unban_token")),
            ban_reason=None if remote_user.get("ban_reason") is None else str(remote_user.get("ban_reason")),
        )

    async def _resolve_remote_user_key(
        self,
        discord_id: int | str,
        *,
        user: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        local_user = user if user is not None else await self.get_user(discord_id)
        if local_user is None:
            return None

        luarmor_key = str(local_user.get("luarmor_user_key") or "").strip()
        if luarmor_key:
            return luarmor_key

        local_key = str(local_user.get("key") or "").strip()
        if not local_key or not self.luarmor.enabled:
            return None

        remote_user = await self.luarmor.get_user_by_key(local_key)
        if remote_user is not None and remote_user.get("user_key"):
            return str(remote_user["user_key"])

        remote_user = await self.luarmor.get_user_by_discord_id(discord_id)
        if remote_user is not None and remote_user.get("user_key"):
            return str(remote_user["user_key"])

        return None

    async def _clear_local_hwid(self, discord_id: int | str) -> None:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                await connection.execute(
                    "UPDATE users SET hwid = NULL WHERE discord_id = $1",
                    discord_id_str,
                )
            return

        def _run() -> None:
            with self._sqlite_connect() as connection:
                connection.execute(
                    "UPDATE users SET hwid = NULL WHERE discord_id = ?",
                    (discord_id_str,),
                )
                connection.commit()

        await asyncio.to_thread(_run)

    async def _sync_luarmor_assignment(self, *, discord_id: int | str, local_key: str) -> None:
        if not self.luarmor.enabled:
            return

        user = await self.get_user(discord_id)
        existing_luarmor_key = None if user is None else user.get("luarmor_user_key")
        note = f"{KEY_PREFIX} local key: {local_key}"

        remote_user: Optional[dict[str, Any]] = None
        remote_by_key = await self.luarmor.get_user_by_key(local_key)
        if remote_by_key and remote_by_key.get("user_key"):
            await self.luarmor.update_user(
                user_key=str(remote_by_key["user_key"]),
                discord_id=discord_id,
                note=note,
            )
            remote_user = await self.luarmor.get_user_by_key(str(remote_by_key["user_key"]))
        elif existing_luarmor_key:
            await self.luarmor.update_user(
                user_key=str(existing_luarmor_key),
                discord_id=discord_id,
                note=note,
            )
            remote_user = await self.luarmor.get_user_by_discord_id(discord_id)
        else:
            remote_user = await self.luarmor.get_user_by_discord_id(discord_id)
            if remote_user and remote_user.get("user_key"):
                await self.luarmor.update_user(
                    user_key=str(remote_user["user_key"]),
                    discord_id=discord_id,
                    note=note,
                )
            else:
                created = await self.luarmor.create_user(discord_id=discord_id, note=note)
                remote_user = None
                if isinstance(created, dict) and created.get("user_key"):
                    remote_user = created
                else:
                    remote_user = await self.luarmor.get_user_by_discord_id(discord_id)

        if remote_user is None or not remote_user.get("user_key"):
            raise LuarmorSyncError("Luarmor did not return a user_key for the synced user.")

        await self._apply_remote_user_snapshot(discord_id, remote_user)

    async def _delete_luarmor_assignment(
        self,
        *,
        discord_id: int | str,
        user_key: Optional[str],
    ) -> None:
        if not self.luarmor.enabled:
            return

        target_key = user_key
        if not target_key:
            remote_user = await self.luarmor.get_user_by_discord_id(discord_id)
            if remote_user is None or not remote_user.get("user_key"):
                return
            target_key = str(remote_user["user_key"])

        await self.luarmor.delete_user(user_key=str(target_key))

    async def _rollback_redeem(
        self,
        discord_id: int | str,
        new_key: str,
        previous_key: Any,
    ) -> None:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)
        previous_key_str = None if previous_key in (None, "") else str(previous_key)

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        "UPDATE keys SET used = 0, used_by = NULL, used_at = NULL WHERE key = $1",
                        new_key,
                    )
                    if previous_key_str:
                        await connection.execute(
                            """
                            INSERT INTO users (discord_id, key, joined_at)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (discord_id) DO UPDATE SET key = EXCLUDED.key
                            """,
                            discord_id_str,
                            previous_key_str,
                            datetime.now(UTC).isoformat(),
                        )
                        await connection.execute(
                            """
                            UPDATE keys SET used = 1, used_by = $1, used_at = $2 WHERE key = $3
                            """,
                            discord_id_str,
                            datetime.now(UTC).isoformat(),
                            previous_key_str,
                        )
                    else:
                        await connection.execute(
                            "DELETE FROM users WHERE discord_id = $1",
                            discord_id_str,
                        )
            return

        def _run() -> None:
            with self._sqlite_connect() as connection:
                connection.execute(
                    "UPDATE keys SET used = 0, used_by = NULL, used_at = NULL WHERE key = ?",
                    (new_key,),
                )
                if previous_key_str:
                    connection.execute(
                        """
                        INSERT INTO users (discord_id, key, joined_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(discord_id) DO UPDATE SET key = excluded.key
                        """,
                        (discord_id_str, previous_key_str, datetime.now(UTC).isoformat()),
                    )
                    connection.execute(
                        "UPDATE keys SET used = 1, used_by = ?, used_at = ? WHERE key = ?",
                        (discord_id_str, datetime.now(UTC).isoformat(), previous_key_str),
                    )
                else:
                    connection.execute("DELETE FROM users WHERE discord_id = ?", (discord_id_str,))
                connection.commit()

        await asyncio.to_thread(_run)

    async def _rollback_unwhitelist(
        self,
        discord_id: int | str,
        released_key: str,
        previous_user: dict[str, Any],
    ) -> None:
        await self.ensure_initialized()
        discord_id_str = str(discord_id)
        luarmor_user_key = previous_user.get("luarmor_user_key")
        luarmor_status = previous_user.get("luarmor_status")
        luarmor_synced_at = previous_user.get("luarmor_synced_at")
        luarmor_unban_token = previous_user.get("luarmor_unban_token")
        luarmor_ban_reason = previous_user.get("luarmor_ban_reason")

        if self._pool is not None:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        """
                        UPDATE users
                        SET key = $2,
                            luarmor_user_key = $3,
                            luarmor_status = $4,
                            luarmor_synced_at = $5,
                            luarmor_unban_token = $6,
                            luarmor_ban_reason = $7
                        WHERE discord_id = $1
                        """,
                        discord_id_str,
                        released_key,
                        luarmor_user_key,
                        luarmor_status,
                        luarmor_synced_at,
                        luarmor_unban_token,
                        luarmor_ban_reason,
                    )
                    await connection.execute(
                        "UPDATE keys SET used = 1, used_by = $1, used_at = $2 WHERE key = $3",
                        discord_id_str,
                        datetime.now(UTC).isoformat(),
                        released_key,
                    )
            return

        def _run() -> None:
            with self._sqlite_connect() as connection:
                connection.execute(
                    """
                    UPDATE users
                    SET key = ?,
                        luarmor_user_key = ?,
                        luarmor_status = ?,
                        luarmor_synced_at = ?,
                        luarmor_unban_token = ?,
                        luarmor_ban_reason = ?
                    WHERE discord_id = ?
                    """,
                    (
                        released_key,
                        luarmor_user_key,
                        luarmor_status,
                        luarmor_synced_at,
                        luarmor_unban_token,
                        luarmor_ban_reason,
                        discord_id_str,
                    ),
                )
                connection.execute(
                    "UPDATE keys SET used = 1, used_by = ?, used_at = ? WHERE key = ?",
                    (discord_id_str, datetime.now(UTC).isoformat(), released_key),
                )
                connection.commit()

        await asyncio.to_thread(_run)


def build_store_from_env(sqlite_path: Path) -> WhitelistStore:
    key_provider = os.getenv("KEY_PROVIDER") or os.getenv("WHITELIST_KEY_PROVIDER")
    luarmor_key_days_raw = (os.getenv("LUARMOR_KEY_DAYS") or "").strip()
    luarmor_key_days = int(luarmor_key_days_raw) if luarmor_key_days_raw.isdigit() else None
    return WhitelistStore(
        sqlite_path,
        database_url=os.getenv("DATABASE_URL"),
        luarmor_api_key=os.getenv("LUARMOR_API_KEY"),
        luarmor_project_id=os.getenv("LUARMOR_PROJECT_ID"),
        key_provider=key_provider,
        luarmor_key_days=luarmor_key_days,
    )
