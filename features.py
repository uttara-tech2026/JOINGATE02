"""
All bot logic: database, per-destination acceptors, join-request handler, admin panel.

NEW in this version (destination-based panel)
  • Home: 📡 Destinations · 💬 Massages · ✅ Acceptors · 👤 User info · 📊 Universal status
  • Destinations are registered automatically (bot made admin / first join request).
    The list shows  <status dot> Title · members | requested(waiting) | left
      🔴 no activity for 7 days            🟢 requests are being accepted
      🟡 other activity (joins/leaves, requests while stopped)
      🔵 only new posts – no requests, nothing accepted
  • Every destination has its OWN accept switch and timer (fixed ± jitter / random range).
  • ✅ Acceptors = global control: start/stop all, timer for all, per-destination toggles.
  • Existing features kept: message library (primary welcome), broadcast, user lists, analytics.
"""
import asyncio
from collections import deque
import csv
import io
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from html import escape
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import BaseMiddleware, Bot, F, Router
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandObject, CommandStart, Filter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ChatJoinRequest,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

log = logging.getLogger("features")

ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_ID", "").replace(" ", "").split(",") if x.strip().isdigit()
}

# Timezone used for "Today / Yesterday / This week / This month" (e.g. Asia/Kolkata)
TZ_NAME = os.getenv("TIMEZONE", "UTC")
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:  # unknown zone or missing tzdata
    TZ, TZ_NAME = timezone.utc, "UTC"

DEFAULT_WELCOME = (
    "👋 Hello {mention}!\n\n"
    "Thanks for requesting to join <b>{chat_title}</b>.\n"
    "Your request is in the queue and will be approved shortly."
)

JITTER_PRESETS = [0, 10, 20, 30, 50]  # percent

DEFAULTS = {
    "running": "0",            # 1 = approving, 0 = stopped
    "interval_mode": "fixed",  # fixed | random
    "interval": "10",          # fixed mode: base seconds
    "jitter_pct": "20",        # fixed mode: ± percent
    "rand_min": "5",           # random mode: min seconds
    "rand_max": "20",          # random mode: max seconds
    "welcome_enabled": "1",
    "active_welcome": "0",     # id of the welcome message in use
    "post_enabled": "0",       # post-request message (sent after approval) ON/OFF
    "post_message": "0",       # id of the default post-request message
    "pre_delay": "0",          # default timer (s) for the pre-request message
    "post_delay": "0",         # default timer (s) for the post-request message
    "pre_gap_min": "0",        # time gap between two pre-request messages (random range, s)
    "pre_gap_max": "0",
    "post_gap_min": "0",       # time gap between two post-request messages (random range, s)
    "post_gap_max": "0",
    "master_log_on": "0",      # 1 = post the activity feed
    "master_log_chat": "",     # chat id that receives the feed
    "master_log_title": "",
    "master_log_msg": "",      # "chat_id:message_id" of the live log message
    "master_log_err": "",      # last send error (shown in the panel)
    "master_log_at": "0",      # last successful update (unix time)
    "bc_exclude_blocked": "0", # 1 = broadcasts skip members who blocked / deleted the bot
}

MEDIA_LABEL = {"photo": "🖼 Photo", "video": "🎬 Video", "animation": "🎞 GIF", "": "— none"}

router = Router()

# ───────────────────────── Database ─────────────────────────
# DATABASE_URL examples:
#   postgresql://user:pass@host:5432/dbname      (PostgreSQL – e.g. Railway)
#   sqlite:///bot_data.db                        (local SQLite file)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
IS_PG = DATABASE_URL.startswith(("postgres://", "postgresql://", "postgresql+asyncpg://"))
SQLITE_PATH = "bot_data.db"

if IS_PG:
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)
else:
    _p = DATABASE_URL
    for _prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if _p.startswith(_prefix):
            _p = _p[len(_prefix):]
            break
    SQLITE_PATH = _p or "bot_data.db"

_pool = None  # asyncpg pool (PostgreSQL only)


def _pg_sql(sql: str) -> str:
    """Convert '?' placeholders to $1, $2 ... for asyncpg."""
    out, n = [], 0
    for ch in sql:
        if ch == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(ch)
    return "".join(out)


async def _pg(method: str, sql: str, *args):
    """Run a query on the pool. If PostgreSQL says a cached plan is stale (the table was altered by
    another connection/instance), run it once more – the retry prepares a fresh plan."""
    import asyncpg
    for attempt in (1, 2):
        try:
            return await getattr(_pool, method)(sql, *args)
        except asyncpg.exceptions.InvalidCachedStatementError:
            if attempt == 2:
                raise
            log.warning("Stale cached statement, retrying once")


async def db_execute(sql: str, *args) -> int:
    """Run a write statement, return affected row count."""
    if IS_PG:
        status = await _pg("execute", _pg_sql(sql), *args)
        tail = status.split()[-1] if status else ""
        return int(tail) if tail.isdigit() else 0
    async with aiosqlite.connect(SQLITE_PATH) as db:
        cur = await db.execute(sql, args)
        await db.commit()
        return cur.rowcount


async def db_insert_id(sql: str, *args) -> int:
    """Run an INSERT and return the new row id."""
    if IS_PG:
        row = await _pg("fetchrow", _pg_sql(sql) + " RETURNING id", *args)
        return int(row["id"])
    async with aiosqlite.connect(SQLITE_PATH) as db:
        cur = await db.execute(sql, args)
        await db.commit()
        return int(cur.lastrowid)


async def db_fetchall(sql: str, *args) -> list[dict]:
    if IS_PG:
        rows = await _pg("fetch", _pg_sql(sql), *args)
        return [dict(r) for r in rows]
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, args) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_fetchone(sql: str, *args) -> dict | None:
    rows = await db_fetchall(sql, *args)
    return rows[0] if rows else None


async def init_db():
    global _pool
    if IS_PG:
        import asyncpg
        _pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=5,
            statement_cache_size=0,                 # no cached plans → schema updates can't break running queries
            max_inactive_connection_lifetime=60,    # drop idle connections before Neon suspends them
        )
        pk, big, real = "BIGSERIAL PRIMARY KEY", "BIGINT", "DOUBLE PRECISION"
        log.info("Using PostgreSQL database")
    else:
        pk, big, real = "INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "REAL"
        await db_execute("PRAGMA journal_mode=WAL")
        log.info("Using SQLite database at %s", SQLITE_PATH)

    await db_execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS queue (
            id {pk},
            chat_id {big} NOT NULL,
            chat_title TEXT,
            user_id {big} NOT NULL,
            user_chat_id {big},
            username TEXT,
            full_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            created_at {real} NOT NULL,
            processed_at {real},
            UNIQUE(chat_id, user_id)
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, id)")
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS welcomes (
            id {pk},
            name TEXT NOT NULL,
            caption TEXT NOT NULL DEFAULT '',
            media_type TEXT NOT NULL DEFAULT '',
            media_id TEXT NOT NULL DEFAULT '',
            created_at {real} NOT NULL
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS users (
            user_id {big} PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            started INTEGER NOT NULL DEFAULT 0,
            subscribed INTEGER NOT NULL DEFAULT 1,
            first_seen {real} NOT NULL,
            last_seen {real} NOT NULL,
            unsub_at {real}
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS broadcasts (
            id {pk},
            created_at {real} NOT NULL,
            total INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            blocked INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'running'
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS destinations (
            chat_id {big} PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            chat_type TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 1,
            running INTEGER NOT NULL DEFAULT 0,
            iv_mode TEXT NOT NULL DEFAULT 'fixed',
            iv_base {real} NOT NULL DEFAULT 10,
            iv_jitter INTEGER NOT NULL DEFAULT 20,
            iv_min {real} NOT NULL DEFAULT 5,
            iv_max {real} NOT NULL DEFAULT 20,
            members INTEGER NOT NULL DEFAULT 0,
            members_at {real} NOT NULL DEFAULT 0,
            added_at {real} NOT NULL,
            last_request_at {real},
            last_approve_at {real},
            last_member_event_at {real},
            last_post_at {real}
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS events (
            id {pk},
            chat_id {big} NOT NULL,
            user_id {big},
            kind TEXT NOT NULL,
            ts {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_events_chat ON events(chat_id, kind, ts)")
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS links (
            id {pk},
            chat_id {big} NOT NULL,
            name TEXT NOT NULL,
            invite_link TEXT NOT NULL,
            created_at {real} NOT NULL,
            revoked INTEGER NOT NULL DEFAULT 0
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS link_events (
            id {pk},
            chat_id {big} NOT NULL,
            link_id INTEGER NOT NULL,
            user_id {big},
            kind TEXT NOT NULL,
            ts {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_link_events ON link_events(link_id, kind, ts)")
    await db_execute("CREATE INDEX IF NOT EXISTS idx_link_events_user ON link_events(chat_id, user_id, kind)")
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS dest_msg (
            chat_id {big} PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            welcome_id INTEGER NOT NULL DEFAULT 0,
            delay {real} NOT NULL DEFAULT 0
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS deliveries (
            id {pk},
            chat_id {big} NOT NULL,
            user_id {big},
            status TEXT NOT NULL,
            ts {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_deliveries ON deliveries(chat_id, ts)")
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS dest_msgs (
            chat_id {big} NOT NULL,
            kind TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            welcome_id INTEGER NOT NULL DEFAULT 0,
            delay {real} NOT NULL DEFAULT 0,
            gap_min {real} NOT NULL DEFAULT 0,
            gap_max {real} NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, kind)
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS msg_log (
            id {pk},
            chat_id {big} NOT NULL,
            user_id {big},
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            ts {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_msg_log ON msg_log(chat_id, kind, ts)")
    for col in ("gap_min", "gap_max"):  # databases created by the previous version
        try:
            await db_execute(f"ALTER TABLE dest_msgs ADD COLUMN {col} {real} NOT NULL DEFAULT 0")
        except Exception:
            pass  # column already exists
    try:  # add-on: "blocked / deleted the bot" flag (databases created by the previous version)
        await db_execute("ALTER TABLE users ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass  # column already exists
    await db_execute(  # add-on: buttons attached to library messages
        f"""CREATE TABLE IF NOT EXISTS msg_buttons (
            id {pk},
            welcome_id {big} NOT NULL,
            label TEXT NOT NULL,
            url TEXT NOT NULL,
            tracked INTEGER NOT NULL DEFAULT 1,
            pos INTEGER NOT NULL DEFAULT 0,
            created_at {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_msg_buttons ON msg_buttons(welcome_id, pos)")
    await db_execute(  # one row per (button, user): n = how many times that user clicked
        f"""CREATE TABLE IF NOT EXISTS btn_clicks (
            button_id {big} NOT NULL,
            user_id {big} NOT NULL,
            n INTEGER NOT NULL DEFAULT 0,
            first_ts {real} NOT NULL,
            last_ts {real} NOT NULL,
            PRIMARY KEY (button_id, user_id)
        )"""
    )
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS scheduled_msgs (
            id {pk},
            chat_id {big} NOT NULL,
            user_id {big} NOT NULL,
            user_chat_id {big} NOT NULL,
            full_name TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            chat_title TEXT NOT NULL DEFAULT '',
            due_at {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_sched_due ON scheduled_msgs(due_at)")
    await db_execute(
        f"""CREATE TABLE IF NOT EXISTS chat_msgs (
            id {pk},
            chat_id {big} NOT NULL,
            message_id {big} NOT NULL,
            ts {real} NOT NULL
        )"""
    )
    await db_execute("CREATE INDEX IF NOT EXISTS idx_chat_msgs ON chat_msgs(chat_id, ts)")
    for k, v in DEFAULTS.items():
        await db_execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
            k, v,
        )
    await backfill_users()
    await migrate_welcomes()
    await migrate_destinations()
    await migrate_msg_tables()


async def close_db():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def get_setting(key: str) -> str:
    row = await db_fetchone("SELECT value FROM settings WHERE key=?", key)
    return row["value"] if row else DEFAULTS[key]


async def set_setting(key: str, value: str):
    await db_execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        key, value,
    )


async def enqueue(req: ChatJoinRequest):
    u = req.from_user
    await db_execute(
        """INSERT INTO queue (chat_id, chat_title, user_id, user_chat_id, username,
                              full_name, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
           ON CONFLICT(chat_id, user_id) DO UPDATE SET
             status='pending', error=NULL, processed_at=NULL,
             chat_title=excluded.chat_title, user_chat_id=excluded.user_chat_id,
             username=excluded.username, full_name=excluded.full_name,
             created_at=excluded.created_at""",
        req.chat.id, req.chat.title or "", u.id, req.user_chat_id,
        u.username, u.full_name, time.time(),
    )


async def next_pending(chat_id: int):
    return await db_fetchone(
        "SELECT * FROM queue WHERE status='pending' AND chat_id=? ORDER BY id LIMIT 1", chat_id
    )


async def mark(row_id: int, status: str, error: str | None = None):
    await db_execute(
        "UPDATE queue SET status=?, error=?, processed_at=? WHERE id=?",
        status, error, time.time(), row_id,
    )


async def counts(chat_id: int | None = None) -> dict:
    out = {"pending": 0, "approved": 0, "failed": 0}
    if chat_id is None:
        rows = await db_fetchall("SELECT status, COUNT(*) AS n FROM queue GROUP BY status")
    else:
        rows = await db_fetchall(
            "SELECT status, COUNT(*) AS n FROM queue WHERE chat_id=? GROUP BY status", chat_id
        )
    for r in rows:
        out[r["status"]] = int(r["n"])
    return out


async def pending_by_chat():
    rows = await db_fetchall(
        "SELECT chat_id, COALESCE(MAX(chat_title), '') AS title, COUNT(*) AS n FROM queue "
        "WHERE status='pending' GROUP BY chat_id ORDER BY COUNT(*) DESC"
    )
    return [(r["chat_id"], r["title"], int(r["n"])) for r in rows]


async def bulk_update(sql: str) -> int:
    return await db_execute(sql)


async def last_chat_title() -> str:
    row = await db_fetchone(
        "SELECT chat_title FROM queue WHERE chat_title <> '' ORDER BY id DESC LIMIT 1"
    )
    return row["chat_title"] if row else "Sample Channel"


# ───────────────────────── Users / analytics ─────────────────────────
async def touch_user(user, started: bool):
    """Remember a user. started=True means they interacted with the bot (can receive DMs)."""
    now = time.time()
    await db_execute(
        """INSERT INTO users (user_id, username, full_name, started, subscribed, first_seen, last_seen)
           VALUES (?, ?, ?, ?, 1, ?, ?)
           ON CONFLICT(user_id) DO UPDATE SET
             username=excluded.username, full_name=excluded.full_name,
             last_seen=excluded.last_seen,
             started=CASE WHEN excluded.started=1 THEN 1 ELSE users.started END,
             blocked=CASE WHEN excluded.started=1 THEN 0 ELSE users.blocked END""",
        user.id, user.username, user.full_name, 1 if started else 0, now, now,
    )


async def backfill_users():
    """Everyone who ever requested to join is a subscriber until they unsubscribe."""
    await db_execute(
        """INSERT INTO users (user_id, username, full_name, started, subscribed, first_seen, last_seen)
           SELECT user_id, MAX(username), MAX(full_name), 0, 1, MIN(created_at), MAX(created_at)
           FROM queue
           WHERE user_id NOT IN (SELECT user_id FROM users)
           GROUP BY user_id"""
    )


async def set_started(user_id: int, started: bool):
    await db_execute("UPDATE users SET started=? WHERE user_id=?", 1 if started else 0, user_id)


async def user_overview() -> dict:
    row = await db_fetchone(
        """SELECT COUNT(*) AS total,
                  COALESCE(SUM(CASE WHEN started=1 THEN 1 ELSE 0 END), 0) AS started,
                  COALESCE(SUM(CASE WHEN subscribed=1 THEN 1 ELSE 0 END), 0) AS audience,
                  COALESCE(SUM(CASE WHEN subscribed=0 THEN 1 ELSE 0 END), 0) AS unsub,
                  COALESCE(SUM(CASE WHEN started=1 AND subscribed=0 THEN 1 ELSE 0 END), 0) AS unsub_reach,
                  COALESCE(SUM(CASE WHEN started=1 AND subscribed=1 THEN 1 ELSE 0 END), 0) AS reach_sub,
                  COALESCE(SUM(CASE WHEN blocked=1 THEN 1 ELSE 0 END), 0) AS blocked,
                  COALESCE(SUM(CASE WHEN blocked=1 AND subscribed=1 THEN 1 ELSE 0 END), 0) AS sub_blocked,
                  COALESCE(SUM(CASE WHEN blocked=1 AND subscribed=0 THEN 1 ELSE 0 END), 0) AS unsub_blocked
           FROM users"""
    )
    return {k: int(v or 0) for k, v in row.items()}


def time_windows():
    now = datetime.now(TZ)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    week = today - timedelta(days=today.weekday())  # Monday
    month = today.replace(day=1)
    end = (now + timedelta(days=1)).timestamp()
    return [
        ("📅 Today", today.timestamp(), end),
        ("🕘 Yesterday", yesterday.timestamp(), today.timestamp()),
        ("🗓 This week", week.timestamp(), end),
        ("📆 This month", month.timestamp(), end),
        ("♾ All time", 0.0, end),
    ]


async def window_stats(t0: float, t1: float) -> dict:
    out = {"pending": 0, "approved": 0, "failed": 0, "users": 0, "bot_users": 0}
    for r in await db_fetchall(
        "SELECT status, COUNT(*) AS n FROM queue WHERE created_at >= ? AND created_at < ? GROUP BY status",
        t0, t1,
    ):
        out[r["status"]] = int(r["n"])
    row = await db_fetchone(
        """SELECT COUNT(DISTINCT q.user_id) AS total,
                  COUNT(DISTINCT CASE WHEN u.started=1 THEN q.user_id END) AS started
           FROM queue q LEFT JOIN users u ON u.user_id = q.user_id
           WHERE q.created_at >= ? AND q.created_at < ?""",
        t0, t1,
    )
    out["users"], out["bot_users"] = int(row["total"] or 0), int(row["started"] or 0)
    return out


def fmt_ts(ts) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%d %b %Y, %H:%M") if ts else "-"


# ───────────────────────── Welcome storage ─────────────────────────
async def wl_create(name: str, caption: str = DEFAULT_WELCOME,
                    media_type: str = "", media_id: str = "") -> int:
    return await db_insert_id(
        "INSERT INTO welcomes (name, caption, media_type, media_id, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        name, caption, media_type, media_id, time.time(),
    )


async def wl_get(wid: int) -> dict | None:
    return await db_fetchone("SELECT * FROM welcomes WHERE id=?", wid)


async def wl_all() -> list[dict]:
    return await db_fetchall("SELECT * FROM welcomes ORDER BY id")


async def wl_update(wid: int, **fields):
    allowed = {"name", "caption", "media_type", "media_id"}
    keys = [k for k in fields if k in allowed]
    if not keys:
        return
    sets = ", ".join(f"{k}=?" for k in keys)
    await db_execute(f"UPDATE welcomes SET {sets} WHERE id=?", *[fields[k] for k in keys], wid)


async def wl_delete(wid: int):
    await db_execute("DELETE FROM btn_clicks WHERE button_id IN (SELECT id FROM msg_buttons WHERE welcome_id=?)", wid)
    await db_execute("DELETE FROM msg_buttons WHERE welcome_id=?", wid)
    await db_execute("DELETE FROM welcomes WHERE id=?", wid)
    if await get_setting("active_welcome") == str(wid):
        await set_setting("active_welcome", "0")  # no primary until admin picks another


async def get_active_welcome() -> dict | None:
    try:
        wid = int(await get_setting("active_welcome"))
    except ValueError:
        return None
    return await wl_get(wid) if wid else None


async def migrate_welcomes():
    """First run (or upgrade from the old single-welcome version)."""
    if await db_fetchone("SELECT id FROM welcomes LIMIT 1"):
        return
    old_t = await db_fetchone("SELECT value FROM settings WHERE key='welcome_text'")
    old_p = await db_fetchone("SELECT value FROM settings WHERE key='welcome_photo'")
    caption = old_t["value"] if old_t else DEFAULT_WELCOME
    photo = old_p["value"] if old_p else ""
    wid = await wl_create("Default", caption, "photo" if photo else "", photo)
    await set_setting("active_welcome", str(wid))


# ───────────────────────── Welcome delivery ─────────────────────────
def render_welcome(template: str, user, chat_title: str) -> str:
    full_name = escape(user.full_name or "friend")
    username = f"@{user.username}" if getattr(user, "username", None) else full_name
    mention = f'<a href="tg://user?id={user.id}">{full_name}</a>'
    return (
        template.replace("{first_name}", escape(user.first_name or "friend"))
        .replace("{full_name}", full_name)
        .replace("{username}", escape(username))
        .replace("{mention}", mention)
        .replace("{chat_title}", escape(chat_title or "the group"))
    )


async def track_msg(chat_id: int, message_id: int) -> None:
    await db_execute(
        "INSERT INTO chat_msgs (chat_id, message_id, ts) VALUES (?, ?, ?)",
        chat_id, message_id, time.time(),
    )


async def track_out(chat_id: int, ids: list) -> None:
    """Remember ids of messages we sent to a user (so the chat can be wiped on unsubscribe)."""
    if not ids or chat_id in ADMIN_IDS:
        return
    try:
        for i in ids:
            await track_msg(chat_id, i)
    except Exception:
        log.exception("track_out failed")


async def wipe_chat(bot: Bot, chat_id: int, extra=()) -> int:
    """Delete every tracked message (ours and the user's) in a private chat. Telegram only
    allows deleting messages younger than 48h, so older ones stay."""
    cut = time.time() - 47 * 3600
    rows = await db_fetchall("SELECT message_id FROM chat_msgs WHERE chat_id=? AND ts>=?", chat_id, cut)
    ids = sorted({int(r["message_id"]) for r in rows} | {int(x) for x in extra})
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        try:
            await bot.delete_messages(chat_id, batch)
        except Exception:
            for mid in batch:
                try:
                    await bot.delete_message(chat_id, mid)
                except Exception:
                    pass
    await db_execute("DELETE FROM chat_msgs WHERE chat_id=?", chat_id)
    return len(ids)


async def deliver_welcome(bot: Bot, chat_id: int, w: dict, user, chat_title: str,
                          reply_markup=None) -> bool:
    """Send a message exactly as the receiver will see it. Returns False if empty."""
    text = render_welcome(w.get("caption") or "", user, chat_title).strip()
    mtype, mid = w.get("media_type") or "", w.get("media_id") or ""
    ids: list = []
    reply_markup = await attach_buttons(bot, w, reply_markup)  # add-on: admin-defined buttons

    if mid:
        # Telegram caption limit is 1024 → longer text is sent as a follow-up message
        split = bool(text) and len(text) > 1024
        caption = None if split else (text or None)
        mk = None if split else reply_markup
        if mtype == "photo":
            m = await bot.send_photo(chat_id, mid, caption=caption, reply_markup=mk)
        elif mtype == "video":
            m = await bot.send_video(chat_id, mid, caption=caption, reply_markup=mk)
        elif mtype == "animation":
            m = await bot.send_animation(chat_id, mid, caption=caption, reply_markup=mk)
        else:
            return False
        ids.append(m.message_id)
        if split:
            m2 = await bot.send_message(chat_id, text, disable_web_page_preview=True, reply_markup=reply_markup)
            ids.append(m2.message_id)
        await track_out(chat_id, ids)
        return True
    if text:
        m = await bot.send_message(chat_id, text, disable_web_page_preview=True, reply_markup=reply_markup)
        await track_out(chat_id, [m.message_id])
        return True
    return False


# ---- pre-request / post-request messages ----
MAX_MSG_DELAY = {"pre": 240, "post": 86400}  # pre: Telegram lets us DM a requester ~5 min only
GLOBAL_KEYS = {
    "pre": {"on": "welcome_enabled", "msg": "active_welcome", "delay": "pre_delay",
            "gmin": "pre_gap_min", "gmax": "pre_gap_max",
            "name": "Pre-request message", "icon": "📥"},
    "post": {"on": "post_enabled", "msg": "post_message", "delay": "post_delay",
             "gmin": "post_gap_min", "gmax": "post_gap_max",
             "name": "Post-request message", "icon": "📤"},
}
_bg_tasks: set = set()


def delay_txt(v: float) -> str:
    return "instant" if v <= 0 else fmt_duration(v)


async def migrate_msg_tables():
    """Copy data of the older single-kind tables into the new per-kind ones (runs once)."""
    if not await db_fetchone("SELECT id FROM msg_log LIMIT 1"):
        for r in await db_fetchall("SELECT chat_id, user_id, status, ts FROM deliveries"):
            await db_execute(
                "INSERT INTO msg_log (chat_id, user_id, kind, status, ts) VALUES (?, ?, 'pre', ?, ?)",
                r["chat_id"], r["user_id"], r["status"], r["ts"],
            )
    for r in await db_fetchall("SELECT * FROM dest_msg"):
        await db_execute(
            "INSERT INTO dest_msgs (chat_id, kind, enabled, welcome_id, delay) VALUES (?, 'pre', ?, ?, ?) "
            "ON CONFLICT(chat_id, kind) DO NOTHING",
            r["chat_id"], r["enabled"], r["welcome_id"], r["delay"],
        )


def gap_txt(lo, hi) -> str:
    lo, hi = float(lo or 0), float(hi or 0)
    if hi <= 0:
        return "none"
    return f"{lo:g}s" if lo == hi else f"{lo:g}–{hi:g}s random"


def parse_gap(text: str):
    """'5-20' -> (5, 20) · '10' -> (10, 10) · '0' -> (0, 0). None if invalid."""
    raw = text.strip().replace(",", ".").replace("–", "-").replace(" ", "-")
    try:
        nums = [float(p) for p in raw.split("-") if p]
    except ValueError:
        return None
    if len(nums) == 1:
        lo = hi = nums[0]
    elif len(nums) == 2:
        lo, hi = nums
    else:
        return None
    return (lo, hi) if 0 <= lo <= hi <= 3600 else None


async def dm_get(chat_id: int, kind: str = "pre") -> dict:
    row = await db_fetchone("SELECT * FROM dest_msgs WHERE chat_id=? AND kind=?", chat_id, kind)
    if row:
        return row
    g = GLOBAL_KEYS[kind]
    return {
        "chat_id": chat_id, "kind": kind, "enabled": 1, "welcome_id": 0,
        "delay": float(await get_setting(g["delay"]) or 0),
        "gap_min": float(await get_setting(g["gmin"]) or 0),
        "gap_max": float(await get_setting(g["gmax"]) or 0),
    }


async def dm_set(chat_id: int, kind: str, **kv):
    base = await dm_get(chat_id, kind)
    await db_execute(
        "INSERT INTO dest_msgs (chat_id, kind, enabled, welcome_id, delay, gap_min, gap_max) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(chat_id, kind) DO NOTHING",
        chat_id, kind, base["enabled"], base["welcome_id"], base["delay"], base["gap_min"], base["gap_max"],
    )
    for k, v in kv.items():
        assert k in ("enabled", "welcome_id", "delay", "gap_min", "gap_max")
        await db_execute(f"UPDATE dest_msgs SET {k}=? WHERE chat_id=? AND kind=?", v, chat_id, kind)


_slot_last: dict[tuple[int, str], float] = {}  # (destination, kind) -> last reserved send time
_post_sent_at: dict[int, float] = {}            # destination -> when the last post-request message left


async def reserve_slot(dest_id: int, kind: str, due: float, max_wait: float | None = None):
    """Time gap: returns the moment this message may go out. After one message the next one waits a
    random time inside the admin's gap range. None = would wait longer than max_wait."""
    dm = await dm_get(dest_id, kind)
    lo, hi = float(dm["gap_min"] or 0), float(dm["gap_max"] or 0)
    if hi <= 0:
        return due
    last = _slot_last.get((dest_id, kind))
    if last is None:  # after a restart: continue from what the database knows
        r = await db_fetchone(
            "SELECT MAX(ts) AS t FROM msg_log WHERE chat_id=? AND kind=?", dest_id, kind
        )
        last = float(r["t"] or 0) if r else 0.0
        if kind == "post":
            r = await db_fetchone("SELECT MAX(due_at) AS t FROM scheduled_msgs WHERE chat_id=?", dest_id)
            last = max(last, float(r["t"] or 0) if r else 0.0)
    slot = max(due, last + random.uniform(lo, hi))
    if max_wait is not None and slot - time.time() > max_wait:
        return None
    _slot_last[(dest_id, kind)] = slot
    return slot


async def log_delivery(dest_id: int, user_id: int, kind: str, status: str):
    await db_execute(
        "INSERT INTO msg_log (chat_id, user_id, kind, status, ts) VALUES (?, ?, ?, ?, ?)",
        dest_id, user_id, kind, status, time.time(),
    )


async def resolve_msg(dest_id: int, kind: str):
    """(message dict, delay seconds) to send for this destination, or None if switched off."""
    g = GLOBAL_KEYS[kind]
    if await get_setting(g["on"]) != "1":
        return None
    dm = await dm_get(dest_id, kind)
    if not dm["enabled"]:
        return None
    w = await wl_get(int(dm["welcome_id"])) if dm["welcome_id"] else None
    if not w:
        gid = int(await get_setting(g["msg"]) or 0)
        w = await wl_get(gid) if gid else None
    if not w:
        return None
    return w, float(dm["delay"] or 0)


async def _got_message(chat_id: int, user_id: int) -> bool:
    """Has this person received (or is about to receive) a message from this destination?"""
    if await db_fetchone(
        "SELECT 1 AS x FROM msg_log WHERE chat_id=? AND user_id=? AND status='sent' LIMIT 1", chat_id, user_id
    ):
        return True
    return bool(await db_fetchone(
        "SELECT 1 AS x FROM scheduled_msgs WHERE chat_id=? AND user_id=? LIMIT 1", chat_id, user_id
    ))


# ---- pre-request message (right after the request) ----
async def _send_welcome_now(bot: Bot, chat_id: int, user, chat_title: str, w: dict, dest_id):
    status = "sent"
    try:
        if not await deliver_welcome(bot, chat_id, w, user, chat_title):
            return  # empty message – nothing sent, nothing logged
    except TelegramForbiddenError as e:
        status = "blocked"
        log.warning("Could not send pre-request message to %s: %s", user.id, e)
        await note_forbidden(user.id, e)
    except TelegramBadRequest as e:
        status = "failed"
        log.warning("Could not send pre-request message to %s: %s", user.id, e)
    except Exception:
        status = "failed"
        log.exception("Pre-request message failed")
    if status == "sent":
        await note_ok(user.id)
    ttl = escape(chat_title or "")
    icon = {"sent": "📨", "blocked": "🚫", "failed": "⚠️"}[status]
    mlog(f"{icon} Pre-request message {status} · {uname(user)} → <b>{ttl}</b>", kind="pre")
    if dest_id is not None:
        await log_delivery(dest_id, user.id, "pre", status)


async def _send_welcome_later(delay: float, *args):
    await asyncio.sleep(delay)
    await _send_welcome_now(*args)


async def send_welcome(bot: Bot, chat_id: int, user, chat_title: str, dest_id: int | None = None):
    """Pre-request message. Uses the destination's own message/timer when set."""
    if dest_id is None:
        if await get_setting("welcome_enabled") != "1":
            return
        w, delay = await get_active_welcome(), 0.0
        if not w:
            return
    else:
        res = await resolve_msg(dest_id, "pre")
        if not res:
            return
        w, delay = res[0], min(res[1], MAX_MSG_DELAY["pre"])
        slot = await reserve_slot(dest_id, "pre", time.time() + delay, max_wait=MAX_MSG_DELAY["pre"] + 30)
        if slot is None:  # the gap queue is too long: Telegram would no longer let us DM this requester
            log.warning("Pre-request message for %s skipped: gap queue longer than Telegram's window", user.id)
            await log_delivery(dest_id, user.id, "pre", "failed")
            mlog(f"⚠️ Pre-request message failed · {uname(user)} → <b>{escape(chat_title or '')}</b> "
                 "· gap queue too long", kind="pre")
            return
        delay = max(0.0, slot - time.time())
    if delay > 0:
        t = asyncio.create_task(_send_welcome_later(delay, bot, chat_id, user, chat_title, w, dest_id))
        _bg_tasks.add(t)
        t.add_done_callback(_bg_tasks.discard)
    else:
        await _send_welcome_now(bot, chat_id, user, chat_title, w, dest_id)


# ---- post-request message (after approval, with an Unsubscribe button) ----
async def on_real_join(chat_id: int, user_id: int) -> None:
    """Telegram confirmed the person joined. If they came through a join request (approved by the
    bot or by a human admin), schedule the post-request message."""
    row = await db_fetchone(
        "SELECT * FROM queue WHERE chat_id=? AND user_id=?", chat_id, user_id
    )
    if not row:
        return  # joined without a request – not a "post-request" case
    if row["status"] == "pending":  # an admin approved it by hand in Telegram
        await mark(row["id"], "approved")
        _mlog_event.set()
    elif not (row["status"] == "approved" and (row["processed_at"] or 0) >= time.time() - 600):
        return  # old request, this is a later re-join
    await schedule_post(row)


async def schedule_post(row) -> None:
    res = await resolve_msg(row["chat_id"], "post")
    if not res:
        return
    if await db_fetchone(
        "SELECT 1 AS x FROM scheduled_msgs WHERE chat_id=? AND user_id=? LIMIT 1", row["chat_id"], row["user_id"]
    ):
        return  # already scheduled
    u = await db_fetchone("SELECT subscribed FROM users WHERE user_id=?", row["user_id"])
    if u and u["subscribed"] == 0:
        return  # respect unsubscribe
    delay = min(res[1], MAX_MSG_DELAY["post"])
    due = await reserve_slot(row["chat_id"], "post", time.time() + delay)
    await db_execute(
        """INSERT INTO scheduled_msgs (chat_id, user_id, user_chat_id, full_name, username, chat_title, due_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        row["chat_id"], row["user_id"], row["user_chat_id"] or row["user_id"],
        row["full_name"] or "", row["username"] or "", row["chat_title"] or "", due,
    )


async def _send_post(bot: Bot, s: dict) -> bool:
    """True = finished (remove from schedule)."""
    res = await resolve_msg(s["chat_id"], "post")
    if not res:
        return True  # switched off meanwhile
    name = s["full_name"] or "friend"
    user = SimpleNamespace(
        id=s["user_id"], username=s["username"] or None, full_name=name, first_name=name.split()[0],
    )
    status = "sent"
    try:
        if not await deliver_welcome(bot, int(s["user_chat_id"]), res[0], user, s["chat_title"],
                                     reply_markup=unsub_kb()):
            return True
    except TelegramRetryAfter as e:
        await db_execute("UPDATE scheduled_msgs SET due_at=? WHERE id=?", time.time() + e.retry_after + 1, s["id"])
        return False
    except TelegramForbiddenError as e:
        status = "blocked"
        log.warning("Post-request message blocked for %s: %s", s["user_id"], e)
        await note_forbidden(s["user_id"], e)
    except TelegramBadRequest as e:
        status = "failed"
        log.warning("Post-request message failed for %s: %s", s["user_id"], e)
    except Exception:
        status = "failed"
        log.exception("Post-request message error")
    if status == "sent":
        await note_ok(s["user_id"])
    await log_delivery(s["chat_id"], s["user_id"], "post", status)
    icon = {"sent": "📤", "blocked": "🚫", "failed": "⚠️"}[status]
    mlog(f"{icon} Post-request message {status} · {uname(user)} → <b>{escape(s['chat_title'] or '')}</b>",
         kind="post")
    return True


async def post_worker(bot: Bot):
    last_purge = 0.0
    while True:
        try:
            now = time.time()
            rows = await db_fetchall(
                "SELECT * FROM scheduled_msgs WHERE due_at<=? ORDER BY due_at LIMIT 20", now
            )
            for s in rows:
                cid = s["chat_id"]
                dm = await dm_get(cid, "post")
                lo, hi = float(dm["gap_min"] or 0), float(dm["gap_max"] or 0)
                if hi > 0 and time.time() < _post_sent_at.get(cid, 0.0) + lo:
                    # backlog (e.g. after downtime): never send closer than the time gap
                    await db_execute(
                        "UPDATE scheduled_msgs SET due_at=? WHERE id=?",
                        _post_sent_at[cid] + random.uniform(lo, hi), s["id"],
                    )
                    continue
                done = await _send_post(bot, s)
                _post_sent_at[cid] = time.time()
                if done:
                    await db_execute("DELETE FROM scheduled_msgs WHERE id=?", s["id"])
                await asyncio.sleep(0.05)
            if now - last_purge > 3600:  # tracked ids older than 47h can't be deleted anyway
                last_purge = now
                await db_execute("DELETE FROM chat_msgs WHERE ts < ?", now - 47 * 3600)
            nxt = await db_fetchone("SELECT MIN(due_at) AS t FROM scheduled_msgs")
            wait = 2.0 if not nxt or nxt["t"] is None else min(2.0, max(0.05, float(nxt["t"]) - time.time()))
            await asyncio.sleep(wait)  # wake exactly when the next message is due
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Post worker error")
            await asyncio.sleep(5)


# ───────────────────────── Interval logic ─────────────────────────
async def interval_cfg_default() -> dict:
    """Default timer (settings table) – copied into every new destination."""
    return {
        "mode": await get_setting("interval_mode"),
        "base": float(await get_setting("interval")),
        "jitter": int(float(await get_setting("jitter_pct"))),
        "rmin": float(await get_setting("rand_min")),
        "rmax": float(await get_setting("rand_max")),
    }


def cfg_from_dest(d: dict) -> dict:
    return {
        "mode": d["iv_mode"],
        "base": float(d["iv_base"]),
        "jitter": int(d["iv_jitter"]),
        "rmin": float(d["iv_min"]),
        "rmax": float(d["iv_max"]),
    }


def describe_interval(c: dict) -> str:
    if c["mode"] == "random":
        return f"🎲 Random {c['rmin']:g}–{c['rmax']:g}s"
    if c["jitter"]:
        lo = c["base"] * (1 - c["jitter"] / 100)
        hi = c["base"] * (1 + c["jitter"] / 100)
        return f"📌 Fixed {c['base']:g}s ±{c['jitter']}% ({lo:.1f}–{hi:.1f}s)"
    return f"📌 Fixed {c['base']:g}s (no jitter)"


def avg_interval(c: dict) -> float:
    return (c["rmin"] + c["rmax"]) / 2 if c["mode"] == "random" else c["base"]


def pick_delay(c: dict) -> float:
    if c["mode"] == "random":
        d = random.uniform(c["rmin"], c["rmax"])
    else:
        j = c["jitter"] / 100
        d = c["base"] * random.uniform(1 - j, 1 + j) if j else c["base"]
    return max(0.5, d)


# ───────────────────────── Destinations (data) ─────────────────────────
ACTIVITY_DAYS = 7
_EVENT_COL = {
    "request": "last_request_at",
    "approve": "last_approve_at",
    "join": "last_member_event_at",
    "leave": "last_member_event_at",
}
_next_due: dict[int, float] = {}        # chat_id -> unix time of next allowed approval
_last_post_touch: dict[int, float] = {}  # throttle for post tracking


async def upsert_destination(chat_id: int, title: str, chat_type) -> None:
    ctype = str(getattr(chat_type, "value", chat_type) or "")
    n = await db_execute(
        "UPDATE destinations SET title=?, chat_type=?, active=1 WHERE chat_id=?",
        title or "", ctype, chat_id,
    )
    if n:
        return
    c = await interval_cfg_default()
    await db_execute(
        """INSERT INTO destinations (chat_id, title, chat_type, active, running, iv_mode, iv_base,
                                     iv_jitter, iv_min, iv_max, added_at)
           VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(chat_id) DO NOTHING""",
        chat_id, title or "", ctype, c["mode"], c["base"], c["jitter"], c["rmin"], c["rmax"], time.time(),
    )


async def migrate_destinations():
    """Old versions had one global timer/switch: create a destination for every chat in the queue."""
    c = await interval_cfg_default()
    run = 1 if await get_setting("running") == "1" else 0
    now = time.time()
    for r in await db_fetchall(
        "SELECT chat_id, COALESCE(MAX(chat_title), '') AS title FROM queue GROUP BY chat_id"
    ):
        await db_execute(
            """INSERT INTO destinations (chat_id, title, active, running, iv_mode, iv_base,
                                         iv_jitter, iv_min, iv_max, added_at)
               VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(chat_id) DO NOTHING""",
            r["chat_id"], r["title"], run, c["mode"], c["base"], c["jitter"], c["rmin"], c["rmax"], now,
        )
    await db_execute(
        """UPDATE destinations SET last_request_at =
             (SELECT MAX(q.created_at) FROM queue q WHERE q.chat_id = destinations.chat_id)
           WHERE last_request_at IS NULL"""
    )
    await db_execute(
        """UPDATE destinations SET last_approve_at =
             (SELECT MAX(q.processed_at) FROM queue q
               WHERE q.chat_id = destinations.chat_id AND q.status='approved')
           WHERE last_approve_at IS NULL"""
    )


async def record_event(chat_id: int, user_id: int | None, kind: str) -> None:
    now = time.time()
    await db_execute(
        "INSERT INTO events (chat_id, user_id, kind, ts) VALUES (?, ?, ?, ?)",
        chat_id, user_id, kind, now,
    )
    col = _EVENT_COL.get(kind)
    if col:
        await db_execute(f"UPDATE destinations SET {col}=? WHERE chat_id=?", now, chat_id)


async def dest_get(chat_id: int) -> dict | None:
    return await db_fetchone("SELECT * FROM destinations WHERE chat_id=?", chat_id)


async def dest_all() -> list[dict]:
    rows = await db_fetchall("SELECT * FROM destinations WHERE active=1 ORDER BY title, chat_id")
    return [d for d in rows if d["chat_id"] != _mlog_chat]


async def event_counts(chat_id: int, since: float = 0.0) -> dict:
    out = {"join": 0, "leave": 0, "request": 0, "approve": 0}
    for r in await db_fetchall(
        "SELECT kind, COUNT(*) AS n FROM events WHERE chat_id=? AND ts>=? GROUP BY kind",
        chat_id, since,
    ):
        out[r["kind"]] = int(r["n"])
    return out


async def leave_counts() -> dict[int, int]:
    rows = await db_fetchall(
        "SELECT chat_id, COUNT(*) AS n FROM events WHERE kind='leave' GROUP BY chat_id"
    )
    return {int(r["chat_id"]): int(r["n"]) for r in rows}


async def refresh_members(bot: Bot, d: dict) -> None:
    """Member count from Telegram, cached for 5 minutes."""
    if time.time() - (d.get("members_at") or 0) < 300:
        return
    try:
        n = await bot.get_chat_member_count(d["chat_id"])
    except Exception as e:  # bot removed, no rights, network...
        log.warning("member count failed for %s: %s", d["chat_id"], e)
        return
    d["members"], d["members_at"] = n, time.time()
    await db_execute(
        "UPDATE destinations SET members=?, members_at=? WHERE chat_id=?",
        n, d["members_at"], d["chat_id"],
    )


def kfmt(n) -> str:
    n = int(n or 0)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}".rstrip("0").rstrip(".") + "M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}".rstrip("0").rstrip(".") + "k"
    return str(n)


def dest_status(d: dict, pending: int) -> tuple[str, str]:
    """Status dot + explanation for a destination.

    🔴 nothing for 7 days · 🟢 accepting requests · 🔵 only posts · 🟡 everything else.
    """
    cut = time.time() - ACTIVITY_DAYS * 86400

    def recent(key: str) -> bool:
        return (d.get(key) or 0) >= cut

    req, app = recent("last_request_at"), recent("last_approve_at")
    mem, post = recent("last_member_event_at"), recent("last_post_at")
    if not (req or app or mem or post or pending):
        return "🔴", f"No activity in the last {ACTIVITY_DAYS} days"
    if d["running"] == 1 and (pending or req):
        return "🟢", "Requests are being accepted"
    if post and not (req or app or mem or pending):
        return "🔵", "Only new posts – no requests, nothing accepted"
    if pending or req:
        return "🟡", "Requests are waiting but accepting is stopped"
    return "🟡", "No new requests – other activity (joins/leaves/posts)"


# ───────────────────────── 🧭 User pipeline (add-on) ─────────────────────────
# Every user passes: 📥 Request → 📨 Pre-msg → 👁 Seen (= delivered) → ✅ Accepted → 📤 Post-msg
# Nothing is stored twice: each step is read from tables the bot already fills
# (queue, msg_log, scheduled_msgs, events, users).
STEP_OK, STEP_WAIT, STEP_FAIL = "🟢", "🟡", "🔴"
STEP_OFF = "⚪"        # message type switched OFF for that destination (set to "🟡" if you prefer)
PIPE_LEGEND = "📥 Request · 📨 Pre-msg · 👁 Seen · ✅ Accepted · 📤 Post-msg"
PIPE_USERS = 10       # users shown in the master log list
MLOG_SHOW_LINKS = False  # True = also show the per-invite-link lines under each destination


def is_block_error(e) -> bool:
    """True when Telegram says the user blocked / deleted the bot (not merely 'never started')."""
    s = str(e).lower()
    return "blocked" in s or "deactivated" in s


async def note_forbidden(user_id: int, e) -> None:
    if not is_block_error(e):
        return
    try:
        await db_execute("UPDATE users SET blocked=1 WHERE user_id=?", user_id)
        _mlog_event.set()
    except Exception:
        log.exception("note_forbidden failed")


async def note_ok(user_id: int) -> None:
    """A message reached the user, so they are not blocking the bot (any more)."""
    try:
        await db_execute("UPDATE users SET blocked=0 WHERE user_id=? AND blocked=1", user_id)
    except Exception:
        log.exception("note_ok failed")


async def pipeline_dots(row: dict, msg_on: dict) -> str:
    """Five status dots for one (destination, user) request row of the queue table."""
    cid, uid = row["chat_id"], row["user_id"]
    since = float(row["created_at"] or 0) - 1  # only messages belonging to the latest request

    async def last(kind: str):
        r = await db_fetchone(
            "SELECT status FROM msg_log WHERE chat_id=? AND user_id=? AND kind=? AND ts>=? "
            "ORDER BY id DESC LIMIT 1", cid, uid, kind, since,
        )
        return r["status"] if r else None

    if cid not in msg_on:
        msg_on[cid] = {"pre": bool(await resolve_msg(cid, "pre")), "post": bool(await resolve_msg(cid, "post"))}

    pre = await last("pre")
    if pre == "sent":
        pre_dot = STEP_OK
    elif pre:
        pre_dot = STEP_FAIL
    else:
        pre_dot = STEP_WAIT if msg_on[cid]["pre"] else STEP_OFF

    acc_dot = {"approved": STEP_OK, "failed": STEP_FAIL}.get(row["status"], STEP_WAIT)

    post = await last("post")
    if post == "sent":
        post_dot = STEP_OK
    elif post:
        post_dot = STEP_FAIL
    elif await db_fetchone("SELECT 1 AS x FROM scheduled_msgs WHERE chat_id=? AND user_id=? LIMIT 1", cid, uid):
        post_dot = STEP_WAIT
    else:
        post_dot = STEP_WAIT if msg_on[cid]["post"] else STEP_OFF

    # request received (always done) · pre-msg · seen (= delivered) · accepted · post-msg
    return STEP_OK + pre_dot + pre_dot + acc_dot + post_dot


async def pipeline_block(limit: int = PIPE_USERS) -> list[str]:
    """The last `limit` users whose pipeline changed, newest first (one text block per user)."""
    cand: dict[tuple, float] = {}
    for r in await db_fetchall(
        "SELECT chat_id, user_id, COALESCE(processed_at, created_at) AS t FROM queue "
        "ORDER BY COALESCE(processed_at, created_at) DESC LIMIT ?", limit * 4,
    ):
        cand[(r["chat_id"], r["user_id"])] = float(r["t"] or 0)
    for r in await db_fetchall(
        "SELECT chat_id, user_id, ts AS t FROM msg_log WHERE user_id IS NOT NULL ORDER BY ts DESC LIMIT ?",
        limit * 4,
    ):
        k = (r["chat_id"], r["user_id"])
        cand[k] = max(cand.get(k, 0.0), float(r["t"] or 0))
    visible = {d["chat_id"] for d in await dest_all()}
    out, msg_on = [], {}
    for (cid, uid) in sorted(cand, key=cand.get, reverse=True):
        if cid not in visible:
            continue
        row = await db_fetchone("SELECT * FROM queue WHERE chat_id=? AND user_id=?", cid, uid)
        if not row:
            continue
        dots = await pipeline_dots(row, msg_on)
        who = escape((row["full_name"] or "").strip() or (f"@{row['username']}" if row["username"] else str(uid)))
        out.append(f"{dots}  {who} · {escape(_cut(row['chat_title'] or str(cid), 14))}")
        if len(out) >= limit:
            break
    return out


async def dest_totals() -> dict[int, dict]:
    """Per destination: total requests · joined · left · members who blocked/deleted the bot."""
    out: dict[int, dict] = {}

    def g(cid):
        return out.setdefault(int(cid), {"req": 0, "join": 0, "leave": 0, "blocked": 0})

    for r in await db_fetchall("SELECT chat_id, COUNT(*) AS n FROM queue GROUP BY chat_id"):
        g(r["chat_id"])["req"] = int(r["n"])
    for r in await db_fetchall(
        "SELECT chat_id, kind, COUNT(*) AS n FROM events WHERE kind IN ('join', 'leave') GROUP BY chat_id, kind"
    ):
        g(r["chat_id"])[r["kind"]] = int(r["n"])
    for r in await db_fetchall(
        "SELECT q.chat_id AS chat_id, COUNT(DISTINCT q.user_id) AS n FROM queue q "
        "JOIN users u ON u.user_id = q.user_id WHERE u.blocked=1 GROUP BY q.chat_id"
    ):
        g(r["chat_id"])["blocked"] = int(r["n"])
    return out


async def layer_stats(t0: float, t1: float) -> dict:
    """Layer counters for the Universal status (each step is counted in the window it happened in)."""
    async def n(sql: str, *a) -> int:
        r = await db_fetchone(sql, *a)
        return int((r or {}).get("n") or 0)

    return {
        "req": await n("SELECT COUNT(*) AS n FROM queue WHERE created_at>=? AND created_at<?", t0, t1),
        "pending": await n("SELECT COUNT(*) AS n FROM queue WHERE status='pending' AND created_at>=? AND created_at<?", t0, t1),
        "pre_ok": await n("SELECT COUNT(*) AS n FROM msg_log WHERE kind='pre' AND status='sent' AND ts>=? AND ts<?", t0, t1),
        "pre_bad": await n("SELECT COUNT(*) AS n FROM msg_log WHERE kind='pre' AND status<>'sent' AND ts>=? AND ts<?", t0, t1),
        "acc_ok": await n("SELECT COUNT(*) AS n FROM queue WHERE status='approved' AND processed_at>=? AND processed_at<?", t0, t1),
        "acc_bad": await n("SELECT COUNT(*) AS n FROM queue WHERE status='failed' AND processed_at>=? AND processed_at<?", t0, t1),
        "post_ok": await n("SELECT COUNT(*) AS n FROM msg_log WHERE kind='post' AND status='sent' AND ts>=? AND ts<?", t0, t1),
        "post_bad": await n("SELECT COUNT(*) AS n FROM msg_log WHERE kind='post' AND status<>'sent' AND ts>=? AND ts<?", t0, t1),
        "join": await n("SELECT COUNT(*) AS n FROM events WHERE kind='join' AND ts>=? AND ts<?", t0, t1),
        "leave": await n("SELECT COUNT(*) AS n FROM events WHERE kind='leave' AND ts>=? AND ts<?", t0, t1),
    }


def layer_text(ls: dict) -> str:
    return (
        f"   📥 Requests <b>{ls['req']}</b> · ⏳ Pending <b>{ls['pending']}</b>\n"
        f"   📨 Pre-msg 🟢<b>{ls['pre_ok']}</b> 🔴<b>{ls['pre_bad']}</b> · 👁 Seen 🟢<b>{ls['pre_ok']}</b>\n"
        f"   ✅ Accepted 🟢<b>{ls['acc_ok']}</b> 🔴<b>{ls['acc_bad']}</b> · "
        f"📤 Post-msg 🟢<b>{ls['post_ok']}</b> 🔴<b>{ls['post_bad']}</b>\n"
        f"   ➕ Joined <b>{ls['join']}</b> · 🚪 Left <b>{ls['leave']}</b>"
    )


# ───────────────────────── 🧾 Master log (core) ─────────────────────────
MLOG_LINES = 80        # activity buffer (only the 5 newest shown kinds are posted)
MLOG_SHOW = {"pre", "req", "joined", "post"}  # pre-message sent · join request · joined w/o message · post-message
MLOG_MIN_GAP = 5.0     # seconds between two updates (bursts are merged into one)
_mlog_lines: deque = deque(maxlen=MLOG_LINES)
_mlog_event = asyncio.Event()
_mlog_chat: int | None = None
_me_cache = None


def uname(u) -> str:
    """HTML-safe display name of a Telegram user."""
    name = escape(((getattr(u, "full_name", None) or "").strip()) or str(getattr(u, "id", "?")))
    un = getattr(u, "username", None)
    return f"{name} (@{escape(un)})" if un else name


def rname(row) -> str:
    """Display name of a queue row."""
    name = (row["full_name"] or "").strip() if "full_name" in row.keys() else ""
    un = row["username"] if "username" in row.keys() else None
    base = escape(name or str(row["user_id"]))
    return f"{base} (@{escape(un)})" if un else base


MLOG_MERGE_SECS = 90   # same kind of event within this time is merged into "×N"


def mlog(text: str, group=None, many=None, kind: str = "other") -> None:
    """Queue one activity line for the master log (sync, never blocks). `text` must be HTML-safe.

    kind     : only kinds in MLOG_SHOW are printed in the log message.
    group+many(n)->text : repeated events of the same group within 90s collapse into "×N".
    """
    now = time.time()
    if group is not None and many is not None:
        for i, e in enumerate(list(_mlog_lines)[:8]):
            if e[2] == group and now - e[0] <= MLOG_MERGE_SECS:
                del _mlog_lines[i]
                n = e[3] + 1
                _mlog_lines.appendleft([now, many(n), group, n, many, e[5]])
                _mlog_event.set()
                return
    _mlog_lines.appendleft([now, text, group, 1, many, kind])
    _mlog_event.set()


async def link_name(lid: int) -> str:
    row = await db_fetchone("SELECT name FROM links WHERE id=?", lid)
    return row["name"] if row else "?"


async def bot_heading(bot: Bot) -> str:
    global _me_cache
    if _me_cache is None:
        try:
            _me_cache = await bot.get_me()
        except Exception:
            return "🤖 <b>Bot</b>"
    me = _me_cache
    return f"🤖 <b>{escape(me.full_name)}</b> · @{escape(me.username or '')}"


def timer_short(c: dict, jitter: bool = False) -> str:
    if c["mode"] == "random":
        return f"{c['rmin']:g}–{c['rmax']:g}s"
    return f"{c['base']:g}s" + (f" ±{c['jitter']}%" if jitter and c["jitter"] else "")


async def _link_pending_map() -> dict[int, int]:
    """Waiting requests per link (a request belongs to the link it was made with)."""
    rows = await db_fetchall(
        """SELECT le.link_id AS lid, COUNT(*) AS n
           FROM queue q
           JOIN link_events le ON le.id = (
               SELECT MAX(x.id) FROM link_events x
               WHERE x.chat_id = q.chat_id AND x.user_id = q.user_id AND x.kind = 'request')
           WHERE q.status = 'pending'
           GROUP BY le.link_id"""
    )
    return {int(r["lid"]): int(r["n"]) for r in rows}


async def build_master_log(bot: Bot) -> str:
    dests = await dest_all()
    pend = {int(cid): n for cid, _t, n in await pending_by_chat()}
    appr = {
        int(r["chat_id"]): int(r["n"])
        for r in await db_fetchall(
            "SELECT chat_id, COUNT(*) AS n FROM queue WHERE status='approved' GROUP BY chat_id"
        )
    }
    tot = await dest_totals()
    lpend = await _link_pending_map()
    lappr = {
        int(r["link_id"]): int(r["n"])
        for r in await db_fetchall(
            "SELECT link_id, COUNT(*) AS n FROM link_events WHERE kind='approve' GROUP BY link_id"
        )
    }
    links: dict[int, list] = {}
    for l in await db_fetchall("SELECT * FROM links WHERE revoked=0 ORDER BY id"):
        links.setdefault(int(l["chat_id"]), []).append(l)

    def pbar(part: int, whole: int) -> str:
        pct = min(100, round(100 * part / whole)) if whole else 0
        k = round(pct / 10)
        return f"{'▰' * k}{'▱' * (10 - k)} {pct}%"

    def dest_block(d: dict, limit: int) -> list[str]:
        """limit < 0 → one compact line (used only when the message gets too long)."""
        cid = d["chat_id"]
        t = tot.get(int(cid), {})
        req, p, a = t.get("req", 0), pend.get(cid, 0), appr.get(cid, 0)
        j, lf, bl = t.get("join", 0), t.get("leave", 0), t.get("blocked", 0)
        state = f"▶️ {timer_short(cfg_from_dest(d))}" if d["running"] == 1 else "⏹"
        head = f"📡 <b>{escape(d['title'] or str(cid))}</b>"
        if d["members"]:
            head += f" · {kfmt(d['members'])}"
        if limit < 0:
            return [f"{head} · 📥{req} · ✅{a} · ➕{j} · ⏳{p} · 🚪{lf} · 🚫{bl}"]
        out = [
            f"{head} · {state}",
            f"📥 Requests <b>{req}</b>",
            f"✅ Accepted {pbar(a, req)}",
            f"➕ Joined   {pbar(j, req)}",
            f"⏳ {p} pending · 🚪 {lf} left · 🚫 {bl} blocked",
        ]
        if MLOG_SHOW_LINKS and limit > 0:  # optional per-link lines (off by default)
            ls = sorted(links.get(cid, []), key=lambda l: (-lpend.get(l["id"], 0), -lappr.get(l["id"], 0), l["id"]))
            for l in ls[:limit]:
                out.append(f"   🔗 <b>{escape(l['name'])}</b>  ⏳{lpend.get(l['id'], 0)} ✅{lappr.get(l['id'], 0)}")
            if len(ls) > limit:
                out.append(f"   <i>+{len(ls) - limit} more links</i>")
        return out

    divider = "━" * 14
    s_req = sum(tot.get(int(d["chat_id"]), {}).get("req", 0) for d in dests)
    s_join = sum(tot.get(int(d["chat_id"]), {}).get("join", 0) for d in dests)
    s_left = sum(tot.get(int(d["chat_id"]), {}).get("leave", 0) for d in dests)
    s_acc = sum(appr.get(d["chat_id"], 0) for d in dests)
    s_pend = sum(pend.get(d["chat_id"], 0) for d in dests)
    s_blk = int((await user_overview())["blocked"])
    head = [
        await bot_heading(bot),
        f"🧾 <b>MASTER LOG</b> · {datetime.now(TZ).strftime('%d %b, %H:%M:%S')}",
        "",
        "📊 <b>ALL CHANNELS</b>",
        f"📥 {s_req} · ✅ {s_acc} · ➕ {s_join} · ⏳ {s_pend} · 🚪 {s_left} · 🚫 {s_blk}",
        "",
    ]
    acts = await pipeline_block(PIPE_USERS) or ["<i>nothing yet</i>"]

    def assemble(link_limit: int, n_users: int, compact: bool = False) -> str:
        body: list[str] = []
        for d in dests:
            body += dest_block(d, -1 if compact else link_limit) + ([] if compact else [""])
        if not dests:
            body = ["<i>No destinations yet</i>", ""]
        if compact:
            body.append("")
        return "\n".join(head + body + [divider, f"<b>LATEST {PIPE_USERS} USERS</b>"] + acts[:n_users]
                         + ["", "<i>Order: Request · Pre · Seen · Accepted · Post</i>"])

    tries = [(5, PIPE_USERS, False), (3, PIPE_USERS, False), (1, PIPE_USERS, False)] if MLOG_SHOW_LINKS else []
    tries += [(0, PIPE_USERS, False), (0, PIPE_USERS, True), (0, 5, True)]
    for link_limit, n_users, compact in tries:
        text = assemble(link_limit, n_users, compact)
        if len(text) <= 3900:
            return text
    return text[:3900].rsplit("\n", 1)[0]  # extreme case: very many destinations


async def delete_logged(bot: Bot, ref: str) -> None:
    """ref = 'chat_id:message_id'. Best effort."""
    try:
        chat_s, msg_s = ref.rsplit(":", 1)
        await bot.delete_message(int(chat_s), int(msg_s))
    except Exception:
        pass


async def push_master_log(bot: Bot, chat_id: int) -> bool:
    """Send the new log message, then delete the previous one."""
    text = await build_master_log(bot)
    try:
        m = await bot.send_message(chat_id, text, disable_web_page_preview=True)
    except TelegramRetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 1)
        _mlog_event.set()
        return False
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await set_setting("master_log_err", str(e)[:150])
        log.warning("Master log send failed: %s", e)
        return False
    prev = await get_setting("master_log_msg")
    await set_setting("master_log_msg", f"{chat_id}:{m.message_id}")
    await set_setting("master_log_err", "")
    await set_setting("master_log_at", str(time.time()))
    if prev:
        await delete_logged(bot, prev)
    return True


async def master_log_worker(bot: Bot):
    global _mlog_chat
    chat = await get_setting("master_log_chat")
    _mlog_chat = int(chat) if chat else None
    last_sent = 0.0
    while True:
        try:
            await _mlog_event.wait()
            gap = MLOG_MIN_GAP - (time.time() - last_sent)
            if gap > 0:
                await asyncio.sleep(gap)  # lets a burst of events collect into one update
            _mlog_event.clear()
            chat = await get_setting("master_log_chat")
            if await get_setting("master_log_on") != "1" or not chat:
                continue
            _mlog_chat = int(chat)
            await push_master_log(bot, int(chat))
            last_sent = time.time()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Master log error")
            await asyncio.sleep(5)


async def queue_worker(bot: Bot):
    """Entry point used by main.py: approval loop + master-log worker."""
    tasks = [asyncio.create_task(master_log_worker(bot)), asyncio.create_task(post_worker(bot))]
    mlog("🟢 Bot started")
    try:
        await _queue_loop(bot)
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass


# ───────────────────────── Invite-link bookkeeping ─────────────────────────
async def link_id_by_url(chat_id: int, url: str | None) -> int | None:
    if not url:
        return None
    row = await db_fetchone("SELECT id FROM links WHERE chat_id=? AND invite_link=?", chat_id, url)
    return int(row["id"]) if row else None


async def link_for_request(chat_id: int, user_id: int) -> int | None:
    row = await db_fetchone(
        "SELECT link_id FROM link_events WHERE chat_id=? AND user_id=? AND kind='request' "
        "ORDER BY id DESC LIMIT 1", chat_id, user_id,
    )
    return int(row["link_id"]) if row else None


async def record_link_event(chat_id: int, link_id: int, user_id: int, kind: str):
    await db_execute(
        "INSERT INTO link_events (chat_id, link_id, user_id, kind, ts) VALUES (?, ?, ?, ?, ?)",
        chat_id, link_id, user_id, kind, time.time(),
    )


# ───────────────────────── Join-request handler ─────────────────────────
@router.chat_join_request()
async def on_join_request(req: ChatJoinRequest, bot: Bot):
    await upsert_destination(req.chat.id, req.chat.title or "", req.chat.type)
    await enqueue(req)
    await record_event(req.chat.id, req.from_user.id, "request")
    lid = await link_id_by_url(req.chat.id, req.invite_link.invite_link if req.invite_link else None)
    if lid:
        await record_link_event(req.chat.id, lid, req.from_user.id, "request")
    await touch_user(req.from_user, False)
    via = f" · 🔗 {escape(await link_name(lid))}" if lid else ""
    ttl = escape(req.chat.title or "")
    mlog(f"📥 Join request · {uname(req.from_user)} → <b>{ttl}</b>{via}", kind="req")
    log.info("Queued join request user=%s chat=%s", req.from_user.id, req.chat.id)
    await send_welcome(bot, req.user_chat_id, req.from_user, req.chat.title or "", req.chat.id)


# ───────────────────────── Queue worker (one timer per destination) ─────────────────────────
async def process_one(bot: Bot, row) -> tuple[bool, float]:
    """Returns (handled, wait). handled=False → flood control, retry after `wait` seconds."""
    try:
        await bot.approve_chat_join_request(row["chat_id"], row["user_id"])
        await mark(row["id"], "approved")
        await record_event(row["chat_id"], row["user_id"], "approve")
        lid = await link_for_request(row["chat_id"], row["user_id"])
        if lid:
            await record_link_event(row["chat_id"], lid, row["user_id"], "approve")
        log.info("Approved user=%s chat=%s", row["user_id"], row["chat_id"])
        ttl = escape(row["chat_title"] or "")
        via = f" · 🔗 {escape(await link_name(lid))}" if lid else ""
        mlog(
            f"✅ {rname(row)} → <b>{ttl}</b>{via}",
            ("ap", row["chat_id"], lid),
            lambda n, ttl=ttl, via=via: f"✅ ×{n} approved → <b>{ttl}</b>{via}",
        )
        return True, 0.0
    except TelegramRetryAfter as e:
        log.warning("Flood control on chat %s, waiting %ss", row["chat_id"], e.retry_after)
        mlog(f"⏳ Flood control · <b>{escape(row['chat_title'] or '')}</b> · waiting {int(e.retry_after)}s")
        return False, float(e.retry_after) + 1
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await mark(row["id"], "failed", str(e)[:200])
        log.warning("Failed user=%s chat=%s: %s", row["user_id"], row["chat_id"], e)
        mlog(f"⚠️ {rname(row)} → <b>{escape(row['chat_title'] or '')}</b> · {escape(str(e)[:60])}")
        return True, 0.0


async def _queue_loop(bot: Bot):
    while True:
        try:
            dests = await db_fetchall("SELECT * FROM destinations WHERE running=1 AND active=1")
            if not dests:
                await asyncio.sleep(2)
                continue
            for d in dests:
                cid = d["chat_id"]
                if cid == _mlog_chat:
                    continue
                if _next_due.get(cid, 0) > time.time():
                    continue
                row = await next_pending(cid)
                if row is None:
                    continue
                handled, wait = await process_one(bot, row)
                if handled:
                    try:
                        delay = pick_delay(cfg_from_dest(d))
                    except (ValueError, TypeError):
                        delay = 10.0
                    log.info("Next approval in %s: %.1fs", cid, delay)
                else:
                    delay = wait
                _next_due[cid] = time.time() + delay
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Worker error")
            mlog("❌ Approval worker error – see server logs")
            await asyncio.sleep(5)


# ───────────────────────── Admin helpers ─────────────────────────
class IsAdmin(Filter):
    async def __call__(self, event) -> bool:
        return bool(event.from_user and event.from_user.id in ADMIN_IDS)


class Form(StatesGroup):
    iv_fixed = State()
    iv_range = State()
    wl_caption = State()
    wl_media = State()
    wl_name = State()
    link_name = State()
    msg_delay = State()
    ml_chat = State()
    gm_delay = State()
    gm_gap = State()
    msg_gap = State()
    bt_label = State()
    bt_url = State()


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return " ".join(p for p in (f"{h}h" if h else "", f"{m}m" if m else "", f"{s}s" if s or not (h or m) else "") if p)


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


async def edit(cb: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass  # "message is not modified"


# ───────────────────────── Main panel ─────────────────────────
async def panel() -> tuple[str, InlineKeyboardMarkup]:
    dests = await dest_all()
    accepting = sum(1 for d in dests if d["running"] == 1)
    welcome_on = await get_setting("welcome_enabled") == "1"
    post_on = await get_setting("post_enabled") == "1"
    active = await get_active_welcome()
    c = await counts()
    text = (
        "<b>🤖 Join Request Manager</b>\n\n"
        f"📡 Destinations: <b>{len(dests)}</b>  ·  ▶️ accepting on <b>{accepting}</b>\n"
        f"📥 Pre-request message: {'✅ ON' if welcome_on else '❌ OFF'} · "
        f"<b>{escape(active['name']) if active else 'none set'}</b>\n"
        f"📤 Post-request message: {'✅ ON' if post_on else '❌ OFF'}\n\n"
        f"⏳ Pending: <b>{c['pending']}</b>\n"
        f"✅ Approved: <b>{c['approved']}</b>\n"
        f"⚠️ Failed: <b>{c['failed']}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📡 Destinations", "dst:menu"), btn("💬 Massages", "ms:menu")],
        [btn("✅ Acceptors", "acs:menu"), btn("👤 User info", "ui:menu")],
        [btn("📊 Universal status", "queue"), btn("🧾 Master log", "ml:menu")],
        [btn("🔄 Refresh", "refresh")],
    ])
    return text, kb


async def show_panel(target: Message | CallbackQuery):
    text, kb = await panel()
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


BACK_KB = InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Back", "refresh")]])


# ───────────────────────── 🔘 Tracked button click (deep link) ─────────────────────────
@router.message(CommandStart(deep_link=True, magic=F.args.startswith("btn_")))
async def cmd_button_click(message: Message, command: CommandObject):
    """A member tapped a TRACKED button → count it, then hand over the real link."""
    if message.chat.type != "private" or not message.from_user:
        return
    try:
        bid = int((command.args or "")[4:])
    except ValueError:
        return
    await touch_user(message.from_user, True)
    b = await bt_register_click(bid, message.from_user.id)
    if not b:
        m = await message.answer("⚠️ This link is no longer available.")
    else:
        mk = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"➡️ {b['label']}", url=b["url"])]])
        m = await message.answer(f"🔗 <b>{escape(b['label'])}</b>\nTap the button below to open it 👇", reply_markup=mk)
    await track_out(message.chat.id, [m.message_id])


@router.message(CommandStart(), IsAdmin())
@router.message(Command("panel"), IsAdmin())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await show_panel(message)


@router.message(Command("cancel"), IsAdmin())
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Cancelled.")
    await show_panel(message)


@router.message(CommandStart())
async def cmd_start_non_admin(message: Message):
    if message.chat.type == "private":
        if message.from_user:
            await touch_user(message.from_user, True)
        m = await message.answer("👋 This bot automatically handles channel/group join requests.")
        await track_out(message.chat.id, [m.message_id])


@router.callback_query(F.data == "refresh", IsAdmin())
async def cb_refresh(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_panel(cb)
    await cb.answer()


@router.callback_query(F.data == "toggle_welcome", IsAdmin())
async def cb_toggle_welcome(cb: CallbackQuery):
    on = await get_setting("welcome_enabled") == "1"
    await set_setting("welcome_enabled", "0" if on else "1")
    await show_ms_menu(cb)
    await cb.answer("Auto-reply OFF" if on else "Auto-reply ON")


# ───────────────────────── Timer menu (scope = "all" or a chat_id) ─────────────────────────
_COLMAP = {"mode": "iv_mode", "base": "iv_base", "jitter": "iv_jitter", "rmin": "iv_min", "rmax": "iv_max"}
_SETMAP = {"mode": "interval_mode", "base": "interval", "jitter": "jitter_pct",
           "rmin": "rand_min", "rmax": "rand_max"}


async def _iv_set_raw(scope: str, **kv):
    if scope == "all":
        for k, v in kv.items():
            await set_setting(_SETMAP[k], str(v))  # default for future destinations
            await db_execute(f"UPDATE destinations SET {_COLMAP[k]}=?", v)
    else:
        for k, v in kv.items():
            await db_execute(f"UPDATE destinations SET {_COLMAP[k]}=? WHERE chat_id=?", v, int(scope))


async def iv_set(scope: str, **kv):
    await _iv_set_raw(scope, **kv)
    if scope == "all":
        c, title = await interval_cfg_default(), "ALL destinations"
    else:
        d = await dest_get(int(scope))
        if not d:
            return
        c, title = cfg_from_dest(d), escape(d["title"] or scope)
    mlog(f"⏱ Timer set · <b>{title}</b> · {timer_short(c, True)}")


async def iv_view(scope: str) -> tuple[str, InlineKeyboardMarkup]:
    if scope == "all":
        c, title, back = await interval_cfg_default(), "ALL destinations", "acs:menu"
        note = "\n<i>Whatever you change here is applied to every destination.</i>\n"
    else:
        d = await dest_get(int(scope))
        if not d:
            return "❌ Destination not found.", BACK_KB
        c, title, back = cfg_from_dest(d), escape(d["title"] or scope), f"ac:menu:{scope}"
        note = ""
    fixed = c["mode"] == "fixed"
    text = (
        f"⏱ <b>Approval timer — {title}</b>\n{note}\n"
        f"Current: <b>{describe_interval(c)}</b>\n\n"
        "📌 <b>Fixed</b> – a set number of seconds with a preset random wobble (jitter).\n"
        "🎲 <b>Random</b> – every delay is a random value between your min and max."
    )
    rows = [[
        btn(("✅ " if fixed else "") + "📌 Fixed", f"iv:mode:{scope}:fixed"),
        btn(("" if fixed else "✅ ") + "🎲 Random", f"iv:mode:{scope}:random"),
    ]]
    if fixed:
        rows.append([btn(f"✏️ Set seconds ({c['base']:g}s)", f"iv:set_fixed:{scope}")])
        rows.append([
            btn(("✅ " if c["jitter"] == j else "") + f"±{j}%", f"iv:jit:{scope}:{j}")
            for j in JITTER_PRESETS
        ])
        text += "\n\n<b>Jitter preset:</b> choose how much the fixed time may vary."
    else:
        rows.append([btn(f"✏️ Set range ({c['rmin']:g}–{c['rmax']:g}s)", f"iv:set_range:{scope}")])
    rows.append([btn("⬅️ Back", back)])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def show_iv(target: Message | CallbackQuery, scope: str):
    text, kb = await iv_view(scope)
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("iv:menu:"), IsAdmin())
async def cb_iv_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_iv(cb, cb.data.split(":")[2])
    await cb.answer()


@router.callback_query(F.data.startswith("iv:mode:"), IsAdmin())
async def cb_iv_mode(cb: CallbackQuery):
    _, _, scope, mode = cb.data.split(":")
    if mode in ("fixed", "random"):
        await iv_set(scope, mode=mode)
    await show_iv(cb, scope)
    await cb.answer("Mode updated")


@router.callback_query(F.data.startswith("iv:jit:"), IsAdmin())
async def cb_iv_jitter(cb: CallbackQuery):
    _, _, scope, pct_s = cb.data.split(":")
    pct = int(pct_s)
    if pct in JITTER_PRESETS:
        await iv_set(scope, jitter=pct)
    await show_iv(cb, scope)
    await cb.answer(f"Jitter ±{pct}%")


@router.callback_query(F.data.startswith("iv:set_fixed:"), IsAdmin())
async def cb_iv_set_fixed(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.iv_fixed)
    await state.update_data(scope=cb.data.split(":")[2])
    await cb.message.answer(
        "📌 Send the <b>fixed interval in seconds</b> (e.g. <code>10</code>).\n"
        "Allowed: 1 – 86400.\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.iv_fixed, IsAdmin())
async def msg_iv_fixed(message: Message, state: FSMContext):
    try:
        value = float((message.text or "").strip().replace(",", "."))
        if not 1 <= value <= 86400:
            raise ValueError
    except ValueError:
        await message.answer("❌ Send a number between 1 and 86400 (or /cancel).")
        return
    scope = (await state.get_data()).get("scope", "all")
    await iv_set(scope, base=value)
    await state.clear()
    await message.answer(f"✅ Fixed interval set to <b>{value:g}s</b>.")
    await show_iv(message, scope)


@router.callback_query(F.data.startswith("iv:set_range:"), IsAdmin())
async def cb_iv_set_range(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.iv_range)
    await state.update_data(scope=cb.data.split(":")[2])
    await cb.message.answer(
        "🎲 Send the random range as <code>min-max</code> in seconds.\n"
        "Example: <code>5-20</code>\n"
        "Allowed: 1 – 86400, min must be lower than max.\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.iv_range, IsAdmin())
async def msg_iv_range(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".").replace("–", "-").replace(" ", "-")
    parts = [p for p in raw.split("-") if p]
    try:
        lo, hi = float(parts[0]), float(parts[1])
        if len(parts) != 2 or not (1 <= lo < hi <= 86400):
            raise ValueError
    except (ValueError, IndexError):
        await message.answer("❌ Format: <code>min-max</code>, e.g. <code>5-20</code> (or /cancel).")
        return
    scope = (await state.get_data()).get("scope", "all")
    await iv_set(scope, rmin=lo, rmax=hi)
    await state.clear()
    await message.answer(f"✅ Random range set to <b>{lo:g}–{hi:g}s</b>.")
    await show_iv(message, scope)


# ───────────────────────── Massage (message library) ─────────────────────────
MENU_LABEL = "Massages"  # menu / button name – change here if you want another label


def short(html: str, n: int = 220) -> str:
    html = (html or "").strip()
    if not html:
        return "<i>(empty)</i>"
    return escape(html[:n]) + ("…" if len(html) > n else "")


async def is_primary(wid: int) -> bool:
    return await get_setting("active_welcome") == str(wid)


# ---- list of all messages ----
async def ms_lib_view() -> tuple[str, InlineKeyboardMarkup]:
    allw = await wl_all()
    active = await get_setting("active_welcome")
    prim = next((w for w in allw if str(w["id"]) == active), None)
    text = (
        f"📚 <b>Message library</b>\n\n"
        f"Saved: <b>{len(allw)}</b>\n"
        f"⭐ Default pre-request: <b>{escape(prim['name']) if prim else 'none set'}</b>\n\n"
        + ("Tap a message to modify, view, set primary, broadcast or delete it.\n"
           if allw else "No messages yet – create your first one.\n")
        + "<i>⭐ = default pre-request message. Pre-request, post-request and broadcast messages are all taken from this library.</i>"
    )
    rows = [
        [btn(("⭐ " if str(w["id"]) == active else "") + w["name"][:40], f"ms:open:{w['id']}")]
        for w in allw[:50]
    ]
    rows.append([btn("➕ Create New Message", "ms:new")])
    rows.append([btn("⬅️ Back", "ms:menu")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def show_ms_menu(target: Message | CallbackQuery):
    text, kb = await ms_lib_view()
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data == "ms:lib", IsAdmin())
async def cb_ms_lib(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_ms_menu(cb)
    await cb.answer()


# ---- create ----
@router.callback_query(F.data == "ms:new", IsAdmin())
async def cb_ms_new(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    wid = await wl_create("New message")
    await wl_update(wid, name=f"Message #{wid}")
    if not await get_active_welcome():
        await set_setting("active_welcome", str(wid))  # first message becomes primary
    await show_editor(cb, wid)
    await cb.answer("Created ✅")


# ---- one message: Modify / View / Set Primary / Broadcast / Delete ----
async def ms_open_view(wid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    w = await wl_get(wid)
    if not w:
        return None
    prim = await is_primary(wid)
    nb = len(await bt_list(wid))
    text = (
        f"💬 <b>{escape(w['name'])}</b>  {'⭐ <i>primary</i>' if prim else ''}\n\n"
        f"📝 <b>Caption:</b>\n{short(w['caption'])}\n\n"
        f"🖼 <b>Media:</b> {MEDIA_LABEL.get(w['media_type'], '— none')}\n"
        f"🔘 <b>Buttons:</b> {nb}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("✏️ Modify", f"ms:edit:{wid}"), btn("👁 View", f"ms:view:{wid}")],
        [btn("⭐ Primary ✓" if prim else "⭐ Set Primary", f"ms:prim:{wid}")],
        [btn("📣 Broadcast", f"ms:bc:{wid}"), btn("🗑 Delete", f"ms:del:{wid}")],
        [btn("⬅️ Back", "ms:lib")],
    ])
    return text, kb


async def show_ms_open(target: Message | CallbackQuery, wid: int):
    view = await ms_open_view(wid)
    if not view:
        if isinstance(target, CallbackQuery):
            await target.answer("Message not found.", show_alert=True)
        return
    text, kb = view
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("ms:open:"), IsAdmin())
async def cb_ms_open(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_ms_open(cb, int(cb.data.split(":")[2]))
    await cb.answer()


@router.callback_query(F.data.startswith("ms:prim:"), IsAdmin())
async def cb_ms_primary(cb: CallbackQuery):
    wid = int(cb.data.split(":")[2])
    if not await wl_get(wid):
        await cb.answer("Message not found.", show_alert=True)
        return
    if await is_primary(wid):
        await cb.answer("Already the primary message.")
        return
    await set_setting("active_welcome", str(wid))  # single value ⇒ only ONE primary
    await show_ms_open(cb, wid)
    await cb.answer("⭐ Now primary. The previous one is no longer primary.", show_alert=True)


# ---- editor (Modify) ----
async def editor_view(wid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    w = await wl_get(wid)
    if not w:
        return None
    prim = await is_primary(wid)
    nb = len(await bt_list(wid))
    text = (
        f"✏️ <b>{escape(w['name'])}</b>  {'⭐ <i>primary</i>' if prim else ''}\n\n"
        f"📝 <b>Caption:</b>\n{short(w['caption'])}\n\n"
        f"🖼 <b>Media:</b> {MEDIA_LABEL.get(w['media_type'], '— none')}\n"
        f"🔘 <b>Buttons:</b> {nb}\n\n"
        "<i>Placeholders: {mention} {first_name} {full_name} {username} {chat_title}</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📝 Set Caption", f"ms:cap:{wid}"), btn("🖼 Set Media", f"ms:media:{wid}")],
        [btn(f"🔘 Buttons ({nb})", f"bt:menu:{wid}")],
        [btn("❌ Remove Media", f"ms:nomedia:{wid}"), btn("🏷 Rename", f"ms:name:{wid}")],
        [btn("👁 Preview (real look)", f"ms:view:{wid}")],
        [btn("⬅️ Back", f"ms:open:{wid}")],
    ])
    return text, kb


async def show_editor(target: Message | CallbackQuery, wid: int):
    view = await editor_view(wid)
    if not view:
        if isinstance(target, CallbackQuery):
            await target.answer("Message not found.", show_alert=True)
        return
    text, kb = view
    if isinstance(target, CallbackQuery):
        await edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("ms:edit:"), IsAdmin())
async def cb_ms_edit(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_editor(cb, int(cb.data.split(":")[2]))
    await cb.answer()


@router.callback_query(F.data.startswith("ms:nomedia:"), IsAdmin())
async def cb_ms_nomedia(cb: CallbackQuery):
    wid = int(cb.data.split(":")[2])
    await wl_update(wid, media_type="", media_id="")
    await show_editor(cb, wid)
    await cb.answer("Media removed")


# ---- caption ----
@router.callback_query(F.data.startswith("ms:cap:"), IsAdmin())
async def cb_ms_cap(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.wl_caption)
    await state.update_data(wid=int(cb.data.split(":")[2]))
    await cb.message.answer(
        "📝 Send the <b>caption / text</b> for this message.\n"
        "• Formatting (bold, italic, links, spoilers…) is kept\n"
        "• Send <code>-</code> to leave it empty (media only)\n\n"
        "<b>Placeholders:</b>\n"
        "<code>{mention}</code> – clickable name\n"
        "<code>{first_name}</code>, <code>{full_name}</code>, <code>{username}</code>\n"
        "<code>{chat_title}</code> – channel/group name\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.wl_caption, IsAdmin())
async def msg_wl_caption(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Send the caption as a text message (or /cancel).")
        return
    wid = (await state.get_data())["wid"]
    html = "" if message.text.strip() == "-" else message.html_text
    if len(html) > 4000:
        await message.answer("❌ Too long (max ~4000 characters).")
        return
    await wl_update(wid, caption=html)
    await state.clear()
    await message.answer("✅ Caption saved.")
    await show_editor(message, wid)


# ---- media ----
@router.callback_query(F.data.startswith("ms:media:"), IsAdmin())
async def cb_ms_media(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.wl_media)
    await state.update_data(wid=int(cb.data.split(":")[2]))
    await cb.message.answer(
        "🖼 Send the <b>photo, video or GIF</b> for this message.\n"
        "The caption is set separately – any caption you attach here is ignored.\n\n"
        "/cancel to abort."
    )
    await cb.answer()


@router.message(Form.wl_media, IsAdmin())
async def msg_wl_media(message: Message, state: FSMContext):
    if message.photo:
        mtype, fid = "photo", message.photo[-1].file_id
    elif message.video:
        mtype, fid = "video", message.video.file_id
    elif message.animation:
        mtype, fid = "animation", message.animation.file_id
    else:
        await message.answer("❌ Please send a photo, video or GIF (not as a file), or /cancel.")
        return
    wid = (await state.get_data())["wid"]
    await wl_update(wid, media_type=mtype, media_id=fid)
    await state.clear()
    await message.answer(f"✅ Media saved ({MEDIA_LABEL[mtype]}).")
    await show_editor(message, wid)


# ---- rename ----
@router.callback_query(F.data.startswith("ms:name:"), IsAdmin())
async def cb_ms_name(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.wl_name)
    await state.update_data(wid=int(cb.data.split(":")[2]))
    await cb.message.answer("🏷 Send a new <b>name</b> for this message (max 40 chars).\n\n/cancel to abort.")
    await cb.answer()


@router.message(Form.wl_name, IsAdmin())
async def msg_wl_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name or len(name) > 40:
        await message.answer("❌ Name must be 1–40 characters (or /cancel).")
        return
    wid = (await state.get_data())["wid"]
    await wl_update(wid, name=name)
    await state.clear()
    await message.answer("✅ Renamed.")
    await show_editor(message, wid)


# ---- view / preview (real destination look) ----
async def send_preview(bot: Bot, chat_id: int, wid: int, admin_user, reply_markup=None,
                       header: str = "👁 <b>Preview</b> — exactly what the receiver sees:") -> bool:
    w = await wl_get(wid)
    if not w:
        return False
    await bot.send_message(chat_id, header)
    try:
        sent = await deliver_welcome(bot, chat_id, w, admin_user, await last_chat_title(),
                                     reply_markup=reply_markup)
        if not sent:
            await bot.send_message(chat_id, "⚠️ This message is empty – add a caption or media.")
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await bot.send_message(chat_id, f"⚠️ Telegram rejected this message:\n<code>{escape(str(e))}</code>")
    return True


@router.callback_query(F.data.startswith("ms:view:"), IsAdmin())
async def cb_ms_view(cb: CallbackQuery, bot: Bot):
    ok = await send_preview(bot, cb.message.chat.id, int(cb.data.split(":")[2]), cb.from_user)
    if ok:
        await cb.answer()
    else:
        await cb.answer("Message not found.", show_alert=True)


# ---- delete ----
@router.callback_query(F.data.startswith("ms:del:"), IsAdmin())
async def cb_ms_del(cb: CallbackQuery):
    wid = int(cb.data.split(":")[2])
    w = await wl_get(wid)
    if not w:
        await cb.answer("Already deleted.", show_alert=True)
        return
    warn = ("\n\n⚠️ This is the <b>primary</b> message. After deleting it, join requests "
            "will get <b>no message</b> until you set another primary.") if await is_primary(wid) else ""
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        btn("✅ Yes, delete", f"ms:delyes:{wid}"), btn("❌ No", f"ms:open:{wid}"),
    ]])
    await edit(cb, f"🗑 Delete <b>{escape(w['name'])}</b>? This cannot be undone.{warn}", kb)
    await cb.answer()


@router.callback_query(F.data.startswith("ms:delyes:"), IsAdmin())
async def cb_ms_delyes(cb: CallbackQuery):
    await wl_delete(int(cb.data.split(":")[2]))
    await show_ms_menu(cb)
    await cb.answer("Deleted 🗑")


# ───────────────────────── 🔘 Message buttons (add-on) ─────────────────────────
# Admin → 💬 Massages → Message library → open/create a message → ✏️ Modify → 🔘 Buttons.
# TRACKED  button: the member taps it → opens this bot with a deep link → the bot counts the click
#                  and replies with the real link (one extra tap for the member).
# DIRECT   button: plain link, nothing is counted (Telegram never tells a bot about link taps).
BTN_MAX = 8                 # buttons per message
COUNT_ADMIN_CLICKS = True   # False = clicks made by admin accounts (your own tests) are not counted


def _cut(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def clean_url(raw: str | None) -> str | None:
    """Accepts https://…, http://…, tg://…, t.me/…, @channel or example.com; returns a full URL or None."""
    u = (raw or "").strip()
    if not u or len(u) > 500 or any(c.isspace() for c in u):
        return None
    low = u.lower()
    if low.startswith(("http://", "https://", "tg://")):
        return u
    if low.startswith(("t.me/", "telegram.me/")):
        return "https://" + u
    if u.startswith("@") and len(u) > 2 and u[1:].replace("_", "").isalnum():
        return "https://t.me/" + u[1:]
    if "." in u and not u.startswith((".", "/")):
        return "https://" + u
    return None


async def bt_list(wid: int) -> list[dict]:
    return await db_fetchall("SELECT * FROM msg_buttons WHERE welcome_id=? ORDER BY pos, id", wid)


async def bt_get(bid: int) -> dict | None:
    return await db_fetchone("SELECT * FROM msg_buttons WHERE id=?", bid)


async def bt_add(wid: int, label: str, url: str, tracked: int = 1) -> int:
    r = await db_fetchone("SELECT COALESCE(MAX(pos), 0) + 1 AS p FROM msg_buttons WHERE welcome_id=?", wid)
    return await db_insert_id(
        "INSERT INTO msg_buttons (welcome_id, label, url, tracked, pos, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        wid, label, url, tracked, int(r["p"]), time.time(),
    )


async def bt_update(bid: int, **fields):
    keys = [k for k in fields if k in {"label", "url", "tracked"}]
    if keys:
        sets = ", ".join(f"{k}=?" for k in keys)
        await db_execute(f"UPDATE msg_buttons SET {sets} WHERE id=?", *[fields[k] for k in keys], bid)


async def bt_delete(bid: int):
    await db_execute("DELETE FROM btn_clicks WHERE button_id=?", bid)
    await db_execute("DELETE FROM msg_buttons WHERE id=?", bid)


async def bt_move(bid: int, step: int):
    b = await bt_get(bid)
    if not b:
        return
    items = await bt_list(b["welcome_id"])
    ids = [int(x["id"]) for x in items]
    i = ids.index(int(bid))
    j = i + step
    if 0 <= j < len(ids):
        ids[i], ids[j] = ids[j], ids[i]
    for pos, x in enumerate(ids):
        await db_execute("UPDATE msg_buttons SET pos=? WHERE id=?", pos, x)


async def bt_stats(bid: int) -> dict:
    r = await db_fetchone(
        "SELECT COALESCE(SUM(n), 0) AS clicks, COUNT(*) AS users, MAX(last_ts) AS last "
        "FROM btn_clicks WHERE button_id=?", bid,
    )
    return {"clicks": int((r or {}).get("clicks") or 0), "users": int((r or {}).get("users") or 0),
            "last": (r or {}).get("last")}


async def bt_register_click(bid: int, user_id: int) -> dict | None:
    """Count one click of a tracked button. Returns the button (None if it no longer exists)."""
    b = await bt_get(bid)
    if not b:
        return None
    if user_id in ADMIN_IDS and not COUNT_ADMIN_CLICKS:
        return b
    now = time.time()
    await db_execute(
        "INSERT INTO btn_clicks (button_id, user_id, n, first_ts, last_ts) VALUES (?, ?, 1, ?, ?) "
        "ON CONFLICT(button_id, user_id) DO UPDATE SET n=btn_clicks.n+1, last_ts=excluded.last_ts",
        bid, user_id, now, now,
    )
    return b


async def attach_buttons(bot: Bot, w: dict, reply_markup=None):
    """Admin buttons first (one per row), then whatever the caller already had (e.g. Unsubscribe)."""
    rows = []
    try:
        items = await bt_list(int(w.get("id") or 0)) if w.get("id") else []
        if items:
            uname_ = (await bot.me()).username
            for b in items:
                url = f"https://t.me/{uname_}?start=btn_{b['id']}" if b["tracked"] and uname_ else b["url"]
                rows.append([InlineKeyboardButton(text=b["label"], url=url)])
    except Exception:
        log.exception("attach_buttons failed")
        rows = []
    if not rows:
        return reply_markup
    if reply_markup is not None:
        rows += [list(r) for r in reply_markup.inline_keyboard]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def button_clicks_text() -> str:
    rows = await db_fetchall(
        "SELECT b.id AS id, b.label AS label, w.name AS wname, COALESCE(SUM(c.n), 0) AS clicks, "
        "COUNT(c.user_id) AS users FROM msg_buttons b JOIN welcomes w ON w.id = b.welcome_id "
        "LEFT JOIN btn_clicks c ON c.button_id = b.id WHERE b.tracked = 1 "
        "GROUP BY b.id, b.label, w.name ORDER BY clicks DESC, b.id LIMIT 10"
    )
    if not rows:
        return "\n<b>🔘 Button clicks</b>\n   <i>no tracked buttons yet</i>"
    out = ["\n<b>🔘 Button clicks</b> <i>(tracked buttons)</i>"]
    for r in rows:
        out.append(f"   <b>{escape(_cut(r['label'], 24))}</b> <i>({escape(_cut(r['wname'], 16))})</i> — "
                   f"👆 <b>{int(r['clicks'])}</b> clicks · 👤 <b>{int(r['users'])}</b> users")
    return "\n".join(out)


async def bt_menu_view(wid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    w = await wl_get(wid)
    if not w:
        return None
    items = await bt_list(wid)
    lines = [f"🔘 <b>Buttons</b> · {escape(w['name'])}", ""]
    if not items:
        lines.append("<i>No buttons yet. Tap ➕ Add button.</i>")
    rows = []
    for i, b in enumerate(items, 1):
        if b["tracked"]:
            s = await bt_stats(b["id"])
            info = f"📊 {s['clicks']} clicks · {s['users']} users"
        else:
            info = "➡️ direct (not counted)"
        lines.append(f"{i}. <b>{escape(b['label'])}</b>\n    🔗 {escape(_cut(b['url'], 60))}\n    {info}")
        rows.append([btn(f"{i}. {_cut(b['label'], 28)}", f"bt:open:{b['id']}")])
    lines += ["", "<i>📊 Counted: the member taps it, the bot counts the click and shows the real link "
                  "(one extra tap). ➡️ Direct: plain link, Telegram gives the bot no click info.</i>"]
    if len(items) < BTN_MAX:
        rows.append([btn("➕ Add button", f"bt:add:{wid}")])
    rows.append([btn("👁 Preview", f"ms:view:{wid}"), btn("⬅️ Back", f"ms:edit:{wid}")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


async def bt_open_view(bid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    b = await bt_get(bid)
    if not b:
        return None
    if b["tracked"]:
        s = await bt_stats(bid)
        stat = (f"📊 <b>Clicks:</b> {s['clicks']} · <b>People:</b> {s['users']}\n"
                f"🕒 <b>Last click:</b> {fmt_ts(s['last'])}")
    else:
        stat = "📊 <i>Not counted (direct link)</i>"
    text = (
        f"🔘 <b>{escape(b['label'])}</b>\n\n"
        f"🔗 {escape(b['url'])}\n"
        f"Mode: {'📊 Counted (via bot)' if b['tracked'] else '➡️ Direct (no count)'}\n\n{stat}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("✏️ Name", f"bt:ren:{bid}"), btn("🔗 Link", f"bt:url:{bid}")],
        [btn("📊 Count clicks: ON ✅" if b["tracked"] else "📊 Count clicks: OFF ❌", f"bt:trk:{bid}")],
        [btn("⬆️ Up", f"bt:up:{bid}"), btn("⬇️ Down", f"bt:dn:{bid}"), btn("♻️ Reset clicks", f"bt:rst:{bid}")],
        [btn("🗑 Remove", f"bt:del:{bid}")],
        [btn("⬅️ Back", f"bt:menu:{b['welcome_id']}")],
    ])
    return text, kb


async def _bt_show_menu(target: Message | CallbackQuery, wid: int):
    view = await bt_menu_view(wid)
    if not view:
        if isinstance(target, CallbackQuery):
            await target.answer("Message not found.", show_alert=True)
        return
    if isinstance(target, CallbackQuery):
        await edit(target, *view)
    else:
        await target.answer(view[0], reply_markup=view[1])


async def _bt_show_open(target: Message | CallbackQuery, bid: int):
    view = await bt_open_view(bid)
    if not view:
        if isinstance(target, CallbackQuery):
            await target.answer("Button not found.", show_alert=True)
        return
    if isinstance(target, CallbackQuery):
        await edit(target, *view)
    else:
        await target.answer(view[0], reply_markup=view[1])


@router.callback_query(F.data.startswith("bt:menu:"), IsAdmin())
async def cb_bt_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await _bt_show_menu(cb, int(cb.data.split(":")[2]))
    await cb.answer()


@router.callback_query(F.data.startswith("bt:open:"), IsAdmin())
async def cb_bt_open(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await _bt_show_open(cb, int(cb.data.split(":")[2]))
    await cb.answer()


@router.callback_query(F.data.startswith("bt:add:"), IsAdmin())
async def cb_bt_add(cb: CallbackQuery, state: FSMContext):
    wid = int(cb.data.split(":")[2])
    if len(await bt_list(wid)) >= BTN_MAX:
        await cb.answer(f"Maximum {BTN_MAX} buttons per message.", show_alert=True)
        return
    await state.clear()
    await state.set_state(Form.bt_label)
    await state.update_data(wid=wid)
    await cb.message.answer("➕ <b>New button</b>\n\nSend the <b>button name</b> (1–40 characters), "
                            "e.g. <code>Join VIP</code>.\n\n/cancel to abort.")
    await cb.answer()


@router.callback_query(F.data.startswith("bt:ren:"), IsAdmin())
async def cb_bt_ren(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(Form.bt_label)
    await state.update_data(bid=int(cb.data.split(":")[2]))
    await cb.message.answer("✏️ Send the <b>new name</b> for this button (1–40 characters).\n\n/cancel to abort.")
    await cb.answer()


@router.callback_query(F.data.startswith("bt:url:"), IsAdmin())
async def cb_bt_url(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(Form.bt_url)
    await state.update_data(bid=int(cb.data.split(":")[2]))
    await cb.message.answer(URL_PROMPT)
    await cb.answer()


URL_PROMPT = ("🔗 Send the <b>link</b> this button opens.\n"
              "Examples: <code>https://example.com</code> · <code>t.me/yourchannel</code> · <code>@yourchannel</code>\n\n"
              "/cancel to abort.")


@router.message(Form.bt_label, IsAdmin())
async def msg_bt_label(message: Message, state: FSMContext):
    label = (message.text or "").strip()
    if not label or len(label) > 40:
        await message.answer("❌ The name must be 1–40 characters (or /cancel).")
        return
    data = await state.get_data()
    if data.get("bid"):  # editing the name of an existing button
        await bt_update(int(data["bid"]), label=label)
        await state.clear()
        await message.answer("✅ Name updated.")
        await _bt_show_open(message, int(data["bid"]))
        return
    await state.update_data(label=label)
    await state.set_state(Form.bt_url)
    await message.answer(URL_PROMPT)


@router.message(Form.bt_url, IsAdmin())
async def msg_bt_url(message: Message, state: FSMContext):
    url = clean_url(message.text)
    if not url:
        await message.answer("❌ That does not look like a link. Try <code>https://example.com</code> "
                             "or <code>t.me/yourchannel</code> (or /cancel).")
        return
    data = await state.get_data()
    await state.clear()
    if data.get("bid"):  # editing the link of an existing button
        await bt_update(int(data["bid"]), url=url)
        await message.answer("✅ Link updated.")
        await _bt_show_open(message, int(data["bid"]))
        return
    if len(await bt_list(int(data["wid"]))) >= BTN_MAX:
        await message.answer(f"❌ Maximum {BTN_MAX} buttons per message.")
        await _bt_show_menu(message, int(data["wid"]))
        return
    bid = await bt_add(int(data["wid"]), data["label"], url, 1)
    await message.answer("✅ Button added (clicks are counted).")
    await _bt_show_open(message, bid)


@router.callback_query(F.data.startswith("bt:trk:"), IsAdmin())
async def cb_bt_trk(cb: CallbackQuery):
    bid = int(cb.data.split(":")[2])
    b = await bt_get(bid)
    if not b:
        await cb.answer("Button not found.", show_alert=True)
        return
    await bt_update(bid, tracked=0 if b["tracked"] else 1)
    await _bt_show_open(cb, bid)
    await cb.answer("Counting OFF – direct link" if b["tracked"] else "Counting ON")


@router.callback_query(F.data.startswith(("bt:up:", "bt:dn:")), IsAdmin())
async def cb_bt_move(cb: CallbackQuery):
    _, kind, bid = cb.data.split(":")
    await bt_move(int(bid), -1 if kind == "up" else 1)
    await _bt_show_open(cb, int(bid))
    await cb.answer("Moved")


@router.callback_query(F.data.startswith("bt:rst:"), IsAdmin())
async def cb_bt_rst(cb: CallbackQuery):
    bid = int(cb.data.split(":")[2])
    await db_execute("DELETE FROM btn_clicks WHERE button_id=?", bid)
    await _bt_show_open(cb, bid)
    await cb.answer("Click counter reset to 0")


@router.callback_query(F.data.startswith("bt:del:"), IsAdmin())
async def cb_bt_del(cb: CallbackQuery):
    bid = int(cb.data.split(":")[2])
    b = await bt_get(bid)
    if not b:
        await cb.answer("Already removed.", show_alert=True)
        return
    s = await bt_stats(bid)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        btn("✅ Yes, remove", f"bt:delyes:{bid}"), btn("❌ No", f"bt:open:{bid}"),
    ]])
    await edit(cb, f"🗑 Remove button <b>{escape(b['label'])}</b>?\n"
                   f"Its {s['clicks']} counted click(s) are deleted too. Messages already sent keep the old "
                   "button, but it will answer “no longer available”.", kb)
    await cb.answer()


@router.callback_query(F.data.startswith("bt:delyes:"), IsAdmin())
async def cb_bt_delyes(cb: CallbackQuery):
    bid = int(cb.data.split(":")[2])
    b = await bt_get(bid)
    if b:
        await bt_delete(bid)
        await _bt_show_menu(cb, int(b["welcome_id"]))
    await cb.answer("Removed 🗑")


# ───────────────────────── Queue tools ─────────────────────────
async def queue_view() -> tuple[str, InlineKeyboardMarkup]:
    parts = [
        f"📊 <b>Universal Status</b>  <i>({escape(TZ_NAME)})</i>\n"
        "<i>Requests grouped by the day they were received.</i>"
    ]
    for label, t0, t1 in time_windows():
        st = await window_stats(t0, t1)
        total = st["pending"] + st["approved"] + st["failed"]
        ls = await layer_stats(t0, t1)
        parts.append(
            f"\n<b>{label}</b> — {total} request(s)\n"
            f"   ⏳ Pending <b>{st['pending']}</b> · ✅ Approved <b>{st['approved']}</b> · "
            f"⚠️ Failed <b>{st['failed']}</b>\n"
            f"   🤖 Bot users: <b>{st['bot_users']}</b> of {st['users']} requester(s)\n"
            + layer_text(ls)
        )
    ov = await user_overview()
    parts.append(
        "\n<b>🤖 Bot users</b> <i>(started the bot, can receive DMs)</i>\n"
        f"   Known users: <b>{ov['total']}</b> · Started: <b>{ov['started']}</b>\n"
        f"   ✅ Subscribed: <b>{ov['audience']}</b> · 🔕 Unsubscribed: <b>{ov['unsub']}</b>\n"
        f"   🚫 Blocked / deleted the bot: <b>{ov['blocked']}</b>"
    )
    parts.append(await button_clicks_text())
    dests = await dest_all()
    if dests:
        parts.append("\n<b>📡 Destinations</b>")
        for d in dests[:15]:
            dc = await counts(d["chat_id"])
            dot, _ = dest_status(d, dc["pending"])
            parts.append(
                f"{dot} {escape(d['title'] or 'Untitled')} — ⏳ <b>{dc['pending']}</b> · "
                f"✅ <b>{dc['approved']}</b> · ⚠️ <b>{dc['failed']}</b>"
            )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("🔄 Refresh", "queue"), btn("⬅️ Back", "refresh")],
    ])
    return "\n".join(parts), kb


@router.callback_query(F.data == "queue", IsAdmin())
async def cb_queue(cb: CallbackQuery):
    text, kb = await queue_view()
    await edit(cb, text, kb)
    await cb.answer()


@router.callback_query(F.data == "retry_failed", IsAdmin())
async def cb_retry(cb: CallbackQuery):
    n = await bulk_update(
        "UPDATE queue SET status='pending', error=NULL, processed_at=NULL WHERE status='failed'"
    )
    text, kb = await acs_view()
    await edit(cb, text, kb)
    await cb.answer(f"{n} failed request(s) re-queued")


# ───────────────────────── UserInfo (subscribed / unsubscribed lists) ─────────────────────────
UI_PAGE = 15


async def ui_menu_view() -> tuple[str, InlineKeyboardMarkup]:
    ov = await user_overview()
    hist = await db_fetchall("SELECT * FROM broadcasts ORDER BY id DESC LIMIT 3")
    text = (
        "👤 <b>UserInfo</b>\n\n"
        f"✅ Subscribed: <b>{ov['audience']}</b>\n"
        f"      🤖 of them started the bot: <b>{ov['reach_sub']}</b>\n"
        f"🔕 Unsubscribed: <b>{ov['unsub']}</b>\n"
        f"🚫 Blocked / deleted the bot: <b>{ov['blocked']}</b>\n"
        f"👥 Total members: <b>{ov['total']}</b>\n\n"
        "<i>Subscribed = requested to join your channel/group and has not unsubscribed yet.\n"
        "Telegram only lets a bot DM people who started it; others show as unreachable "
        "in broadcast results.\n"
        "Broadcast from 💬 Massages → open a message → 📣 Broadcast.</i>"
    )
    if hist:
        text += "\n\n<b>Recent broadcasts</b>"
        for h in hist:
            text += (
                f"\n#{h['id']} · {fmt_ts(h['created_at'])} — ✅ {h['sent']} · "
                f"🚫 {h['blocked']} · ⚠️ {h['failed']} / {h['total']} ({h['status']})"
            )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn(f"✅ Subscribed ({ov['audience']})", "ui:list:sub:0"),
         btn(f"🔕 Unsubscribed ({ov['unsub']})", "ui:list:unsub:0")],
        [btn("🔄 Refresh", "ui:menu"), btn("⬅️ Back", "refresh")],
    ])
    return text, kb


@router.callback_query(F.data == "ui:menu", IsAdmin())
async def cb_ui_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    text, kb = await ui_menu_view()
    await edit(cb, text, kb)
    await cb.answer()


def ui_query(kind: str) -> tuple[str, str]:
    if kind == "sub":
        return "subscribed=1", "first_seen DESC"
    return "subscribed=0", "unsub_at DESC"


@router.callback_query(F.data.startswith("ui:list:"), IsAdmin())
async def cb_ui_list(cb: CallbackQuery):
    _, _, kind, page_s = cb.data.split(":")
    ov = await user_overview()
    total = ov["audience"] if kind == "sub" else ov["unsub"]
    pages = max(1, -(-total // UI_PAGE))
    page = min(max(int(page_s), 0), pages - 1)
    where, order = ui_query(kind)
    rows = await db_fetchall(
        f"SELECT user_id, username, full_name, started, first_seen, unsub_at FROM users "
        f"WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?",
        UI_PAGE, page * UI_PAGE,
    )
    title = "✅ <b>Subscribed members</b>" if kind == "sub" else "🔕 <b>Unsubscribed members</b>"
    if not rows:
        text = f"{title}\n\n<i>Nobody here yet.</i>"
    else:
        lines = [
            f"• {escape(r['full_name'] or 'User')}"
            f"{' (@' + escape(r['username']) + ')' if r['username'] else ''} "
            f"<code>{r['user_id']}</code> — "
            f"{fmt_ts(r['first_seen']) if kind == 'sub' else fmt_ts(r['unsub_at'])}"
            f"{' 🤖' if r['started'] else ''}"
            for r in rows
        ]
        text = f"{title} ({total}) — page {page + 1}/{pages}\n\n" + "\n".join(lines)
        text += "\n\n<i>🤖 = started the bot (can receive DMs)</i>"
    nav = []
    if page > 0:
        nav.append(btn("⬅️ Prev", f"ui:list:{kind}:{page - 1}"))
    if page < pages - 1:
        nav.append(btn("Next ➡️", f"ui:list:{kind}:{page + 1}"))
    kb_rows = ([nav] if nav else []) + [
        [btn("📤 Export CSV", f"ui:export:{kind}")],
        [btn("⬅️ Back", "ui:menu")],
    ]
    await edit(cb, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await cb.answer()


@router.callback_query(F.data.startswith("ui:export:"), IsAdmin())
async def cb_ui_export(cb: CallbackQuery):
    kind = cb.data.split(":")[2]
    where, order = ui_query(kind)
    rows = await db_fetchall(
        f"SELECT user_id, username, full_name, started, first_seen, unsub_at FROM users WHERE {where} ORDER BY {order}"
    )
    if not rows:
        await cb.answer("Nothing to export.", show_alert=True)
        return
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id", "username", "full_name", "started_bot", "first_seen", "unsubscribed_at"])
    for r in rows:
        w.writerow([r["user_id"], r["username"] or "", r["full_name"] or "",
                    "yes" if r["started"] else "no",
                    fmt_ts(r["first_seen"]), fmt_ts(r["unsub_at"])])
    name = "subscribed_users.csv" if kind == "sub" else "unsubscribed_users.csv"
    await cb.message.answer_document(
        BufferedInputFile(buf.getvalue().encode("utf-8-sig"), filename=name),
        caption=f"{'✅' if kind == 'sub' else '🔕'} {len(rows)} member(s)",
    )
    await cb.answer()


# ───────────────────────── Broadcast (from a saved message) ─────────────────────────
_bc_stops: dict[int, asyncio.Event] = {}
_bg_tasks: set = set()

AUDIENCES = {
    "sub": ("✅ Subscribed", "subscribed=1"),
    "unsub": ("🔕 Unsubscribed", "subscribed=0"),
    "both": ("👥 Both", "1=1"),
}


def unsub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("🔕 Unsubscribe", "bc:unsub")]])


def resub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("🔔 Subscribe again", "bc:resub")]])


async def audience_counts() -> dict:
    ov = await user_overview()
    if await get_setting("bc_exclude_blocked") == "1":  # blocked / deleted-bot members left out
        return {"sub": ov["audience"] - ov["sub_blocked"], "unsub": ov["unsub"] - ov["unsub_blocked"],
                "both": ov["total"] - ov["blocked"]}
    return {"sub": ov["audience"], "unsub": ov["unsub"], "both": ov["total"]}


async def _bc_menu(cb: CallbackQuery, wid: int):
    """Step 1 – ask WHICH list should receive the broadcast (+ include/exclude blocked members)."""
    w = await wl_get(wid)
    if not w:
        await cb.answer("Message not found.", show_alert=True)
        return
    n = await audience_counts()
    ov = await user_overview()
    excl = await get_setting("bc_exclude_blocked") == "1"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn(f"✅ Subscribed members ({n['sub']})", f"ms:bca:{wid}:sub")],
        [btn(f"🔕 Unsubscribed members ({n['unsub']})", f"ms:bca:{wid}:unsub")],
        [btn(f"👥 Both ({n['both']})", f"ms:bca:{wid}:both")],
        [btn(f"🚫 Blocked/deleted bot ({ov['blocked']}): {'EXCLUDED ✅' if excl else 'INCLUDED'} · tap to switch",
             f"ms:bcblk:{wid}")],
        [btn("⬅️ Back", f"ms:open:{wid}")],
    ])
    await edit(
        cb,
        f"📣 <b>Broadcast “{escape(w['name'])}”</b>\n\nWho should receive it?\n\n"
        "<i>Subscribed = requested to join and not unsubscribed. Telegram lets a bot DM only "
        f"people who started it: {ov['reach_sub']} of {ov['audience']} subscribed and "
        f"{ov['unsub_reach']} of {ov['unsub']} unsubscribed members can be reached now – "
        "the rest will show as unreachable. Unsubscribed members asked not to be messaged, "
        "so send to them only if you really need to.</i>\n\n"
        f"🚫 <b>{ov['blocked']}</b> member(s) blocked or deleted the bot — they are currently "
        f"<b>{'EXCLUDED from' if excl else 'INCLUDED in'}</b> broadcasts (the counts above follow this switch).",
        kb,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("ms:bc:"), IsAdmin())
async def cb_ms_bc(cb: CallbackQuery):
    await _bc_menu(cb, int(cb.data.split(":")[2]))


@router.callback_query(F.data.startswith("ms:bcblk:"), IsAdmin())
async def cb_ms_bcblk(cb: CallbackQuery):
    """Switch: include / exclude members who blocked or deleted the bot."""
    wid = int(cb.data.split(":")[2])
    on = await get_setting("bc_exclude_blocked") == "1"
    await set_setting("bc_exclude_blocked", "0" if on else "1")
    await _bc_menu(cb, wid)


@router.callback_query(F.data.startswith("ms:bca:"), IsAdmin())
async def cb_ms_bca(cb: CallbackQuery, bot: Bot):
    """Step 2 – preview + confirmation."""
    _, _, wid_s, aud = cb.data.split(":")
    wid = int(wid_s)
    w = await wl_get(wid)
    if not w or aud not in AUDIENCES:
        await cb.answer("Message not found.", show_alert=True)
        return
    if not (w["caption"] or "").strip() and not w["media_id"]:
        await cb.answer("This message is empty – add a caption or media first.", show_alert=True)
        return
    n = (await audience_counts())[aud]
    if n == 0:
        await cb.answer("No members in that list.", show_alert=True)
        return
    ov = await user_overview()
    reach = {"sub": ov["reach_sub"], "unsub": ov["unsub_reach"],
             "both": ov["reach_sub"] + ov["unsub_reach"]}[aud]
    mk = resub_kb() if aud == "unsub" else unsub_kb()
    await send_preview(bot, cb.message.chat.id, wid, cb.from_user, reply_markup=mk,
                       header="👁 <b>Broadcast preview</b> — exactly what members receive:")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        btn(f"✅ Send to {n}", f"ms:bcgo:{wid}:{aud}"), btn("❌ Cancel", f"ms:open:{wid}"),
    ]])
    await cb.message.answer(
        f"📣 <b>{escape(w['name'])}</b>\nAudience: <b>{AUDIENCES[aud][0]}</b> — {n} member(s)\n"
        f"🤖 Can receive DMs now: ~{reach} (others will show as unreachable)\n\nSend now?",
        reply_markup=kb,
    )
    await cb.answer()


@router.callback_query(F.data.startswith("ms:bcgo:"), IsAdmin())
async def cb_ms_bcgo(cb: CallbackQuery, bot: Bot):
    _, _, wid_s, aud = cb.data.split(":")
    wid = int(wid_s)
    if aud not in AUDIENCES or not await wl_get(wid):
        await cb.answer("Message not found.", show_alert=True)
        return
    if _bc_stops:
        await cb.answer("A broadcast is already running.", show_alert=True)
        return
    total = (await audience_counts())[aud]
    bid = await db_insert_id(
        "INSERT INTO broadcasts (created_at, total, status) VALUES (?, ?, 'running')",
        time.time(), total,
    )
    task = asyncio.create_task(run_broadcast(bot, bid, cb.message.chat.id, wid, aud))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    await edit(cb, "🚀 Broadcast started…", InlineKeyboardMarkup(inline_keyboard=[]))
    await cb.answer()


async def send_one(bot: Bot, u: dict, w: dict, chat_title: str) -> str:
    uid = u["user_id"]
    name = u.get("full_name") or "friend"
    user = SimpleNamespace(
        id=uid, username=u.get("username"), full_name=name, first_name=name.split()[0],
    )
    mk = unsub_kb() if u["subscribed"] == 1 else resub_kb()
    for _ in range(3):
        try:
            await deliver_welcome(bot, uid, w, user, chat_title, reply_markup=mk)
            await note_ok(uid)
            return "sent"
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except TelegramForbiddenError as e:
            await set_started(uid, False)  # user blocked the bot
            await note_forbidden(uid, e)
            return "blocked"
        except TelegramBadRequest:
            return "failed"
        except Exception:
            log.exception("Broadcast send error")
            return "failed"
    return "failed"


async def run_broadcast(bot: Bot, bid: int, admin_chat: int, wid: int, aud: str):
    stop = _bc_stops[bid] = asyncio.Event()
    stop_kb = InlineKeyboardMarkup(inline_keyboard=[[btn("⏹ Stop", f"bc:stop:{bid}")]])
    sent = failed = blocked = 0
    status = "done"
    try:
        w = await wl_get(wid)
        chat_title = await last_chat_title()
        excl = " AND blocked=0" if await get_setting("bc_exclude_blocked") == "1" else ""
        users = await db_fetchall(
            f"SELECT user_id, username, full_name, subscribed FROM users "
            f"WHERE {AUDIENCES[aud][1]}{excl} ORDER BY user_id"
        )
        total = len(users)
        mlog(f"📣 Broadcast started · {AUDIENCES[aud][0]} · {total} users")
        prog = await bot.send_message(admin_chat, f"📣 Broadcasting… 0/{total}", reply_markup=stop_kb)
        last_edit = time.time()
        for i, u in enumerate(users, 1):
            if stop.is_set():
                status = "stopped"
                break
            res = await send_one(bot, u, w, chat_title)
            if res == "sent":
                sent += 1
            elif res == "blocked":
                blocked += 1
            else:
                failed += 1
            await asyncio.sleep(0.05)  # ≈ 20 msgs/sec, under Telegram's limits
            if time.time() - last_edit > 3:
                last_edit = time.time()
                try:
                    await bot.edit_message_text(
                        f"📣 Broadcasting… {i}/{total}\n✅ {sent} · 🚫 {blocked} · ⚠️ {failed}",
                        chat_id=admin_chat, message_id=prog.message_id, reply_markup=stop_kb,
                    )
                except TelegramBadRequest:
                    pass
        await db_execute(
            "UPDATE broadcasts SET sent=?, failed=?, blocked=?, status=? WHERE id=?",
            sent, failed, blocked, status, bid,
        )
        mlog(f"{'⏹ Broadcast stopped' if status == 'stopped' else '✅ Broadcast done'} · "
             f"✅ {sent} · 🚫 {blocked} · ⚠️ {failed}")
        try:
            await bot.edit_message_text(
                f"{'⏹ Broadcast stopped' if status == 'stopped' else '✅ Broadcast finished'}\n\n"
                f"📨 Audience: <b>{AUDIENCES[aud][0]}</b> ({total})\n"
                f"✅ Delivered: <b>{sent}</b>\n🚫 Unreachable (never started / blocked): <b>{blocked}</b>\n"
                f"⚠️ Failed: <b>{failed}</b>",
                chat_id=admin_chat, message_id=prog.message_id,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[btn("⬅️ Back", "ms:menu")]]),
            )
        except TelegramBadRequest:
            pass
    except Exception:
        log.exception("Broadcast crashed")
        await db_execute("UPDATE broadcasts SET status='error' WHERE id=?", bid)
    finally:
        _bc_stops.pop(bid, None)


@router.callback_query(F.data.startswith("bc:stop:"), IsAdmin())
async def cb_bc_stop(cb: CallbackQuery):
    ev = _bc_stops.get(int(cb.data.split(":")[2]))
    if ev:
        ev.set()
    await cb.answer("Stopping…")


# ---- user-side: unsubscribe / subscribe again (open to everyone) ----
@router.callback_query(F.data == "bc:unsub")
async def cb_unsub(cb: CallbackQuery, bot: Bot):
    await touch_user(cb.from_user, True)
    await db_execute(
        "UPDATE users SET subscribed=0, unsub_at=? WHERE user_id=?", time.time(), cb.from_user.id
    )
    mlog(f"🔕 {uname(cb.from_user)} unsubscribed", ("un",), lambda n: f"🔕 ×{n} unsubscribed")
    await cb.answer("🔕 You're unsubscribed. This chat is being cleared.", show_alert=True)
    extra = [cb.message.message_id] if cb.message else []
    await wipe_chat(bot, cb.from_user.id, extra)  # private chat id == user id


@router.callback_query(F.data == "bc:resub")
async def cb_resub(cb: CallbackQuery):
    await touch_user(cb.from_user, True)
    await db_execute("UPDATE users SET subscribed=1, unsub_at=NULL WHERE user_id=?", cb.from_user.id)
    mlog(f"🔔 {uname(cb.from_user)} resubscribed")
    try:
        await cb.message.edit_reply_markup(reply_markup=unsub_kb())
    except TelegramBadRequest:
        pass
    await cb.answer("🔔 Subscribed again.", show_alert=True)


# ───────────────────────── 📡 Destinations ─────────────────────────
async def dest_menu_view(bot: Bot) -> tuple[str, InlineKeyboardMarkup]:
    dests = await dest_all()
    await asyncio.gather(*(refresh_members(bot, d) for d in dests))
    pend = {int(cid): n for cid, _t, n in await pending_by_chat()}
    left = await leave_counts()
    text = (
        "📡 <b>Destinations</b>\n\n"
        "Numbers on each button:  👥 members | ⏳ requested (waiting) | 🚪 left\n"
        "🟢 accepting requests · 🟡 other activity · 🔵 only new posts · "
        f"🔴 nothing for {ACTIVITY_DAYS} days\n\n"
        + ("Tap a destination to manage it." if dests else
           "No destination yet. Add the bot as admin to a channel/group (see ➕ Add destination).")
        + "\n<i>“Left” is counted from the moment this version started tracking.</i>"
    )
    rows = []
    for d in dests[:40]:
        cid = d["chat_id"]
        dot, _ = dest_status(d, pend.get(cid, 0))
        label = (f"{dot} {(d['title'] or str(cid))[:22]} · "
                 f"{kfmt(d['members'])} | {pend.get(cid, 0)} | {left.get(cid, 0)}")
        rows.append([btn(label, f"dst:open:{cid}")])
    rows.append([btn("➕ Add destination", "dst:add")])
    rows.append([btn("🔄 Refresh", "dst:menu"), btn("⬅️ Back", "refresh")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "dst:menu", IsAdmin())
async def cb_dst_menu(cb: CallbackQuery, bot: Bot, state: FSMContext):
    await state.clear()
    text, kb = await dest_menu_view(bot)
    await edit(cb, text, kb)
    await cb.answer()


@router.callback_query(F.data == "dst:add", IsAdmin())
async def cb_dst_add(cb: CallbackQuery):
    await edit(
        cb,
        "➕ <b>Add destination</b>\n\n"
        "1. Open your channel/group → Administrators → add this bot.\n"
        "2. Give it the permission <b>Invite Users via Link / Add Members</b> "
        "(and, in channels, it can also see new posts).\n"
        "3. The destination appears automatically in 📡 Destinations "
        "(or as soon as the first join request arrives).\n\n"
        "<i>Tip: for join requests, the invite link must have “Request admin approval” on.</i>",
        InlineKeyboardMarkup(inline_keyboard=[[btn("🔄 Check again", "dst:menu"), btn("⬅️ Back", "dst:menu")]]),
    )
    await cb.answer()


async def dest_view(bot: Bot, cid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    d = await dest_get(cid)
    if not d:
        return None
    await refresh_members(bot, d)
    c = await counts(cid)
    ev = await event_counts(cid)
    dot, why = dest_status(d, c["pending"])
    cfg = cfg_from_dest(d)
    text = (
        f"{dot} <b>{escape(d['title'] or str(cid))}</b>  <i>({escape(d['chat_type'] or 'chat')})</i>\n"
        f"<i>{why}</i>\n\n"
        f"👥 Members: <b>{d['members']}</b>\n"
        f"⏳ Requested (waiting): <b>{c['pending']}</b> · ✅ Approved: <b>{c['approved']}</b> · "
        f"⚠️ Failed: <b>{c['failed']}</b>\n"
        f"➕ Joined: <b>{ev['join']}</b> · 🚪 Left: <b>{ev['leave']}</b>\n\n"
        f"Accepting: {'🟢 <b>RUNNING</b>' if d['running'] == 1 else '🔴 <b>STOPPED</b>'}\n"
        f"Timer: <b>{describe_interval(cfg)}</b>\n"
        "<i>Joined/Left are counted from the moment this version started tracking.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("✅ Accept request", f"ac:menu:{cid}"), btn("🔗 Ask a link", f"lk:new:{cid}")],
        [btn("📈 Status", f"lk:list:{cid}"), btn("💬 Set massage", f"dmx:{cid}")],
        [btn("🔄 Refresh", f"dst:open:{cid}"), btn("⬅️ Back", "dst:menu")],
    ])
    return text, kb


@router.callback_query(F.data.startswith("dst:open:"), IsAdmin())
async def cb_dst_open(cb: CallbackQuery, bot: Bot, state: FSMContext):
    await state.clear()
    view = await dest_view(bot, int(cb.data.split(":")[2]))
    if not view:
        await cb.answer("Destination not found.", show_alert=True)
        return
    await edit(cb, *view)
    await cb.answer()


@router.callback_query(F.data.startswith("dst:soon:"), IsAdmin())
async def cb_dst_soon(cb: CallbackQuery):
    await cb.answer("🚧 This part is coming in the next step.", show_alert=True)


# ───────────────────────── ✅ Accept request (one destination) ─────────────────────────
async def ac_view(cid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    d = await dest_get(cid)
    if not d:
        return None
    c = await counts(cid)
    cfg = cfg_from_dest(d)
    running = d["running"] == 1
    text = (
        f"✅ <b>Accept requests — {escape(d['title'] or str(cid))}</b>\n\n"
        f"Status: {'🟢 <b>RUNNING</b>' if running else '🔴 <b>STOPPED</b>'}\n"
        f"Timer: <b>{describe_interval(cfg)}</b>\n\n"
        f"⏳ Pending: <b>{c['pending']}</b> (≈ {fmt_duration(c['pending'] * avg_interval(cfg))})\n"
        f"✅ Approved: <b>{c['approved']}</b> · ⚠️ Failed: <b>{c['failed']}</b>\n\n"
        "<i>While stopped, requests keep queuing and welcome messages still go out.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("⏹ Stop accepting" if running else "▶️ Start accepting", f"ac:run:{cid}")],
        [btn("⏱ Set timer", f"iv:menu:{cid}")],
        [btn("👁 Joins per link", f"lk:list:{cid}"), btn("♻️ Retry failed", f"ac:retry:{cid}")],
        [btn("⬅️ Back", f"dst:open:{cid}")],
    ])
    return text, kb


async def show_ac(cb: CallbackQuery, cid: int) -> bool:
    view = await ac_view(cid)
    if not view:
        await cb.answer("Destination not found.", show_alert=True)
        return False
    await edit(cb, *view)
    return True


@router.callback_query(F.data.startswith("ac:menu:"), IsAdmin())
async def cb_ac_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    if await show_ac(cb, int(cb.data.split(":")[2])):
        await cb.answer()


@router.callback_query(F.data.startswith("ac:run:"), IsAdmin())
async def cb_ac_run(cb: CallbackQuery):
    cid = int(cb.data.split(":")[2])
    d = await dest_get(cid)
    if not d:
        await cb.answer("Destination not found.", show_alert=True)
        return
    new = 0 if d["running"] == 1 else 1
    await db_execute("UPDATE destinations SET running=? WHERE chat_id=?", new, cid)
    _next_due.pop(cid, None)
    mlog(f"{'▶️ Accepting started' if new else '⏹ Accepting stopped'} · <b>{escape(d['title'] or '')}</b>")
    await show_ac(cb, cid)
    await cb.answer("Started ▶️" if new else "Stopped ⏹")


@router.callback_query(F.data.startswith("ac:retry:"), IsAdmin())
async def cb_ac_retry(cb: CallbackQuery):
    cid = int(cb.data.split(":")[2])
    n = await db_execute(
        "UPDATE queue SET status='pending', error=NULL, processed_at=NULL "
        "WHERE status='failed' AND chat_id=?", cid,
    )
    await show_ac(cb, cid)
    await cb.answer(f"{n} failed request(s) re-queued")


# ───────────────────────── ✅ Acceptors (global control) ─────────────────────────
async def acs_view() -> tuple[str, InlineKeyboardMarkup]:
    dests = await dest_all()
    pend = {int(cid): n for cid, _t, n in await pending_by_chat()}
    running = sum(1 for d in dests if d["running"] == 1)
    lines = [
        f"{'🟢' if d['running'] == 1 else '🔴'} {escape(d['title'] or str(d['chat_id']))} — "
        f"{describe_interval(cfg_from_dest(d))} · ⏳ <b>{pend.get(d['chat_id'], 0)}</b>"
        for d in dests[:30]
    ]
    text = (
        "✅ <b>Acceptors</b>\n"
        "<i>Accepts the requested members one by one, with the timer you set.</i>\n\n"
        f"Accepting on <b>{running}</b> of <b>{len(dests)}</b> destination(s)\n\n"
        + ("\n".join(lines) if lines else "No destinations yet.")
    )
    rows = [
        [btn("▶️ Start all", "acs:all:1"), btn("⏹ Stop all", "acs:all:0")],
        [btn("⏱ Set timer for all", "iv:menu:all"), btn("♻️ Retry failed (all)", "retry_failed")],
    ]
    for d in dests[:30]:
        rows.append([btn(
            f"{'⏹ Stop' if d['running'] == 1 else '▶️ Start'} · {(d['title'] or str(d['chat_id']))[:26]}",
            f"acs:tog:{d['chat_id']}",
        )])
    rows.append([btn("🔄 Refresh", "acs:menu"), btn("⬅️ Back", "refresh")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "acs:menu", IsAdmin())
async def cb_acs_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    text, kb = await acs_view()
    await edit(cb, text, kb)
    await cb.answer()


@router.callback_query(F.data.startswith("acs:all:"), IsAdmin())
async def cb_acs_all(cb: CallbackQuery):
    on = 1 if cb.data.split(":")[2] == "1" else 0
    await db_execute("UPDATE destinations SET running=? WHERE active=1", on)
    if on:
        _next_due.clear()
    mlog(f"{'▶️ Accepting started' if on else '⏹ Accepting stopped'} · <b>ALL</b>")
    text, kb = await acs_view()
    await edit(cb, text, kb)
    await cb.answer("All started ▶️" if on else "All stopped ⏹")


@router.callback_query(F.data.startswith("acs:tog:"), IsAdmin())
async def cb_acs_tog(cb: CallbackQuery):
    cid = int(cb.data.split(":")[2])
    d = await dest_get(cid)
    if d:
        new = 0 if d["running"] == 1 else 1
        await db_execute("UPDATE destinations SET running=? WHERE chat_id=?", new, cid)
        _next_due.pop(cid, None)
        mlog(f"{'▶️ Accepting started' if new else '⏹ Accepting stopped'} · <b>{escape(d['title'] or '')}</b>")
    text, kb = await acs_view()
    await edit(cb, text, kb)
    await cb.answer()


# ───────────────────────── Destination tracking (bot status, joins/leaves, posts) ─────────────────────────
_GROUPISH = {"channel", "group", "supergroup"}


@router.my_chat_member(F.chat.type.in_(_GROUPISH))
async def on_bot_chat_status(ev: ChatMemberUpdated):
    """Bot promoted to admin → register destination. Removed → hide it."""
    st = ev.new_chat_member.status
    if st == "administrator":
        await upsert_destination(ev.chat.id, ev.chat.title or "", ev.chat.type)
        log.info("Destination registered: %s (%s)", ev.chat.title, ev.chat.id)
        if ev.chat.id != _mlog_chat:
            mlog(f"📡 Bot added · <b>{escape(ev.chat.title or '')}</b>")
    elif st in ("left", "kicked"):
        await db_execute("UPDATE destinations SET active=0, running=0 WHERE chat_id=?", ev.chat.id)
        log.info("Bot removed from %s (%s)", ev.chat.title, ev.chat.id)
        if ev.chat.id != _mlog_chat:
            mlog(f"🚫 Bot removed · <b>{escape(ev.chat.title or '')}</b>")


def _in_chat(m) -> bool:
    if m.status in ("member", "administrator", "creator"):
        return True
    if m.status == "restricted":
        return bool(getattr(m, "is_member", True))
    return False


@router.chat_member(F.chat.type.in_(_GROUPISH))
async def on_member_change(ev: ChatMemberUpdated):
    """Count joins / leaves (needs the bot to be an admin of the chat)."""
    was, now = _in_chat(ev.old_chat_member), _in_chat(ev.new_chat_member)
    if was == now or ev.new_chat_member.user.is_bot or ev.chat.id == _mlog_chat:
        return
    if not await dest_get(ev.chat.id):
        await upsert_destination(ev.chat.id, ev.chat.title or "", ev.chat.type)
    uid = ev.new_chat_member.user.id
    await record_event(ev.chat.id, uid, "join" if now else "leave")
    _mlog_event.set()  # master log: joined / left counters changed
    if now:  # joined: through which of our links?
        lid = await link_id_by_url(
            ev.chat.id, ev.invite_link.invite_link if ev.invite_link else None
        ) or await link_for_request(ev.chat.id, uid)
        if lid:
            await record_link_event(ev.chat.id, lid, uid, "join")
    if now:  # post-request message: sent when the person has really joined
        try:
            await on_real_join(ev.chat.id, uid)
        except Exception:
            log.exception("post-request scheduling failed")
    who, where = uname(ev.new_chat_member.user), escape(ev.chat.title or "")
    if not now:
        mlog(f"🚪 {who} left <b>{where}</b>", ("lf", ev.chat.id),
             lambda n, where=where: f"🚪 ×{n} left <b>{where}</b>")
    elif not await _got_message(ev.chat.id, uid):  # joined and no message was/will be sent
        mlog(f"➕ Joined, no message sent · {who} → <b>{where}</b>", kind="joined")


async def touch_post(chat_id: int):
    now = time.time()
    if chat_id == _mlog_chat:  # the master-log chat is not a destination's activity
        return
    if now - _last_post_touch.get(chat_id, 0) < 60:  # at most one write a minute per chat
        return
    _last_post_touch[chat_id] = now
    await db_execute("UPDATE destinations SET last_post_at=? WHERE chat_id=?", now, chat_id)


@router.channel_post()
async def on_channel_post(message: Message):
    await touch_post(message.chat.id)


# ───────────────────────── 🔗 Links (ask a link, status) ─────────────────────────
def link_windows():
    now = datetime.now(TZ)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = (now + timedelta(days=1)).timestamp()
    return [
        ("📅 Today", today.timestamp(), end),
        ("🕘 Yesterday", (today - timedelta(days=1)).timestamp(), today.timestamp()),
        ("🗓 Last 7 days", (now - timedelta(days=7)).timestamp(), end),
        ("📆 Last 30 days", (now - timedelta(days=30)).timestamp(), end),
    ]


async def link_counts(link_id: int, t0: float = 0.0, t1: float = 4e9) -> dict:
    out = {"request": 0, "approve": 0, "join": 0}
    for r in await db_fetchall(
        "SELECT kind, COUNT(*) AS n FROM link_events WHERE link_id=? AND ts>=? AND ts<? GROUP BY kind",
        link_id, t0, t1,
    ):
        out[r["kind"]] = int(r["n"])
    return out


async def link_bot_users(link_id: int) -> int:
    row = await db_fetchone(
        """SELECT COUNT(DISTINCT le.user_id) AS n FROM link_events le
           JOIN users u ON u.user_id = le.user_id
           WHERE le.link_id=? AND le.kind='request' AND u.started=1""",
        link_id,
    )
    return int(row["n"] or 0) if row else 0


async def lk_list_view(cid: int) -> tuple[str, InlineKeyboardMarkup] | None:
    d = await dest_get(cid)
    if not d:
        return None
    links = await db_fetchall(
        "SELECT * FROM links WHERE chat_id=? ORDER BY revoked, id DESC LIMIT 8", cid
    )
    parts = [f"📈 <b>Links — {escape(d['title'] or str(cid))}</b>\n"]
    rows = []
    if not links:
        parts.append("No link created by the bot yet. Tap <b>🔗 Ask a link</b> to make one.")
    for l in links:
        c = await link_counts(l["id"])
        bots = await link_bot_users(l["id"])
        t0, t1 = link_windows()[0][1:]
        today = await link_counts(l["id"], t0, t1)
        parts.append(
            f"{'🚫' if l['revoked'] else '🔗'} <b>{escape(l['name'])}</b>\n"
            f"   ⏳ requested <b>{c['request']}</b> · ✅ approved <b>{c['approve']}</b> · "
            f"➕ joined <b>{c['join']}</b> · 🤖 bot <b>{bots}</b> · today <b>{today['request']}</b>"
        )
        rows.append([btn(f"{'🚫' if l['revoked'] else '🔗'} {l['name'][:30]}", f"lk:view:{l['id']}")])
    parts.append("\n<i>Counts start from the moment this version began tracking links.</i>")
    rows.append([btn("🔗 Ask a link", f"lk:new:{cid}")])
    rows.append([btn("🔄 Refresh", f"lk:list:{cid}"), btn("⬅️ Back", f"dst:open:{cid}")])
    return "\n".join(parts), InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("lk:list:"), IsAdmin())
async def cb_lk_list(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    view = await lk_list_view(int(cb.data.split(":")[2]))
    if not view:
        await cb.answer("Destination not found.", show_alert=True)
        return
    await edit(cb, *view)
    await cb.answer()


async def lk_view(link_id: int) -> tuple[str, InlineKeyboardMarkup] | None:
    l = await db_fetchone("SELECT * FROM links WHERE id=?", link_id)
    if not l:
        return None
    c = await link_counts(link_id)
    bots = await link_bot_users(link_id)
    lines = []
    for label, t0, t1 in link_windows():
        w = await link_counts(link_id, t0, t1)
        lines.append(f"{label}: ⏳ <b>{w['request']}</b> requested · ➕ <b>{w['join']}</b> joined")
    text = (
        f"{'🚫' if l['revoked'] else '🔗'} <b>{escape(l['name'])}</b>"
        f"{'  <i>(revoked)</i>' if l['revoked'] else ''}\n"
        f"<code>{escape(l['invite_link'])}</code>\n"
        f"Created: {fmt_ts(l['created_at'])}\n\n"
        f"⏳ Requested: <b>{c['request']}</b>\n"
        f"✅ Approved: <b>{c['approve']}</b>\n"
        f"➕ Total joined: <b>{c['join']}</b>\n"
        f"🤖 Started the bot: <b>{bots}</b>\n\n"
        + "\n".join(lines)
    )
    rows = []
    if not l["revoked"]:
        rows.append([btn("🚫 Revoke link", f"lk:revoke:{link_id}")])
    rows.append([btn("🔄 Refresh", f"lk:view:{link_id}"), btn("⬅️ Back", f"lk:list:{l['chat_id']}")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("lk:view:"), IsAdmin())
async def cb_lk_view(cb: CallbackQuery):
    view = await lk_view(int(cb.data.split(":")[2]))
    if not view:
        await cb.answer("Link not found.", show_alert=True)
        return
    await edit(cb, *view)
    await cb.answer()


@router.callback_query(F.data.startswith("lk:revoke:"), IsAdmin())
async def cb_lk_revoke(cb: CallbackQuery, bot: Bot):
    lid = int(cb.data.split(":")[2])
    l = await db_fetchone("SELECT * FROM links WHERE id=?", lid)
    if not l:
        await cb.answer("Link not found.", show_alert=True)
        return
    try:
        await bot.revoke_chat_invite_link(l["chat_id"], l["invite_link"])
    except Exception as e:
        await cb.answer(f"Could not revoke: {str(e)[:150]}", show_alert=True)
        return
    await db_execute("UPDATE links SET revoked=1 WHERE id=?", lid)
    mlog(f"🚫 Link revoked · <b>{escape(l['name'])}</b>")
    await edit(cb, *(await lk_view(lid)))
    await cb.answer("Link revoked")


@router.callback_query(F.data.startswith("lk:new:"), IsAdmin())
async def cb_lk_new(cb: CallbackQuery, state: FSMContext):
    cid = int(cb.data.split(":")[2])
    if not await dest_get(cid):
        await cb.answer("Destination not found.", show_alert=True)
        return
    await state.set_state(Form.link_name)
    await state.update_data(cid=cid)
    await cb.message.answer(
        "🔗 What should I <b>name</b> this link? (max 32 characters)\n"
        "Example: <code>Instagram bio</code> or <code>YouTube video 12</code>\n\n"
        "The bot will create a private link that needs your approval to join. /cancel to abort."
    )
    await cb.answer()


@router.message(Form.link_name, IsAdmin())
async def msg_link_name(message: Message, state: FSMContext, bot: Bot):
    name = (message.text or "").strip()
    if not name or len(name) > 32:
        await message.answer("❌ Send a name of 1–32 characters (or /cancel).")
        return
    cid = int((await state.get_data())["cid"])
    try:
        inv = await bot.create_chat_invite_link(cid, name=name, creates_join_request=True)
    except Exception as e:
        await state.clear()
        await message.answer(
            f"❌ Could not create the link: <code>{escape(str(e)[:200])}</code>\n"
            "Make sure the bot is an admin there with the <b>Invite Users via Link</b> permission."
        )
        return
    await db_execute(
        "INSERT INTO links (chat_id, name, invite_link, created_at, revoked) VALUES (?, ?, ?, ?, 0)",
        cid, name, inv.invite_link, time.time(),
    )
    lid = await link_id_by_url(cid, inv.invite_link)
    _d = await dest_get(cid)
    mlog(f"🔗 Link created · <b>{escape(name)}</b> · {escape((_d or {}).get('title') or str(cid))}")
    await state.clear()
    await message.answer(
        f"✅ Link <b>{escape(name)}</b> created (members need your approval):\n\n"
        f"<code>{escape(inv.invite_link)}</code>\n\n"
        "Share this link — people who use it are counted under this name.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [btn("📈 Link stats", f"lk:view:{lid}")],
            [btn("📋 All links", f"lk:list:{cid}"), btn("⬅️ Back", f"dst:open:{cid}")],
        ]),
        disable_web_page_preview=True,
    )


# ───────────────────────── 💬 Massages (pre-request · post-request · broadcast) ─────────────────────────
def _kind(data: str, i: int) -> str:
    p = data.split(":")
    return p[i] if len(p) > i and p[i] in GLOBAL_KEYS else "pre"


async def ms_menu_view() -> tuple[str, InlineKeyboardMarkup]:
    allw = await wl_all()
    lines = []
    for kind, g in GLOBAL_KEYS.items():
        on = await get_setting(g["on"]) == "1"
        wid = int(await get_setting(g["msg"]) or 0)
        w = await wl_get(wid) if wid else None
        lines.append(
            f"{g['icon']} <b>{g['name']}</b>: {'✅ ON' if on else '❌ OFF'} · "
            f"{escape(w['name']) if w else 'no message'} · ⏱ {delay_txt(float(await get_setting(g['delay']) or 0))}"
            f" · ⏳ gap {gap_txt(await get_setting(g['gmin']), await get_setting(g['gmax']))}"
        )
    text = (
        "💬 <b>Massages</b>\n\n" + "\n".join(lines) +
        f"\n📚 Library: <b>{len(allw)}</b> message(s)\n\n"
        "📥 <b>Pre-request</b> – sent when someone asks to join.\n"
        "📤 <b>Post-request</b> – sent when they actually join (with an Unsubscribe button).\n"
        "📣 <b>Broadcast</b> – send a message to your users whenever you want."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📥 Pre-request message", "gm:menu:pre"), btn("📤 Post-request message", "gm:menu:post")],
        [btn("📣 Broadcast", "ms:bcpick"), btn("📚 Message library", "ms:lib")],
        [btn("⬅️ Back", "refresh")],
    ])
    return text, kb


@router.callback_query(F.data == "ms:menu", IsAdmin())
async def cb_ms_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await edit(cb, *await ms_menu_view())
    await cb.answer()


@router.callback_query(F.data == "ms:bcpick", IsAdmin())
async def cb_ms_bcpick(cb: CallbackQuery):
    allw = await wl_all()
    rows = [[btn(f"📨 {w['name'][:40]}", f"ms:bc:{w['id']}")] for w in allw[:50]]
    rows.append([btn("⬅️ Back", "ms:menu")])
    await edit(
        cb,
        "📣 <b>Broadcast</b>\n\nPick the message to send. Next you choose who receives it: "
        "subscribed members, unsubscribed members, or both."
        + ("" if allw else "\n\nYou have no message yet – create one in 📚 Message library first."),
        InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await cb.answer()


async def msg_stats(kind: str, chat_id: int | None = None) -> tuple[dict, dict]:
    tot = {"sent": 0, "blocked": 0, "failed": 0}
    day = dict(tot)
    cond, args = ("kind=?", [kind]) if chat_id is None else ("kind=? AND chat_id=?", [kind, chat_id])
    for r in await db_fetchall(f"SELECT status, COUNT(*) AS n FROM msg_log WHERE {cond} GROUP BY status", *args):
        tot[r["status"]] = int(r["n"])
    for r in await db_fetchall(
        f"SELECT status, COUNT(*) AS n FROM msg_log WHERE {cond} AND ts>=? GROUP BY status",
        *args, link_windows()[0][1],
    ):
        day[r["status"]] = int(r["n"])
    return day, tot


KIND_HELP = {
    "pre": ("<i>Sent to a person right after they ask to join – before you approve them. Telegram lets the "
            "bot message a requester for about 5 minutes only, so the timer is capped at 240s.</i>"),
    "post": ("<i>Sent when the person has actually joined (after approval), with a 🔕 Unsubscribe button under it. "
             "The timer is the wait after they join (up to 24 h). Telegram only lets the bot message people who started the bot "
             "or whose request is still open, so some deliveries can show as blocked.</i>"),
}


def _delivery_lines(day: dict, tot: dict) -> str:
    return (
        "<b>📬 Delivery status</b>\n"
        f"Today: ✅ <b>{day['sent']}</b> sent · 🚫 <b>{day['blocked']}</b> blocked · ⚠️ <b>{day['failed']}</b> failed\n"
        f"All time: ✅ <b>{tot['sent']}</b> · 🚫 <b>{tot['blocked']}</b> · ⚠️ <b>{tot['failed']}</b>"
    )


# ---- global screen for one kind ----
async def gm_view(kind: str) -> tuple[str, InlineKeyboardMarkup]:
    g = GLOBAL_KEYS[kind]
    on = await get_setting(g["on"]) == "1"
    wid = int(await get_setting(g["msg"]) or 0)
    w = await wl_get(wid) if wid else None
    delay = float(await get_setting(g["delay"]) or 0)
    gap = gap_txt(await get_setting(g["gmin"]), await get_setting(g["gmax"]))
    day, tot = await msg_stats(kind)
    text = (
        f"{g['icon']} <b>{g['name']}</b>\n{KIND_HELP[kind]}\n\n"
        f"Status: {'✅ <b>ON</b>' if on else '❌ <b>OFF</b>'}\n"
        f"📨 Message: <b>{escape(w['name']) if w else 'none chosen'}</b>\n"
        f"⏱ Timer: <b>{delay_txt(delay)}</b> <i>(default for all destinations)</i>\n"
        f"⏳ Time gap: <b>{gap}</b> <i>(wait between one message and the next)</i>\n\n"
        + _delivery_lines(day, tot)
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📨 Choose message", f"gm:pick:{kind}"), btn("⏱ Set timer (all)", f"gm:delay:{kind}")],
        [btn("⏳ Set time gap (all)", f"gm:gap:{kind}")],
        [btn("🔕 Turn OFF" if on else "🔔 Turn ON", f"gm:tog:{kind}")],
        [btn("📡 Per destination", f"gm:dests:{kind}")],
        [btn("🔄 Refresh", f"gm:menu:{kind}"), btn("⬅️ Back", "ms:menu")],
    ])
    return text, kb


@router.callback_query(F.data.startswith("gm:menu:"), IsAdmin())
async def cb_gm_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await edit(cb, *await gm_view(_kind(cb.data, 2)))
    await cb.answer()


@router.callback_query(F.data.startswith("gm:tog:"), IsAdmin())
async def cb_gm_tog(cb: CallbackQuery):
    kind = _kind(cb.data, 2)
    key = GLOBAL_KEYS[kind]["on"]
    on = await get_setting(key) == "1"
    if not on and not int(await get_setting(GLOBAL_KEYS[kind]["msg"]) or 0):
        await cb.answer("Choose a message first.", show_alert=True)
        return
    await set_setting(key, "0" if on else "1")
    await edit(cb, *await gm_view(kind))
    await cb.answer("Turned OFF" if on else "Turned ON")


@router.callback_query(F.data.startswith("gm:pick:"), IsAdmin())
async def cb_gm_pick(cb: CallbackQuery):
    kind = _kind(cb.data, 2)
    cur = int(await get_setting(GLOBAL_KEYS[kind]["msg"]) or 0)
    rows = [
        [btn(("✅ " if cur == w["id"] else "") + f"📨 {w['name'][:36]}", f"gm:set:{kind}:{w['id']}")]
        for w in (await wl_all())[:50]
    ]
    rows.append([btn("⬅️ Back", f"gm:menu:{kind}")])
    await edit(
        cb,
        f"📨 <b>Choose the {GLOBAL_KEYS[kind]['name'].lower()}</b>\n"
        "Messages come from 📚 Message library (create or edit them there)."
        + ("" if rows[:-1] else "\n\nNo message yet – create one in the library first."),
        InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("gm:set:"), IsAdmin())
async def cb_gm_set(cb: CallbackQuery):
    p = cb.data.split(":")
    kind, wid = _kind(cb.data, 2), int(p[3])
    if not await wl_get(wid):
        await cb.answer("That message no longer exists.", show_alert=True)
        return
    await set_setting(GLOBAL_KEYS[kind]["msg"], str(wid))
    await edit(cb, *await gm_view(kind))
    await cb.answer("Message set")


@router.callback_query(F.data.startswith("gm:delay:"), IsAdmin())
async def cb_gm_delay(cb: CallbackQuery, state: FSMContext):
    kind = _kind(cb.data, 2)
    await state.set_state(Form.gm_delay)
    await state.update_data(kind=kind)
    await cb.message.answer(
        f"⏱ Send the timer in <b>seconds</b> for the {GLOBAL_KEYS[kind]['name'].lower()} "
        f"(<code>0</code> = instantly, max {MAX_MSG_DELAY[kind]}). It is applied to <b>every</b> destination.\n\n"
        "/cancel to abort."
    )
    await cb.answer()


@router.message(Form.gm_delay, IsAdmin())
async def msg_gm_delay(message: Message, state: FSMContext):
    kind = (await state.get_data()).get("kind", "pre")
    try:
        value = float((message.text or "").strip().replace(",", "."))
        if not 0 <= value <= MAX_MSG_DELAY[kind]:
            raise ValueError
    except ValueError:
        await message.answer(f"❌ Send a number between 0 and {MAX_MSG_DELAY[kind]} (or /cancel).")
        return
    await set_setting(GLOBAL_KEYS[kind]["delay"], str(value))
    await db_execute("UPDATE dest_msgs SET delay=? WHERE kind=?", value, kind)
    await state.clear()
    await message.answer(f"✅ Timer set to <b>{delay_txt(value)}</b> for all destinations.")
    text, kb = await gm_view(kind)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("gm:dests:"), IsAdmin())
async def cb_gm_dests(cb: CallbackQuery):
    kind = _kind(cb.data, 2)
    rows = [
        [btn(f"📡 {(d['title'] or str(d['chat_id']))[:34]}", f"dm:menu:{d['chat_id']}:{kind}")]
        for d in (await dest_all())[:40]
    ]
    rows.append([btn("⬅️ Back", f"gm:menu:{kind}")])
    await edit(
        cb,
        f"📡 <b>{GLOBAL_KEYS[kind]['name']} — per destination</b>\n"
        "Pick a destination to give it its own message, timer or on/off switch.",
        InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await cb.answer()


# ---- time gap ----
GAP_HELP = (
    "⏳ Send the <b>time gap</b> in seconds. After one message is sent, the next one waits this long.\n"
    "• Random range: <code>5-20</code>\n• Fixed: <code>10</code>\n• No gap: <code>0</code>\n"
    "Maximum 3600."
)
PRE_GAP_NOTE = (
    "\n\n<i>Pre-request note: timer + waiting in the gap queue must stay under about 4.5 minutes, because "
    "Telegram stops letting the bot message a requester after about 5 minutes. Messages that would be "
    "later are marked failed.</i>"
)


@router.callback_query(F.data.startswith("gm:gap:"), IsAdmin())
async def cb_gm_gap(cb: CallbackQuery, state: FSMContext):
    kind = _kind(cb.data, 2)
    await state.set_state(Form.gm_gap)
    await state.update_data(kind=kind)
    g = GLOBAL_KEYS[kind]
    await cb.message.answer(
        f"{g['icon']} <b>{g['name']} — time gap (all destinations)</b>\n{GAP_HELP}"
        + (PRE_GAP_NOTE if kind == "pre" else "") + "\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.gm_gap, IsAdmin())
async def msg_gm_gap(message: Message, state: FSMContext):
    kind = (await state.get_data()).get("kind", "pre")
    r = parse_gap(message.text or "")
    if not r:
        await message.answer("❌ Send <code>min-max</code> (like <code>5-20</code>), one number, or <code>0</code> (or /cancel).")
        return
    g = GLOBAL_KEYS[kind]
    await set_setting(g["gmin"], str(r[0]))
    await set_setting(g["gmax"], str(r[1]))
    await db_execute("UPDATE dest_msgs SET gap_min=?, gap_max=? WHERE kind=?", r[0], r[1], kind)
    await state.clear()
    await message.answer(f"✅ Time gap set to <b>{gap_txt(*r)}</b> for all destinations.")
    text, kb = await gm_view(kind)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("dm:gap:"), IsAdmin())
async def cb_dm_gap(cb: CallbackQuery, state: FSMContext):
    cid, kind = _cid_kind(cb.data)
    await state.set_state(Form.msg_gap)
    await state.update_data(cid=cid, kind=kind)
    await cb.message.answer(
        f"{GLOBAL_KEYS[kind]['icon']} <b>{GLOBAL_KEYS[kind]['name']} — time gap (this destination)</b>\n{GAP_HELP}"
        + (PRE_GAP_NOTE if kind == "pre" else "") + "\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.msg_gap, IsAdmin())
async def msg_dm_gap(message: Message, state: FSMContext):
    data = await state.get_data()
    cid, kind = int(data["cid"]), data.get("kind", "pre")
    r = parse_gap(message.text or "")
    if not r:
        await message.answer("❌ Send <code>min-max</code> (like <code>5-20</code>), one number, or <code>0</code> (or /cancel).")
        return
    await dm_set(cid, kind, gap_min=r[0], gap_max=r[1])
    await state.clear()
    await message.answer(f"✅ Time gap set to <b>{gap_txt(*r)}</b>.")
    view = await dm_view(cid, kind)
    if view:
        await message.answer(view[0], reply_markup=view[1])


# ---- per-destination screens ----
@router.callback_query(F.data.startswith("dmx:"), IsAdmin())
async def cb_dmx(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    cid = int(cb.data.split(":")[1])
    d = await dest_get(cid)
    if not d:
        await cb.answer("Destination not found.", show_alert=True)
        return
    await edit(
        cb,
        f"💬 <b>Massages — {escape(d['title'] or str(cid))}</b>\nChoose which message to manage for this destination.",
        InlineKeyboardMarkup(inline_keyboard=[
            [btn("📥 Pre-request message", f"dm:menu:{cid}:pre"), btn("📤 Post-request message", f"dm:menu:{cid}:post")],
            [btn("⬅️ Back", f"dst:open:{cid}")],
        ]),
    )
    await cb.answer()


async def dm_view(cid: int, kind: str) -> tuple[str, InlineKeyboardMarkup] | None:
    d = await dest_get(cid)
    if not d:
        return None
    g = GLOBAL_KEYS[kind]
    dm = await dm_get(cid, kind)
    chosen = await wl_get(int(dm["welcome_id"])) if dm["welcome_id"] else None
    gid = int(await get_setting(g["msg"]) or 0)
    default = await wl_get(gid) if gid else None
    msg_line = (
        f"📨 Message: <b>{escape(chosen['name'])}</b>" if chosen
        else f"📨 Message: <b>⭐ default</b> ({escape(default['name']) if default else 'none set'})"
    )
    day, tot = await msg_stats(kind, cid)
    global_on = await get_setting(g["on"]) == "1"
    text = (
        f"{g['icon']} <b>{g['name']} — {escape(d['title'] or str(cid))}</b>\n{KIND_HELP[kind]}\n\n"
        f"Here: {'✅ ON' if dm['enabled'] else '❌ OFF'}"
        f"{'' if global_on else f'  (⚠️ switched OFF globally in 💬 Massages → {g['name']})'}\n"
        f"{msg_line}\n"
        f"⏱ Timer: <b>{delay_txt(float(dm['delay'] or 0))}</b>\n"
        f"⏳ Time gap: <b>{gap_txt(dm['gap_min'], dm['gap_max'])}</b>\n\n"
        + _delivery_lines(day, tot)
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📨 Choose message", f"dm:pick:{cid}:{kind}"), btn("⏱ Set timer", f"dm:delay:{cid}:{kind}")],
        [btn("⏳ Time gap", f"dm:gap:{cid}:{kind}")],
        [btn("🔕 Turn OFF here" if dm["enabled"] else "🔔 Turn ON here", f"dm:tog:{cid}:{kind}")],
        [btn("🔄 Refresh", f"dm:menu:{cid}:{kind}"), btn("⬅️ Back", f"dmx:{cid}")],
    ])
    return text, kb


async def show_dm(cb: CallbackQuery, cid: int, kind: str) -> bool:
    view = await dm_view(cid, kind)
    if not view:
        await cb.answer("Destination not found.", show_alert=True)
        return False
    await edit(cb, *view)
    return True


def _cid_kind(data: str) -> tuple[int, str]:
    return int(data.split(":")[2]), _kind(data, 3)


@router.callback_query(F.data.startswith("dm:menu:"), IsAdmin())
async def cb_dm_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    cid, kind = _cid_kind(cb.data)
    if await show_dm(cb, cid, kind):
        await cb.answer()


@router.callback_query(F.data.startswith("dm:tog:"), IsAdmin())
async def cb_dm_tog(cb: CallbackQuery):
    cid, kind = _cid_kind(cb.data)
    dm = await dm_get(cid, kind)
    await dm_set(cid, kind, enabled=0 if dm["enabled"] else 1)
    await show_dm(cb, cid, kind)
    await cb.answer("Updated")


@router.callback_query(F.data.startswith("dm:pick:"), IsAdmin())
async def cb_dm_pick(cb: CallbackQuery):
    cid, kind = _cid_kind(cb.data)
    cur = int((await dm_get(cid, kind))["welcome_id"] or 0)
    rows = [[btn(("✅ " if not cur else "") + "⭐ Use the default message", f"dm:set:{cid}:{kind}:0")]]
    for w in (await wl_all())[:20]:
        rows.append([btn(("✅ " if cur == w["id"] else "") + f"📨 {w['name'][:30]}", f"dm:set:{cid}:{kind}:{w['id']}")])
    rows.append([btn("⬅️ Back", f"dm:menu:{cid}:{kind}")])
    await edit(
        cb,
        f"📨 <b>Choose the {GLOBAL_KEYS[kind]['name'].lower()} for this destination</b>\n"
        "Messages come from 📚 Message library.",
        InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("dm:set:"), IsAdmin())
async def cb_dm_set(cb: CallbackQuery):
    cid, kind = _cid_kind(cb.data)
    wid = int(cb.data.split(":")[4])
    if wid and not await wl_get(wid):
        await cb.answer("That message no longer exists.", show_alert=True)
        return
    await dm_set(cid, kind, welcome_id=wid)
    await show_dm(cb, cid, kind)
    await cb.answer("Message set")


@router.callback_query(F.data.startswith("dm:delay:"), IsAdmin())
async def cb_dm_delay(cb: CallbackQuery, state: FSMContext):
    cid, kind = _cid_kind(cb.data)
    await state.set_state(Form.msg_delay)
    await state.update_data(cid=cid, kind=kind)
    await cb.message.answer(
        f"⏱ Send the delay in <b>seconds</b> for the {GLOBAL_KEYS[kind]['name'].lower()} "
        f"(<code>0</code> = instantly, max {MAX_MSG_DELAY[kind]}).\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.msg_delay, IsAdmin())
async def msg_dm_delay(message: Message, state: FSMContext):
    data = await state.get_data()
    cid, kind = int(data["cid"]), data.get("kind", "pre")
    try:
        value = float((message.text or "").strip().replace(",", "."))
        if not 0 <= value <= MAX_MSG_DELAY[kind]:
            raise ValueError
    except ValueError:
        await message.answer(f"❌ Send a number between 0 and {MAX_MSG_DELAY[kind]} (or /cancel).")
        return
    await dm_set(cid, kind, delay=value)
    await state.clear()
    await message.answer(f"✅ Timer set to <b>{delay_txt(value)}</b>.")
    view = await dm_view(cid, kind)
    if view:
        await message.answer(view[0], reply_markup=view[1])


# ───────────────────────── 🧾 Master log (admin panel) ─────────────────────────
async def ml_view() -> tuple[str, InlineKeyboardMarkup]:
    on = await get_setting("master_log_on") == "1"
    chat = await get_setting("master_log_chat")
    title = await get_setting("master_log_title")
    err = await get_setting("master_log_err")
    at = float(await get_setting("master_log_at") or 0)
    text = (
        "🧾 <b>Master log</b>\n"
        "<i>Every action of the bot is posted to one chat. Each new update is sent as a "
        "fresh message and the previous one is deleted, so only the latest stays.</i>\n\n"
        f"Status: {'🟢 <b>ON</b>' if on else '🔴 <b>OFF</b>'}\n"
        f"Destination: <b>{escape(title) if title else 'not set'}</b>"
        f"{f' (<code>{chat}</code>)' if chat else ''}\n"
        f"Last update: {fmt_ts(at) if at else '-'}\n"
        f"Lines kept: {len(_mlog_lines)}/{MLOG_LINES}"
        + (f"\n\n⚠️ <b>Last error:</b> <code>{escape(err)}</code>\n"
           "Add the bot to that chat (as admin for a channel) and try again." if err else "")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn("📍 Set destination", "ml:pick")],
        [btn("🔴 Turn OFF" if on else "🟢 Turn ON", "ml:tog"), btn("📤 Update now", "ml:now")],
        [btn("🧹 Clear log", "ml:clear")],
        [btn("⬅️ Back", "refresh")],
    ])
    return text, kb


@router.callback_query(F.data == "ml:menu", IsAdmin())
async def cb_ml_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await edit(cb, *await ml_view())
    await cb.answer()


@router.callback_query(F.data == "ml:pick", IsAdmin())
async def cb_ml_pick(cb: CallbackQuery):
    rows = [
        [btn(f"📡 {(d['title'] or str(d['chat_id']))[:34]}", f"ml:set:{d['chat_id']}")]
        for d in (await dest_all())[:30]
    ]
    rows.append([btn("✏️ Enter chat ID / @username", "ml:enter")])
    rows.append([btn("⬅️ Back", "ml:menu")])
    await edit(
        cb,
        "📍 <b>Where should the master log go?</b>\n\n"
        "Pick one of your destinations, or enter any other chat where the bot is a member "
        "(a channel needs the bot as admin). A separate private channel/group just for logs "
        "works best — you can also enter your own user ID to get the log in this bot's chat.",
        InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await cb.answer()


async def set_master_chat(bot: Bot, cid: int, title: str) -> None:
    global _mlog_chat
    old_chat = await get_setting("master_log_chat")
    if old_chat and old_chat != str(cid):
        await delete_logged(bot, await get_setting("master_log_msg"))
        await set_setting("master_log_msg", "")
    await set_setting("master_log_chat", str(cid))
    await set_setting("master_log_title", title)
    await set_setting("master_log_on", "1")
    _mlog_chat = cid
    mlog(f"📍 Master log connected · <b>{escape(title)}</b>")
    await push_master_log(bot, cid)
    _mlog_event.clear()


@router.callback_query(F.data.startswith("ml:set:"), IsAdmin())
async def cb_ml_set(cb: CallbackQuery, bot: Bot):
    cid = int(cb.data.split(":")[2])
    d = await dest_get(cid)
    await set_master_chat(bot, cid, (d or {}).get("title") or str(cid))
    await edit(cb, *await ml_view())
    await cb.answer("Master log set")


@router.callback_query(F.data == "ml:enter", IsAdmin())
async def cb_ml_enter(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Form.ml_chat)
    await cb.message.answer(
        "✏️ Send the <b>chat ID</b> (like <code>-1001234567890</code>) or the <b>@username</b> "
        "of the chat that should receive the master log.\n\n/cancel to abort."
    )
    await cb.answer()


@router.message(Form.ml_chat, IsAdmin())
async def msg_ml_chat(message: Message, state: FSMContext, bot: Bot):
    raw = (message.text or "").strip()
    target = int(raw) if raw.lstrip("-").isdigit() else raw
    try:
        chat = await bot.get_chat(target)
    except Exception as e:
        await message.answer(
            f"❌ I can't see that chat: <code>{escape(str(e)[:150])}</code>\n"
            "Add the bot there first, then send the ID again (or /cancel)."
        )
        return
    await state.clear()
    title = chat.title or chat.full_name or str(chat.id)
    await set_master_chat(bot, chat.id, title)
    text, kb = await ml_view()
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "ml:tog", IsAdmin())
async def cb_ml_tog(cb: CallbackQuery):
    if not await get_setting("master_log_chat"):
        await cb.answer("Set a destination first.", show_alert=True)
        return
    on = await get_setting("master_log_on") == "1"
    await set_setting("master_log_on", "0" if on else "1")
    if not on:
        mlog("🟢 Master log ON")
    await edit(cb, *await ml_view())
    await cb.answer("Master log OFF" if on else "Master log ON")


@router.callback_query(F.data == "ml:now", IsAdmin())
async def cb_ml_now(cb: CallbackQuery, bot: Bot):
    chat = await get_setting("master_log_chat")
    if not chat or await get_setting("master_log_on") != "1":
        await cb.answer("Turn the master log ON and set a destination first.", show_alert=True)
        return
    await push_master_log(bot, int(chat))
    _mlog_event.clear()
    await edit(cb, *await ml_view())
    await cb.answer("Updated")


@router.callback_query(F.data == "ml:clear", IsAdmin())
async def cb_ml_clear(cb: CallbackQuery):
    _mlog_lines.clear()
    mlog("🧹 Log cleared")
    await edit(cb, *await ml_view())
    await cb.answer("Log cleared")


# ───────────────────────── Bot-user detection ─────────────────────────
class TrackIncoming(BaseMiddleware):
    """Remember ids of messages users send us, so their chat can be wiped on unsubscribe."""

    async def __call__(self, handler, event, data):
        try:
            if (event.chat.type == "private" and event.from_user and not event.from_user.is_bot
                    and event.from_user.id not in ADMIN_IDS):
                await track_msg(event.chat.id, event.message_id)
        except Exception:
            log.exception("TrackIncoming failed")
        return await handler(event, data)


router.message.outer_middleware(TrackIncoming())


@router.my_chat_member(F.chat.type == "private")
async def on_my_chat_member(ev: ChatMemberUpdated):
    """User started / blocked / unblocked the bot."""
    status = ev.new_chat_member.status
    if status == "kicked":
        await set_started(ev.from_user.id, False)
        await db_execute("UPDATE users SET blocked=1 WHERE user_id=?", ev.from_user.id)
        _mlog_event.set()
    elif status == "member":
        await touch_user(ev.from_user, True)


@router.message(F.chat.type == "private")
async def track_private(message: Message):
    """Any private message to the bot marks that user as a bot user (must stay last)."""
    if message.from_user and not message.from_user.is_bot:
        await touch_user(message.from_user, True)


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(message: Message):
    """Any group message = activity (the bot sees all messages when it is an admin)."""
    await touch_post(message.chat.id)
