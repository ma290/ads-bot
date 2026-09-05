import os
import sys
import asyncio
import logging
import random
from dotenv import load_dotenv

import asyncpg
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    UserNotParticipantError,
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    MessageNotModifiedError,
)
from telethon.tl.functions.channels import GetParticipantRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.types import Chat, Channel, PeerChannel, PeerChat, PeerUser

# ---------------------------------------------------------------------------
# Environment & Config
# ---------------------------------------------------------------------------

load_dotenv()

API_ID       = int(os.getenv("API_ID", "0"))
API_HASH     = os.getenv("API_HASH", "")
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
LOGGER_BOT_TOKEN    = os.getenv("LOGGER_BOT_TOKEN", "")
LOGGER_BOT_USERNAME = os.getenv("LOGGER_BOT_USERNAME", "").lstrip("@")
# Channel username for "About bot" button (e.g. MyAdsChannel or @MyAdsChannel)
ABOUT_CHANNEL = os.getenv("ABOUT_CHANNEL", "").lstrip("@")
SESSION_CHANNEL = os.getenv("SESSION_CHANNEL", "").strip()
FORCE_SUB_CHANNELS = [x.strip() for x in os.getenv("FORCE_SUB_CHANNELS", "").split(",") if x.strip()]
ADMIN_USER_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    if x.strip().isdigit()
}
DATABASE_URL = os.getenv("DATABASE_URL", "")
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "3"))
MIN_DELAY    = int(os.getenv("MIN_DELAY", "15"))
MAX_DELAY    = int(os.getenv("MAX_DELAY", "40"))

# Free vs Premium limits
FREE_MAX_ACCOUNTS    = int(os.getenv("FREE_MAX_ACCOUNTS", "3"))
PREMIUM_MAX_ACCOUNTS = int(os.getenv("PREMIUM_MAX_ACCOUNTS", "20"))
FREE_MAX_CYCLES      = int(os.getenv("FREE_MAX_CYCLES", "20"))
PREMIUM_MAX_CYCLES   = int(os.getenv("PREMIUM_MAX_CYCLES", "100"))
# Auto-reply: free users capped per ad run; premium = unlimited
FREE_MAX_AI_REPLIES  = int(os.getenv("FREE_MAX_AI_REPLIES", "20"))

DATA_DIR = os.path.abspath("data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.join(DATA_DIR, "app.log")

# Welcome / landing banner (Tecxo-style photo + Start Bot card)
_ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
_banner_env = os.getenv("WELCOME_BANNER", "").strip()
WELCOME_BANNER = os.path.abspath(
    _banner_env if _banner_env else os.path.join(_ASSETS_DIR, "welcome_banner.png")
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("AdsBot")

# ---------------------------------------------------------------------------
# Global State
# ---------------------------------------------------------------------------

db_pool: asyncpg.Pool = None
_logger_home_tasks: dict = {}
bot_client: TelegramClient = None
logger_client: TelegramClient = None  # separate Logger Bot

# Per-user running tasks  {user_id: asyncio.Task}
active_tasks: dict = {}
stop_events: dict = {}   # {user_id: asyncio.Event}

user_states: dict = {}   # conversational state machine

# Live logger message ids  {user_id: {phone: message_id}}
logger_msg_ids: dict = {}
# Logger home (/start menu) message ids for auto-refresh  {user_id: message_id}
logger_home_msg_ids: dict = {}
# Latest per-phone live stats for auto-refresh home  {user_id: {phone: dict}}
live_stats: dict = {}
# Throttle home auto-edits  {user_id: monotonic_ts}
_logger_home_last_edit: dict = {}
LOGGER_HOME_EDIT_INTERVAL = 2.5  # seconds

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                session_name TEXT PRIMARY KEY,
                phone        TEXT,
                owner_id     BIGINT NOT NULL,
                is_active    BOOLEAN NOT NULL DEFAULT TRUE,
                session_msg_id BIGINT,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id        BIGINT PRIMARY KEY,
                ad_message     TEXT,
                cycle_interval INTEGER NOT NULL DEFAULT 180,
                target_type    TEXT    NOT NULL DEFAULT 'groups',
                max_cycles     INTEGER NOT NULL DEFAULT 20,
                current_cycle  INTEGER NOT NULL DEFAULT 0,
                status         TEXT    NOT NULL DEFAULT 'paused',
                ai_reply       BOOLEAN NOT NULL DEFAULT FALSE,
                ai_reply_message TEXT,
                is_premium     BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS targets (
                id         SERIAL PRIMARY KEY,
                chat_id    TEXT   NOT NULL,
                title      TEXT,
                owner_id   BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (owner_id, chat_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS excluded_groups (
                id       SERIAL PRIMARY KEY,
                chat_id  TEXT   NOT NULL,
                title    TEXT,
                owner_id BIGINT NOT NULL,
                UNIQUE (owner_id, chat_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ad_logs (
                id            SERIAL PRIMARY KEY,
                owner_id      BIGINT NOT NULL,
                chat_id       TEXT,
                status        TEXT,
                error_message TEXT,
                sent_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # ---- Migrations for pre-existing tables ----
        for tbl in ("accounts", "targets"):
            await conn.execute(f"""
                DO $$ BEGIN
                    ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS owner_id BIGINT;
                EXCEPTION WHEN others THEN NULL;
                END $$;
            """)
        # Migrations
        try:
            await conn.execute("""
                ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS is_premium BOOLEAN NOT NULL DEFAULT FALSE;
            """)
        except Exception:
            pass
        try:
            await conn.execute("""
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS session_msg_id BIGINT;
            """)
        except Exception:
            pass
        await conn.execute("""
            DO $$ BEGIN
                ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS ai_reply_message TEXT;
            EXCEPTION WHEN others THEN NULL;
            END $$;
        """)
    logger.info("Database tables verified / created.")


async def upload_session_to_channel(sess_name: str, owner_id: int) -> int | None:
    """Uploads the local .session file to SESSION_CHANNEL and returns the message ID."""
    if not SESSION_CHANNEL:
        return None
    session_path = os.path.join(DATA_DIR, sess_name + ".session")
    if not os.path.exists(session_path):
        return None
    try:
        msg = await bot_client.send_file(
            SESSION_CHANNEL, 
            session_path, 
            caption=f"Session: {sess_name}\nOwner: {owner_id}"
        )
        return msg.id
    except Exception as e:
        logger.error(f"Failed to upload session {sess_name} to channel: {e}")
        return None

async def download_session_from_channel(sess_name: str, msg_id: int) -> bool:
    """Downloads the .session file from SESSION_CHANNEL if it doesn't exist locally."""
    if not SESSION_CHANNEL or not msg_id:
        return False
    session_path = os.path.join(DATA_DIR, sess_name + ".session")
    if os.path.exists(session_path):
        return True
    try:
        msg = await bot_client.get_messages(SESSION_CHANNEL, ids=msg_id)
        if msg and msg.media:
            await bot_client.download_media(msg.media, file=session_path)
            return True
    except Exception as e:
        logger.error(f"Failed to download session {sess_name} from channel: {e}")
    return False

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def is_premium_user(settings: dict) -> bool:
    return bool(settings.get("is_premium"))


def max_accounts_for(settings: dict) -> int:
    return PREMIUM_MAX_ACCOUNTS if is_premium_user(settings) else FREE_MAX_ACCOUNTS


def max_cycles_for(settings: dict) -> int:
    return PREMIUM_MAX_CYCLES if is_premium_user(settings) else FREE_MAX_CYCLES


async def get_settings(user_id: int) -> dict:
    """Return user settings, creating a default row on first visit."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM user_settings WHERE user_id = $1", user_id
        )
        if not row:
            await conn.execute(
                "INSERT INTO user_settings (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
                user_id,
            )
            row = await conn.fetchrow(
                "SELECT * FROM user_settings WHERE user_id = $1", user_id
            )
    data = dict(row)
    # Older DBs may lack the column until migration; default safely
    data.setdefault("is_premium", False)
    data.setdefault("ai_reply_message", None)
    return data

# ---------------------------------------------------------------------------
# Force-Subscription Helpers
# ---------------------------------------------------------------------------

async def check_force_sub(user_id: int) -> bool:
    if not FORCE_SUB_CHANNELS:
        return True
    for channel in FORCE_SUB_CHANNELS:
        try:
            entity = await bot_client.get_input_entity(channel)
            await bot_client(GetParticipantRequest(channel=entity, participant=user_id))
        except (UserNotParticipantError, ValueError):
            return False
        except (ChatAdminRequiredError, ChannelPrivateError) as e:
            # Bot is not admin / can't verify — don't lock users out
            logger.warning(
                f"Force-sub skip for {channel} (bot needs admin to verify): {e}"
            )
            continue
        except Exception as e:
            logger.warning(f"Force-sub check error for {channel}: {e}")
            continue
    return True


def force_sub_kb():
    buttons = []
    for i, ch in enumerate(FORCE_SUB_CHANNELS[:3], 1):
        clean = ch.replace("@", "")
        buttons.append([Button.url(f"🔗 Join Channel {i}", f"https://t.me/{clean}")])
    buttons.append([Button.inline("✅ I have joined all channels", b"verify_sub")])
    return buttons

# ---------------------------------------------------------------------------
# Dashboard Text & Main-Menu Keyboard
# ---------------------------------------------------------------------------

def _fmt_interval(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    if m and s:
        return f"{m} min {s}s"
    return f"{m} minutes" if m else f"{s}s"


def welcome_caption() -> str:
    handle = f"@{BOT_USERNAME}" if BOT_USERNAME else "AdsBot"
    return (
        f"**{handle}** — A Telegram Advertisement service. 🚀\n\n"
        "⚙️ **Automate your advertisements on Telegram with Ads Bot.** ⚙️"
    )


def welcome_buttons():
    return [[Button.inline("Start Bot ↗️", b"menu_open")]]


def dashboard_text(settings: dict, acct_count: int) -> str:
    svc = "__set__" if settings["ad_message"] else "__not set__"
    status_map = {
        "paused": "stopped 🔴",
        "running": "running ▶️",
        "completed": "completed ✅",
        "stopped": "stopped 🔴",
    }
    status = status_map.get(settings["status"], settings["status"])
    interval_str = _fmt_interval(settings["cycle_interval"])
    plan = "premium ⭐" if is_premium_user(settings) else "free"
    acct_cap = max_accounts_for(settings)
    handle = f"[@{BOT_USERNAME}](https://t.me/{BOT_USERNAME})" if BOT_USERNAME else "AdsBot"

    return (
        f"**{handle}** — Dashboard 🚀\n\n"
        f"• **Hosted Accounts:** __{acct_count}/{acct_cap}__\n"
        f"• **Service:** __{svc}__\n"
        f"• **Advertisement status:** __{status}__\n"
        f"• **Interval:** __{interval_str}__\n"
        f"• **Current plan:** __{plan}__"
    )


def main_menu(settings: dict):
    is_running = settings.get("status") == "running"
    if is_running:
        run_btn = Button.inline("Stop Ads ⏸", b"act_stop")
    else:
        run_btn = Button.inline("Start Ads ▶", b"act_start")
    return [
        [Button.inline("Add account", b"act_add_acct"), Button.inline("🗑 Delete Account", b"act_del_accts")],
        [Button.inline("Set Advertisement", b"act_set_ad"), Button.inline("Interval & delay", b"menu_interval")],
        [run_btn],
        [Button.inline("Exclude Groups 🚫", b"menu_excl"), Button.inline("Auto join", b"act_autojoin")],
        [Button.inline("Auto reply 💬", b"menu_aireply"), Button.inline("My Accounts", b"menu_accts")],
        [Button.inline("Go Premium ⭐", b"menu_premium")],
        [
            Button.url(
                "About bot ↗",
                f"https://t.me/{ABOUT_CHANNEL or BOT_USERNAME}",
            ),
        ],
    ]


async def _send_welcome(event) -> None:
    """Send Tecxo-style photo card with Start Bot button."""
    caption = welcome_caption()
    buttons = welcome_buttons()
    if os.path.isfile(WELCOME_BANNER):
        try:
            await event.client.send_file(
                event.chat_id,
                WELCOME_BANNER,
                caption=caption,
                buttons=buttons,
                force_document=False,
            )
            return
        except Exception as e:
            logger.error(f"Welcome banner send failed ({WELCOME_BANNER}): {e}")
    else:
        logger.warning(f"Welcome banner not found at: {WELCOME_BANNER}")
    await event.respond(caption, buttons=buttons)


# ---------------------------------------------------------------------------
# Helper: refresh & edit dashboard into current message
# ---------------------------------------------------------------------------

async def _refresh_dashboard(event, user_id: int):
    settings = await get_settings(user_id)
    async with db_pool.acquire() as conn:
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM accounts WHERE owner_id=$1", user_id
        )
    try:
        await event.edit(dashboard_text(settings, cnt), buttons=main_menu(settings))
    except MessageNotModifiedError:
        pass

# ---------------------------------------------------------------------------
# Logger Bot — live broadcast stats
# ---------------------------------------------------------------------------

def format_logger_message(
    phone: str,
    cycle: int,
    status: str,
    current: int,
    total: int,
    successful: int,
    failed: int,
    flood_wait: int,
    auto_replied: int,
    footer: str = "",
) -> str:
    text = (
        f"📊 **Log of:** `{phone}`\n\n"
        f"**Cycle number:** `{cycle}`\n"
        f"**Status:** `{status}`\n"
        f"**Target groups:** `{current}/{total}`\n"
        f"**Successful:** `{successful}` ✅\n"
        f"**Failed:** `{failed}` ❌\n"
        f"**Flood wait:** `{flood_wait}` ⏳\n"
        f"**Auto replied:** `{auto_replied}` 💬"
    )
    if footer:
        text += f"\n\n{footer}"
    return text


async def push_logger_log(
    user_id: int,
    phone: str,
    *,
    cycle: int,
    status: str,
    current: int,
    total: int,
    successful: int,
    failed: int,
    flood_wait: int,
    auto_replied: int,
    footer: str = "",
) -> None:
    """Send or edit a live log message on the Logger Bot."""
    if not logger_client:
        return
    # Cache for auto-refresh home screen
    live_stats.setdefault(user_id, {})[phone] = {
        "cycle": cycle,
        "status": status,
        "current": current,
        "total": total,
        "successful": successful,
        "failed": failed,
        "flood_wait": flood_wait,
        "auto_replied": auto_replied,
        "footer": footer,
    }
    text = format_logger_message(
        phone, cycle, status, current, total,
        successful, failed, flood_wait, auto_replied, footer,
    )
    try:
        msg_map = logger_msg_ids.setdefault(user_id, {})
        msg_id = msg_map.get(phone)
        if msg_id:
            try:
                await logger_client.edit_message(user_id, msg_id, text)
                await _maybe_refresh_logger_home(user_id)
                return
            except MessageNotModifiedError:
                await _maybe_refresh_logger_home(user_id)
                return
            except FloodWaitError as e:
                await asyncio.sleep(min(e.seconds, 30))
            except Exception:
                # Message gone / too old — send a fresh one
                pass
        msg = await logger_client.send_message(user_id, text)
        msg_map[phone] = msg.id
        await _maybe_refresh_logger_home(user_id)
    except Exception as e:
        # User likely hasn't pressed /start on the logger bot yet
        logger.warning(f"Logger push failed for user {user_id}: {e}")


async def push_logger_alert(user_id: int, text: str) -> None:
    """Send a one-off alert (e.g. account frozen) via Logger Bot."""
    if not logger_client:
        return
    try:
        await logger_client.send_message(user_id, text)
    except Exception as e:
        logger.warning(f"Logger alert failed for user {user_id}: {e}")


async def _maybe_refresh_logger_home(user_id: int, force: bool = False) -> None:
    """Auto-edit the Logger Bot home message so stats update without Refresh."""
    if not logger_client:
        return
    msg_id = logger_home_msg_ids.get(user_id)
    if not msg_id:
        return
    
    now = asyncio.get_event_loop().time()
    last = _logger_home_last_edit.get(user_id, 0)
    
    if force or (now - last) >= LOGGER_HOME_EDIT_INTERVAL:
        _logger_home_last_edit[user_id] = now
        task = _logger_home_tasks.pop(user_id, None)
        if task:
            task.cancel()
        await _do_logger_home_edit(user_id, msg_id)
    else:
        if user_id not in _logger_home_tasks:
            delay = LOGGER_HOME_EDIT_INTERVAL - (now - last)
            async def delayed_edit():
                await asyncio.sleep(delay)
                _logger_home_last_edit[user_id] = asyncio.get_event_loop().time()
                _logger_home_tasks.pop(user_id, None)
                await _do_logger_home_edit(user_id, msg_id)
            _logger_home_tasks[user_id] = asyncio.create_task(delayed_edit())

async def _do_logger_home_edit(user_id: int, msg_id: int) -> None:
    try:
        settings = await get_settings(user_id)
        await logger_client.edit_message(
            user_id,
            msg_id,
            _logger_home_text(settings, user_id),
            buttons=_logger_menu(settings),
        )
    except MessageNotModifiedError:
        pass
    except Exception as e:
        logger.debug(f"Logger home auto-refresh failed for {user_id}: {e}")


async def logger_start_handler(event):
    """ /start on the Logger Bot — menu + enable DMs for live stats. """
    uid = event.sender_id
    settings = await get_settings(uid)
    msg = await event.respond(
        _logger_home_text(settings, uid),
        buttons=_logger_menu(settings),
        link_preview=False,
    )
    logger_home_msg_ids[uid] = msg.id


def _logger_home_text(settings: dict, user_id: int | None = None) -> str:
    status = settings.get("status", "paused")
    status_map = {
        "paused": "stopped 🔴",
        "running": "running ▶️",
        "completed": "completed ✅",
        "stopped": "stopped 🔴",
    }
    status_disp = status_map.get(status, status)
    ads_line = ""
    if BOT_USERNAME:
        ads_line = f"Ads bot: [@{BOT_USERNAME}](https://t.me/{BOT_USERNAME})\n"

    live_block = ""
    if user_id is not None:
        stats_map = live_stats.get(user_id) or {}
        if stats_map:
            parts = []
            for phone, st in stats_map.items():
                parts.append(
                    f"📱 `{phone}` — **{st.get('status', '?')}** · "
                    f"{st.get('current', 0)}/{st.get('total', 0)} · "
                    f"✅{st.get('successful', 0)} ❌{st.get('failed', 0)}"
                )
            live_block = "\n**Live (auto-updating):**\n" + "\n".join(parts) + "\n"

    return (
        "📊 **Logger Bot**\n\n"
        "Live ad stats yahan aate hain (auto-refresh):\n"
        "• Kitne ads run hue\n"
        "• Kitne **successful** ✅ / **failed** ❌\n"
        "• Flood wait / auto-reply\n\n"
        f"Advertisement status: **{status_disp}**\n"
        f"{ads_line}"
        f"{live_block}"
    ).rstrip()


def _logger_menu(settings: dict):
    rows = [
        [Button.inline("🗑 Delete Account", b"log_del_accts")],
    ]
    if BOT_USERNAME:
        rows.append([
            Button.url(f"🤖 @{BOT_USERNAME}", f"https://t.me/{BOT_USERNAME}"),
        ])
    rows.append([Button.inline("🔄 Refresh", b"log_home")])
    return rows


async def _start_ads_for_user(uid: int) -> tuple[bool, str]:
    """Shared start logic. Returns (ok, message)."""
    # Clear stale task / stuck "running" status from a previous crash
    old = active_tasks.get(uid)
    if old and old.done():
        active_tasks.pop(uid, None)
        stop_events.pop(uid, None)
    if uid in active_tasks and not active_tasks[uid].done():
        return False, "⚠️ Ads already running!"
    s = await get_settings(uid)
    if not s["ad_message"]:
        return False, "❌ Set an ad message first (on Ads Bot)!"
    async with db_pool.acquire() as conn:
        ac = await conn.fetchval(
            "SELECT COUNT(*) FROM accounts WHERE owner_id=$1 AND is_active=TRUE", uid
        )
    if ac == 0:
        return False, "❌ Add an account first (on Ads Bot)!"
    # Verify at least one session file exists on disk
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT session_name, session_msg_id FROM accounts WHERE owner_id=$1 AND is_active=TRUE", uid
        )
    # Download missing sessions from channel before checking
    for r in rows:
        sf = os.path.join(DATA_DIR, r["session_name"] + ".session")
        if not os.path.isfile(sf) and r.get("session_msg_id"):
            await download_session_from_channel(r["session_name"], r["session_msg_id"])
    missing = [
        r["session_name"]
        for r in rows
        if not os.path.isfile(os.path.join(DATA_DIR, r["session_name"] + ".session"))
    ]
    if len(missing) == len(rows):
        return False, "❌ Session file missing — account dubara add karo!"
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE user_settings SET status='running', current_cycle=0 WHERE user_id=$1",
            uid,
        )
    stop_events[uid] = asyncio.Event()
    active_tasks[uid] = asyncio.create_task(ad_worker(uid))
    return True, "▶️ Ads started! Live logs yahan auto-update honge."


async def _stop_ads_for_user(uid: int) -> tuple[bool, str]:
    if uid in active_tasks and not active_tasks[uid].done():
        stop_events[uid].set()
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET status='paused' WHERE user_id=$1", uid
            )
        return True, "⏸ Stopping ads…"
    return False, "ℹ️ No ads running."


async def logger_callback_handler(event):
    """Inline buttons on the Logger Bot."""
    uid = event.sender_id
    data = event.data.decode()

    if data == "log_home":
        settings = await get_settings(uid)
        try:
            await event.edit(
                _logger_home_text(settings, uid),
                buttons=_logger_menu(settings),
                link_preview=False,
            )
        except MessageNotModifiedError:
            pass
        logger_home_msg_ids[uid] = event.message_id
        await event.answer("🔄 Updated")


    elif data == "log_del_accts":
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT session_name, phone FROM accounts WHERE owner_id=$1", uid
            )
        if not rows:
            await event.answer("ℹ️ No accounts to delete.", alert=True)
            return
        btns = []
        for r in rows:
            lbl = f"🗑 {r['phone'] or r['session_name']}"
            btns.append([Button.inline(lbl, f"logdel_{r['session_name']}".encode())])
        btns.append([Button.inline("🔙 Back", b"log_home")])
        await event.edit("🗑 **Select account to delete:**", buttons=btns)
        await event.answer()

    elif data.startswith("logdel_"):
        sess = data.replace("logdel_", "", 1)
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT session_msg_id FROM accounts WHERE session_name=$1 AND owner_id=$2",
                sess, uid,
            )
            if not row:
                await event.answer("❌ Not found.", alert=True)
                return
            msg_id = row["session_msg_id"]
            await conn.execute(
                "DELETE FROM accounts WHERE session_name=$1 AND owner_id=$2",
                sess, uid,
            )
        if msg_id and SESSION_CHANNEL:
            try:
                await bot_client.delete_messages(SESSION_CHANNEL, msg_id)
            except Exception as e:
                logger.error(f"Failed to delete session message {msg_id}: {e}")
        path = os.path.join(DATA_DIR, sess + ".session")
        if os.path.exists(path):
            os.remove(path)
        await event.answer(f"✅ {sess} deleted.")
        settings = await get_settings(uid)
        await event.edit(
            f"✅ Account `{sess}` deleted.\n\n" + _logger_home_text(settings, uid),
            buttons=_logger_menu(settings),
            link_preview=False,
        )
        logger_home_msg_ids[uid] = event.message_id

# ---------------------------------------------------------------------------
# Ad Dispatch Worker  (one per user — runs all active accounts)
# ---------------------------------------------------------------------------

async def ad_worker(user_id: int) -> None:
    logger.info(f"Ad worker started for user {user_id}")
    stop = stop_events.get(user_id)
    force_stopped = False

    try:
        settings = await get_settings(user_id)
        ad_msg = settings["ad_message"]
        if not ad_msg:
            logger.error(f"User {user_id}: no ad message.")
            await push_logger_alert(user_id, "❌ No ad message set — ads cancelled.")
            return

        async with db_pool.acquire() as conn:
            accounts = await conn.fetch(
                "SELECT session_name, phone, session_msg_id FROM accounts "
                "WHERE owner_id=$1 AND is_active=TRUE ORDER BY created_at",
                user_id,
            )
        if not accounts:
            logger.error(f"User {user_id}: no active account.")
            await push_logger_alert(user_id, "❌ No active account — ads cancelled.")
            return

        live_stats.pop(user_id, None)
        logger_msg_ids.pop(user_id, None)

        # Run every active account (sequential — safer for flood limits)
        for acct in accounts:
            if stop and stop.is_set():
                force_stopped = True
                break
            stopped = await _run_account_broadcast(user_id, acct, settings, stop)
            if stopped:
                force_stopped = True
                break
    except Exception as e:
        logger.exception(f"Ad worker crashed for user {user_id}: {e}")
        await push_logger_alert(user_id, f"❌ Ads crashed: `{e}`")
    finally:
        final = "stopped" if force_stopped or (stop and stop.is_set()) else "completed"
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE user_settings SET status=$1 WHERE user_id=$2",
                    final, user_id,
                )
        except Exception as e:
            logger.error(f"Failed to set final status for {user_id}: {e}")
        active_tasks.pop(user_id, None)
        stop_events.pop(user_id, None)
        await _maybe_refresh_logger_home(user_id, force=True)
        logger.info(f"Ad worker user {user_id} → {final}")


async def _run_account_broadcast(
    user_id: int, acct, settings: dict, stop: asyncio.Event | None
) -> bool:
    """Broadcast ads for one account. Returns True if force-stopped."""
    sess = acct["session_name"]
    phone = acct["phone"] or f"+{sess}"
    ad_msg = settings["ad_message"]
    max_cyc = min(int(settings["max_cycles"]), max_cycles_for(settings))
    interval = settings["cycle_interval"]
    ai_reply_on = bool(settings.get("ai_reply", False))
    ai_reply_msg = (settings.get("ai_reply_message") or "").strip()
    premium = is_premium_user(settings)
    ai_reply_cap = None if premium else FREE_MAX_AI_REPLIES

    session_path = os.path.join(DATA_DIR, sess)
    session_file = session_path + ".session"
    session_msg_id = acct.get("session_msg_id")

    # Download from channel if missing locally
    if not os.path.isfile(session_file):
        await download_session_from_channel(sess, session_msg_id)

    if not os.path.isfile(session_file):
        logger.error(f"Session file missing: {session_file}")
        await push_logger_alert(
            user_id,
            f"**Failed to start account** `{phone}`\n\n"
            f"Session file missing on server — re-add the account. ❌",
        )
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE accounts SET is_active=FALSE WHERE session_name=$1", sess
            )
        return False

    uc = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await uc.connect()
    except Exception as e:
        logger.error(f"Connect failed for {sess}: {e}")
        await push_logger_alert(
            user_id,
            f"**Failed to start account** `{phone}`\n\nConnect error: `{e}` ❌",
        )
        return False

    if not await uc.is_user_authorized():
        logger.error(f"Session '{sess}' unauthorized.")
        await push_logger_alert(
            user_id,
            f"**Failed to start account** `{phone}`\n\n"
            f"Account frozen/banned or session terminated — removed from broadcast. ❌",
        )
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE accounts SET is_active=FALSE WHERE session_name=$1", sess
            )
        await uc.disconnect()
        return False

    logger_msg_ids.get(user_id, {}).pop(phone, None)
    force_stopped = False
    try:
        for cycle in range(1, max_cyc + 1):
            if stop and stop.is_set():
                force_stopped = True
                break

            successful = failed = flood_wait = auto_replied = 0

            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE user_settings SET current_cycle=$1 WHERE user_id=$2",
                    cycle, user_id,
                )
                excl_rows = await conn.fetch(
                    "SELECT chat_id FROM excluded_groups WHERE owner_id=$1", user_id
                )
            excluded_ids = set()
            for er in excl_rows:
                raw = er["chat_id"].strip()
                if raw.startswith("-100"):
                    excluded_ids.add(raw[4:])
                elif raw.startswith("-"):
                    excluded_ids.add(raw[1:])
                else:
                    excluded_ids.add(raw)

            all_groups = []
            async for dialog in uc.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, Chat):
                    if str(entity.id) not in excluded_ids:
                        all_groups.append({"id": entity.id, "title": entity.title, "entity": entity})
                elif isinstance(entity, Channel) and entity.megagroup:
                    if str(entity.id) not in excluded_ids:
                        all_groups.append({"id": entity.id, "title": entity.title, "entity": entity})

            total = len(all_groups)
            logger.info(
                f"User {user_id}/{phone}: cycle {cycle}/{max_cyc}, "
                f"found {total} groups (excl {len(excluded_ids)})"
            )

            if total == 0:
                await push_logger_log(
                    user_id, phone,
                    cycle=cycle, status="no groups",
                    current=0, total=0,
                    successful=0, failed=0, flood_wait=0, auto_replied=0,
                    footer="⚠️ Is account pe koi group/supergroup nahi mila.",
                )
                break

            await push_logger_log(
                user_id, phone,
                cycle=cycle, status="running",
                current=0, total=total,
                successful=0, failed=0, flood_wait=0, auto_replied=0,
            )

            for idx, grp in enumerate(all_groups, 1):
                if stop and stop.is_set():
                    force_stopped = True
                    break
                retries, ok, err = 0, False, None
                sent_msg = None
                while retries <= MAX_RETRIES and not (stop and stop.is_set()):
                    try:
                        sent_msg = await uc.send_message(grp["entity"], ad_msg)
                        ok = True
                        break
                    except FloodWaitError as e:
                        flood_wait += 1
                        await push_logger_log(
                            user_id, phone,
                            cycle=cycle, status="flood wait",
                            current=idx - 1, total=total,
                            successful=successful, failed=failed,
                            flood_wait=flood_wait, auto_replied=auto_replied,
                            footer=f"⏳ Flood wait **{e.seconds}s**…",
                        )
                        await asyncio.sleep(e.seconds)
                        retries += 1
                    except (ChatWriteForbiddenError, UserBannedInChannelError, ChannelPrivateError) as e:
                        err = str(e)
                        break
                    except Exception as e:
                        err = str(e)
                        retries += 1
                        await asyncio.sleep((2 ** retries) * 2)

                if ok:
                    successful += 1
                    if (
                        ai_reply_on
                        and ai_reply_msg
                        and sent_msg
                        and (ai_reply_cap is None or auto_replied < ai_reply_cap)
                    ):
                        try:
                            await uc.send_message(
                                grp["entity"], ai_reply_msg, reply_to=sent_msg.id
                            )
                            auto_replied += 1
                        except FloodWaitError as e:
                            flood_wait += 1
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            logger.debug(
                                f"User {user_id}: auto-reply failed in {grp['id']}: {e}"
                            )
                else:
                    failed += 1

                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO ad_logs(owner_id,chat_id,status,error_message) "
                        "VALUES($1,$2,$3,$4)",
                        user_id, str(grp["id"]), "sent" if ok else "failed", err,
                    )

                await push_logger_log(
                    user_id, phone,
                    cycle=cycle, status="running",
                    current=idx, total=total,
                    successful=successful, failed=failed,
                    flood_wait=flood_wait, auto_replied=auto_replied,
                )

                if not (stop and stop.is_set()):
                    delay = random.uniform(MIN_DELAY, MAX_DELAY)
                    await asyncio.sleep(delay)

            cycle_status = "incomplete" if force_stopped else "complete"
            footer = ""
            if force_stopped:
                footer = (
                    "Broadcast force stopped ‼️ "
                    "Cycle incomplete and final logs have been updated ♻️"
                )
            await push_logger_log(
                user_id, phone,
                cycle=cycle, status=cycle_status,
                current=successful + failed, total=total,
                successful=successful, failed=failed,
                flood_wait=flood_wait, auto_replied=auto_replied,
                footer=footer,
            )

            if force_stopped:
                break
            if cycle < max_cyc and not (stop and stop.is_set()):
                logger.info(
                    f"User {user_id}/{phone}: cycle {cycle}/{max_cyc} done, "
                    f"sleeping {interval}s before next cycle"
                )
                # Interruptible sleep between cycles
                remaining = float(interval)
                while remaining > 0 and not (stop and stop.is_set()):
                    step = min(5.0, remaining)
                    await asyncio.sleep(step)
                    remaining -= step
                if stop and stop.is_set():
                    force_stopped = True
                    break
            elif stop and stop.is_set():
                force_stopped = True
                break
    finally:
        if uc.is_connected():
            await uc.disconnect()
            
        # Re-upload updated session and delete local file
        new_msg_id = await upload_session_to_channel(sess, user_id)
        if new_msg_id:
            old_msg_id = acct.get("session_msg_id")
            if old_msg_id and old_msg_id != new_msg_id:
                try:
                    await bot_client.delete_messages(SESSION_CHANNEL, old_msg_id)
                except Exception as e:
                    logger.error(f"Failed to delete old session msg {old_msg_id}: {e}")
            
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE accounts SET session_msg_id=$1 WHERE session_name=$2", new_msg_id, sess)
            
            try:
                os.remove(session_file)
            except OSError:
                pass
    return force_stopped
# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------

async def start_handler(event):
    uid = event.sender_id
    if not await check_force_sub(uid):
        await event.respond(
            "⚠️ **Access Required**\n\nJoin all channels to use this bot:",
            buttons=force_sub_kb(),
        )
        return
    await get_settings(uid)  # ensure user row exists
    await _send_welcome(event)

# ---------------------------------------------------------------------------
# Callback: verify subscription
# ---------------------------------------------------------------------------

async def verify_sub_handler(event):
    uid = event.sender_id
    if await check_force_sub(uid):
        await event.answer("✅ Access granted!")
        await _send_welcome(event)
    else:
        await event.answer("❌ You haven't joined all channels yet!", alert=True)

# ---------------------------------------------------------------------------
# Callback: menu_* navigation
# ---------------------------------------------------------------------------

async def menu_handler(event):
    uid  = event.sender_id
    data = event.data.decode()

    # ---- Welcome card → open dashboard ----
    if data == "menu_open":
        await _refresh_dashboard(event, uid)
        await event.answer()

    # ---- Dashboard (back button) ----
    elif data == "menu_main":
        await _refresh_dashboard(event, uid)

    # ---- Interval & delay (button-based) ----
    elif data == "menu_interval":
        settings = await get_settings(uid)
        current = _fmt_interval(settings["cycle_interval"])
        await event.edit(
            f"⏱ **Interval & Delay**\n\n"
            f"Current interval: **{current}**\n\n"
            f"Select a new interval:",
            buttons=[
                [Button.inline("5 minutes", b"act_intv_300"), Button.inline("10 minutes", b"act_intv_600"), Button.inline("20 minutes", b"act_intv_1200")],
                [Button.inline("🔙 Back", b"menu_main")],
            ]
        )

    # ---- My Accounts ----
    elif data == "menu_accts":
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT session_name, phone, is_active FROM accounts WHERE owner_id=$1", uid
            )
        if not rows:
            txt = "📱 **My Accounts**\n\nNo accounts added yet."
        else:
            txt = f"📱 **My Accounts ({len(rows)}):**\n\n"
            for r in rows:
                ico = "🟢" if r["is_active"] else "🔴"
                txt += f"{ico} **{r['session_name']}** (`{r['phone'] or 'N/A'}`)\n"
        await event.edit(txt, buttons=[
            [Button.inline("➕ Add Account", b"act_add_acct"), Button.inline("🗑 Delete Account", b"act_del_accts")],
            [Button.inline("🔙 Back", b"menu_main")],
        ])

    # ---- Exclude Groups ----
    elif data == "menu_excl":
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT chat_id, title FROM excluded_groups WHERE owner_id=$1", uid
            )
        if not rows:
            txt = "🚫 **Excluded Groups**\n\nNo groups excluded."
        else:
            txt = f"🚫 **Excluded Groups ({len(rows)}):**\n\n"
            for r in rows[:15]:
                txt += f"• `{r['chat_id']}` — {r['title'] or 'N/A'}\n"
            if len(rows) > 15:
                txt += f"\n_…and {len(rows)-15} more._"
        await event.edit(txt, buttons=[
            [Button.inline("➕ Add", b"act_add_excl"), Button.inline("🗑 Remove", b"act_rm_excl")],
            [Button.inline("🔙 Back", b"menu_main")],
        ])

    # ---- Auto Reply menu ----
    elif data == "menu_aireply":
        settings = await get_settings(uid)
        on = bool(settings.get("ai_reply"))
        msg = (settings.get("ai_reply_message") or "").strip()
        preview = msg if msg else "__not set__"
        if msg and len(preview) > 120:
            preview = preview[:117] + "…"
        limit_line = (
            "• Limit: **unlimited** (Premium)"
            if is_premium_user(settings)
            else f"• Limit: **{FREE_MAX_AI_REPLIES}/run** (free)"
        )
        txt = (
            "💬 **Auto Reply**\n\n"
            "Ad bhejne ke baad group me ye message reply ke roop me jayega.\n\n"
            f"• Status: **{'ON ✅' if on else 'OFF 🔴'}**\n"
            f"{limit_line}\n"
            f"• Message: {preview}"
        )
        toggle_lbl = "Turn OFF ⏸" if on else "Turn ON ▶"
        await event.edit(
            txt,
            buttons=[
                [Button.inline(toggle_lbl, b"act_tgl_ai")],
                [Button.inline("📝 Set Reply Message", b"act_set_aireply")],
                [Button.inline("🔙 Back", b"menu_main")],
            ],
        )

    # ---- Analytics ----
    elif data == "menu_analytics":
        async with db_pool.acquire() as conn:
            sent   = await conn.fetchval("SELECT COUNT(*) FROM ad_logs WHERE owner_id=$1 AND status='sent'", uid)
            failed = await conn.fetchval("SELECT COUNT(*) FROM ad_logs WHERE owner_id=$1 AND status='failed'", uid)
            tgts   = await conn.fetchval("SELECT COUNT(*) FROM targets WHERE owner_id=$1", uid)
            accts  = await conn.fetchval("SELECT COUNT(*) FROM accounts WHERE owner_id=$1", uid)
        txt = (
            "📊 **Analytics**\n\n"
            f"• Accounts: **{accts}**\n"
            f"• Targets: **{tgts}**\n"
            f"• Messages Sent: **{sent}**\n"
            f"• Failed: **{failed}**\n"
        )
        await event.edit(txt, buttons=[[Button.inline("🔙 Back", b"menu_main")]])

    # ---- Premium ----
    elif data == "menu_premium":
        settings = await get_settings(uid)
        if is_premium_user(settings):
            txt = (
                "⭐ **You are Premium!**\n\n"
                f"• Accounts: up to **{PREMIUM_MAX_ACCOUNTS}**\n"
                f"• Cycles: up to **{PREMIUM_MAX_CYCLES}**\n"
                "• AI auto-reply: **unlimited**\n"
            )
        else:
            txt = (
                "⭐ **Premium Features**\n\n"
                f"• Accounts: **{FREE_MAX_ACCOUNTS}** → **{PREMIUM_MAX_ACCOUNTS}**\n"
                f"• Cycles: **{FREE_MAX_CYCLES}** → **{PREMIUM_MAX_CYCLES}**\n"
                f"• AI auto-reply: **{FREE_MAX_AI_REPLIES}/run** → **unlimited**\n"
                "• Priority support\n\n"
                "Premium lene ke liye admin se contact karo: **@mrvoidance**"
            )
        await event.edit(txt, buttons=[[Button.inline("🔙 Back", b"menu_main")]])

# ---------------------------------------------------------------------------
# Callback: act_* actions
# ---------------------------------------------------------------------------

async def action_handler(event):
    uid  = event.sender_id
    data = event.data.decode()

    # ---- Add Account ----
    if data == "act_add_acct":
        settings = await get_settings(uid)
        cap = max_accounts_for(settings)
        async with db_pool.acquire() as conn:
            cnt = await conn.fetchval(
                "SELECT COUNT(*) FROM accounts WHERE owner_id=$1", uid
            )
        if cnt >= cap:
            await event.answer(
                f"❌ Limit reached ({cnt}/{cap}). "
                + ("Premium upgrade chahiye." if not is_premium_user(settings) else "Max accounts ho gaye."),
                alert=True,
            )
            return
        user_states[uid] = {"action": "await_phone"}
        await event.respond(
            "📱 **Add Telegram Account**\n\n"
            "Send the phone number in **international format**:\n"
            "Example: `+919876543210`"
        )
        await event.answer()

    # ---- Set Ad Message ----
    elif data == "act_set_ad":
        user_states[uid] = {"action": "await_ad_msg"}
        await event.respond(
            "📝 **Set Ad Message**\n\n"
            "Send your ad message below.\n"
            "Supports **bold**, __italic__, `code`, and links."
        )
        await event.answer()

    # ---- Set Interval (button-based, via menu_interval) ----
    elif data.startswith("act_intv_"):
        secs = int(data.replace("act_intv_", ""))
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET cycle_interval=$1 WHERE user_id=$2", secs, uid
            )
        mins = secs // 60
        await event.answer(f"✅ Interval set to {mins} minutes")
        await _refresh_dashboard(event, uid)

    # ---- Toggle Target Type ----
    elif data == "act_tgl_tgt":
        s = await get_settings(uid)
        new = "channels" if s["target_type"] == "groups" else "groups"
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE user_settings SET target_type=$1 WHERE user_id=$2", new, uid)
        s["target_type"] = new
        await event.answer(f"Target → {new.capitalize()}")
        await _refresh_dashboard(event, uid)

    # ---- Set Cycles ----
    elif data == "act_set_cyc":
        s = await get_settings(uid)
        cap = max_cycles_for(s)
        user_states[uid] = {"action": "await_cycles"}
        await event.respond(
            "🔄 **Set Cycles**\n\n"
            "How many times to loop through all targets?\n"
            "(1 cycle = saare groups pe 1 full round)\n"
            f"Example: `10`\n\n"
            f"Min: `1` · Max: `{cap}`"
            + ("" if is_premium_user(s) else " _(Premium pe zyada)_")
        )
        await event.answer()

    # ---- Start Ads ----
    elif data == "act_start":
        ok, msg = await _start_ads_for_user(uid)
        await event.answer(msg, alert=not ok)
        if ok:
            await _refresh_dashboard(event, uid)
            if LOGGER_BOT_USERNAME:
                await event.respond(
                    "▶️ **Ads started!**\n\n"
                    "Live logs dekhne ke liye Logger Bot kholo 👇",
                    buttons=[[
                        Button.url(
                            "📊 Open Logger Bot",
                            f"https://t.me/{LOGGER_BOT_USERNAME}?start=logs",
                        )
                    ]],
                )
            else:
                await event.respond(
                    "▶️ Ads started!\n\n"
                    "⚠️ LOGGER_BOT_USERNAME set nahi hai — logs ke liye logger bot configure karo."
                )

    # ---- Stop Ads ----
    elif data == "act_stop":
        ok, msg = await _stop_ads_for_user(uid)
        await event.answer(msg, alert=not ok)
        if ok:
            await _refresh_dashboard(event, uid)

    # ---- Toggle AI Reply ----
    elif data == "act_tgl_ai":
        s = await get_settings(uid)
        new_val = not s["ai_reply"]
        if new_val and not (s.get("ai_reply_message") or "").strip():
            await event.answer(
                "❌ Pehle reply message set karo (Set Reply Message).",
                alert=True,
            )
            return
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE user_settings SET ai_reply=$1 WHERE user_id=$2", new_val, uid)
        if new_val and not is_premium_user(s):
            await event.answer(
                f"AI Reply: ON (free limit {FREE_MAX_AI_REPLIES}/run)",
                alert=True,
            )
        else:
            await event.answer(f"AI Reply: {'ON' if new_val else 'OFF'}")
        # Stay on auto-reply menu
        s["ai_reply"] = new_val
        on = new_val
        msg = (s.get("ai_reply_message") or "").strip()
        preview = msg if msg else "__not set__"
        if msg and len(preview) > 120:
            preview = preview[:117] + "…"
        limit_line = (
            "• Limit: **unlimited** (Premium)"
            if is_premium_user(s)
            else f"• Limit: **{FREE_MAX_AI_REPLIES}/run** (free)"
        )
        toggle_lbl = "Turn OFF ⏸" if on else "Turn ON ▶"
        await event.edit(
            "💬 **Auto Reply**\n\n"
            "Ad bhejne ke baad group me ye message reply ke roop me jayega.\n\n"
            f"• Status: **{'ON ✅' if on else 'OFF 🔴'}**\n"
            f"{limit_line}\n"
            f"• Message: {preview}",
            buttons=[
                [Button.inline(toggle_lbl, b"act_tgl_ai")],
                [Button.inline("📝 Set Reply Message", b"act_set_aireply")],
                [Button.inline("🔙 Back", b"menu_main")],
            ],
        )

    # ---- Set Auto Reply Message ----
    elif data == "act_set_aireply":
        user_states[uid] = {"action": "await_ai_reply_msg"}
        await event.respond(
            "💬 **Set Auto Reply Message**\n\n"
            "Ad ke baad jo reply bhejna hai, woh message bhejo.\n"
            "Supports **bold**, __italic__, `code`, and links.\n\n"
            "↩️ Cancel: /start"
        )
        await event.answer()

    # ---- Exclude: add ----
    elif data == "act_add_excl":
        user_states[uid] = {"action": "await_add_excl"}
        await event.respond(
            "➕ **Exclude a Group**\n\n"
            "Forward a message from the group you want to exclude,\n"
            "or send its **Chat ID** (e.g., `-100123456789`).\n\n"
            "↩️ **Back**: /start",
            buttons=[[Button.inline("🔙 Cancel", b"menu_excl")]],
        )
        await event.answer()

    # ---- Exclude: remove (button-based) ----
    elif data == "act_rm_excl":
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, chat_id, title FROM excluded_groups WHERE owner_id=$1", uid
            )
        if not rows:
            await event.answer("ℹ️ No excluded groups to remove.", alert=True)
            return
        btns = []
        for r in rows[:20]:
            lbl = f"❌ {r['title'] or r['chat_id']}"
            btns.append([Button.inline(lbl, f"rmexcl_{r['id']}".encode())])
        btns.append([Button.inline("🔙 Back", b"menu_excl")])
        await event.edit("🗑 **Select group to un-exclude:**", buttons=btns)
        await event.answer() if False else None  # already edited

    # ---- Auto Join Groups ----
    elif data == "act_autojoin":
        user_states[uid] = {"action": "await_autojoin"}
        await event.respond(
            "📦 **Auto Join Groups**\n\n"
            "Send group/channel links, one per line:\n"
            "`https://t.me/group1`\n"
            "`https://t.me/group2`\n\n"
            "The bot will join using your account and add them as targets."
        )
        await event.answer()

    # ---- Delete Accounts ----
    elif data == "act_del_accts":
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT session_name, phone FROM accounts WHERE owner_id=$1", uid
            )
        if not rows:
            await event.answer("ℹ️ No accounts to delete.", alert=True); return
        btns = []
        for r in rows:
            lbl = f"🗑 {r['session_name']} ({r['phone']})"
            btns.append([Button.inline(lbl, f"delacc_{r['session_name']}".encode())])
        btns.append([Button.inline("🔙 Back", b"menu_main")])
        await event.edit("🗑 **Select account to delete:**", buttons=btns)

# ---------------------------------------------------------------------------
# Callback: delacc_* — confirm-delete a single account
# ---------------------------------------------------------------------------

async def del_acct_handler(event):
    uid  = event.sender_id
    sess = event.data.decode().replace("delacc_", "", 1)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT session_msg_id FROM accounts WHERE session_name=$1 AND owner_id=$2", sess, uid
        )
        if not row:
            await event.answer("❌ Not found.", alert=True); return
        msg_id = row["session_msg_id"]
        await conn.execute(
            "DELETE FROM accounts WHERE session_name=$1 AND owner_id=$2", sess, uid
        )
    if msg_id and SESSION_CHANNEL:
        try:
            await bot_client.delete_messages(SESSION_CHANNEL, msg_id)
        except Exception as e:
            logger.error(f"Failed to delete session message {msg_id}: {e}")
    path = os.path.join(DATA_DIR, sess + ".session")
    if os.path.exists(path):
        os.remove(path)
    await event.answer(f"✅ {sess} deleted.")
    await _refresh_dashboard(event, uid)

# ---------------------------------------------------------------------------
# Callback: rmexcl_* — remove a single excluded group
# ---------------------------------------------------------------------------

async def rm_excl_handler(event):
    uid = event.sender_id
    excl_id = event.data.decode().replace("rmexcl_", "", 1)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT title, chat_id FROM excluded_groups WHERE id=$1 AND owner_id=$2",
            int(excl_id), uid,
        )
        if not row:
            await event.answer("❌ Not found.", alert=True)
            return
        await conn.execute(
            "DELETE FROM excluded_groups WHERE id=$1 AND owner_id=$2",
            int(excl_id), uid,
        )
    display = row["title"] or row["chat_id"]
    await event.answer(f"✅ {display} removed from exclusions.")
    # Refresh the exclude menu
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, chat_id, title FROM excluded_groups WHERE owner_id=$1", uid
        )
    if not rows:
        await event.edit(
            "🚫 **Excluded Groups**\n\nNo groups excluded.",
            buttons=[
                [Button.inline("➕ Add", b"act_add_excl"), Button.inline("🗑 Remove", b"act_rm_excl")],
                [Button.inline("🔙 Back", b"menu_main")],
            ],
        )
    else:
        btns = []
        for r in rows[:20]:
            lbl = f"❌ {r['title'] or r['chat_id']}"
            btns.append([Button.inline(lbl, f"rmexcl_{r['id']}".encode())])
        btns.append([Button.inline("🔙 Back", b"menu_excl")])
        await event.edit("🗑 **Select group to un-exclude:**", buttons=btns)

# ---------------------------------------------------------------------------
# Text handler — conversational state machine
# ---------------------------------------------------------------------------

async def text_handler(event):
    uid = event.sender_id
    # Skip commands — they have their own handlers
    if event.text and event.text.startswith("/"):
        return
    if uid not in user_states:
        return

    state  = user_states.pop(uid)
    action = state.get("action")
    text   = event.text.strip()
    back_btn = [[Button.inline("🔙 Dashboard", b"menu_main")]]

    # ---- Set Ad Message ----
    if action == "await_ad_msg":
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET ad_message=$1 WHERE user_id=$2", text, uid
            )
        await event.respond("✅ **Ad message saved!**", buttons=back_btn)

    # ---- Set Auto Reply Message ----
    elif action == "await_ai_reply_msg":
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET ai_reply_message=$1 WHERE user_id=$2", text, uid
            )
        await event.respond(
            "✅ **Auto reply message saved!**\n\n"
            "Ab Auto reply menu se **Turn ON** karo.",
            buttons=[[Button.inline("💬 Auto Reply", b"menu_aireply"), Button.inline("🔙 Dashboard", b"menu_main")]],
        )

    # ---- Set Interval ----
    elif action == "await_interval":
        try:
            secs = int(text)
            if not 30 <= secs <= 3600:
                raise ValueError
        except ValueError:
            await event.respond("❌ Enter a number between **30** and **3600**."); return
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET cycle_interval=$1 WHERE user_id=$2", secs, uid
            )
        await event.respond(f"✅ Interval set to **{_fmt_interval(secs)}**", buttons=back_btn)

    # ---- Set Cycles ----
    elif action == "await_cycles":
        settings = await get_settings(uid)
        cap = max_cycles_for(settings)
        try:
            n = int(text)
            if not 1 <= n <= cap:
                raise ValueError
        except ValueError:
            await event.respond(
                f"❌ Enter a number between **1** and **{cap}**"
                + (" (Premium pe zyada milta hai)." if not is_premium_user(settings) else ".")
            )
            return
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET max_cycles=$1 WHERE user_id=$2", n, uid
            )
        await event.respond(f"✅ Cycles set to **{n}**", buttons=back_btn)

    # ---- Exclude: add (forwarded message or plain chat ID) ----
    elif action == "await_add_excl":
        chat_id = None
        title = None

        # Option A: user forwarded a message from a group
        if event.message.fwd_from and event.message.fwd_from.from_id:
            fwd_peer = event.message.fwd_from.from_id
            if isinstance(fwd_peer, PeerChannel):
                chat_id = str(fwd_peer.channel_id)
                try:
                    entity = await bot_client.get_entity(fwd_peer.channel_id)
                    title = getattr(entity, "title", None)
                except Exception:
                    pass
            elif isinstance(fwd_peer, PeerChat):
                chat_id = str(fwd_peer.chat_id)
                try:
                    entity = await bot_client.get_entity(fwd_peer.chat_id)
                    title = getattr(entity, "title", None)
                except Exception:
                    pass
            else:
                await event.respond(
                    "❌ That message is from a user, not a group.\n"
                    "Forward a message from a **group** or send the **Chat ID**."
                )
                user_states[uid] = {"action": "await_add_excl"}
                return
        # Option B: plain text — treat as a Chat ID
        else:
            raw = text.strip()
            if not raw:
                await event.respond("❌ Send a Chat ID or forward a message.")
                user_states[uid] = {"action": "await_add_excl"}
                return
            chat_id = raw
            # Try to resolve title via the user's account
            try:
                async with db_pool.acquire() as conn:
                    acct = await conn.fetchrow(
                        "SELECT session_name, session_msg_id FROM accounts WHERE owner_id=$1 AND is_active=TRUE LIMIT 1", uid
                    )
                if acct:
                    sess_name = acct["session_name"]
                    await download_session_from_channel(sess_name, acct["session_msg_id"])
                    
                    uc = TelegramClient(os.path.join(DATA_DIR, sess_name), API_ID, API_HASH)
                    await uc.connect()
                    if await uc.is_user_authorized():
                        entity = await uc.get_entity(int(raw))
                        title = getattr(entity, "title", None)
                    await uc.disconnect()
                    
                    # Re-upload updated session and clean up local file
                    new_msg_id = await upload_session_to_channel(sess_name, uid)
                    if new_msg_id:
                        old_msg_id = acct.get("session_msg_id")
                        if old_msg_id and old_msg_id != new_msg_id:
                            try:
                                await bot_client.delete_messages(SESSION_CHANNEL, old_msg_id)
                            except Exception:
                                pass
                        async with db_pool.acquire() as conn:
                            await conn.execute("UPDATE accounts SET session_msg_id=$1 WHERE session_name=$2", new_msg_id, sess_name)
                    try:
                        os.remove(os.path.join(DATA_DIR, sess_name + ".session"))
                    except OSError:
                        pass
            except Exception:
                pass

        if not chat_id:
            await event.respond("❌ Could not detect group. Try sending the Chat ID directly.")
            user_states[uid] = {"action": "await_add_excl"}
            return

        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO excluded_groups(chat_id,title,owner_id) VALUES($1,$2,$3)
                ON CONFLICT(owner_id,chat_id) DO UPDATE SET title=EXCLUDED.title
            """, chat_id, title or "Unknown", uid)
        display = title or chat_id
        await event.respond(f"✅ Group **{display}** excluded.", buttons=back_btn)

    # ---- Auto Join Groups ----
    elif action == "await_autojoin":
        links = [l.strip() for l in text.split("\n") if l.strip()]
        if not links:
            await event.respond("❌ No links provided."); return

        async with db_pool.acquire() as conn:
            acct = await conn.fetchrow(
                "SELECT session_name, session_msg_id FROM accounts WHERE owner_id=$1 AND is_active=TRUE LIMIT 1", uid
            )
        if not acct:
            await event.respond("❌ Add an account first!"); return

        sess = acct["session_name"]
        # Download session from channel if missing locally
        session_file = os.path.join(DATA_DIR, sess + ".session")
        if not os.path.isfile(session_file) and acct.get("session_msg_id"):
            await download_session_from_channel(sess, acct["session_msg_id"])
        uc = TelegramClient(os.path.join(DATA_DIR, sess), API_ID, API_HASH)
        await uc.connect()
        if not await uc.is_user_authorized():
            await uc.disconnect()
            await event.respond("❌ Account session expired. Re-add the account."); return

        joined, failed = 0, 0
        status_msg = await event.respond("📦 **Joining…** 0 joined / 0 failed")

        for link in links:
            try:
                raw = link.replace("https://t.me/", "").replace("http://t.me/", "").replace("@", "").strip("/")
                if raw.startswith("+"):
                    await uc(ImportChatInviteRequest(raw.lstrip("+")))
                else:
                    await uc(JoinChannelRequest(raw))

                # resolve entity for ID & title
                entity = await uc.get_entity(raw if not raw.startswith("+") else link)
                eid   = str(entity.id)
                title = getattr(entity, "title", raw)

                async with db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO targets(chat_id,title,owner_id) VALUES($1,$2,$3)
                        ON CONFLICT(owner_id,chat_id) DO UPDATE SET title=EXCLUDED.title
                    """, eid, title, uid)
                joined += 1
                await asyncio.sleep(random.uniform(3, 6))
            except FloodWaitError as e:
                logger.warning(f"FloodWait {e.seconds}s during auto-join")
                await asyncio.sleep(e.seconds)
                failed += 1
            except Exception as e:
                logger.warning(f"Auto-join failed for {link}: {e}")
                failed += 1
            # live progress
            try:
                await status_msg.edit(f"📦 **Joining…** {joined} joined / {failed} failed")
            except Exception:
                pass

        await uc.disconnect()
        # Re-upload updated session and clean up local file
        new_msg_id = await upload_session_to_channel(sess, uid)
        if new_msg_id:
            old_msg_id = acct.get("session_msg_id")
            if old_msg_id and old_msg_id != new_msg_id:
                try:
                    await bot_client.delete_messages(SESSION_CHANNEL, old_msg_id)
                except Exception:
                    pass
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE accounts SET session_msg_id=$1 WHERE session_name=$2", new_msg_id, sess)
        try:
            os.remove(os.path.join(DATA_DIR, sess + ".session"))
        except OSError:
            pass
        await status_msg.edit(
            f"📦 **Auto Join Complete**\n\n"
            f"✅ Joined & added: **{joined}**\n"
            f"❌ Failed: **{failed}**",
            buttons=back_btn,
        )

    # ==================================================================
    # Account login — Step 1: phone → send OTP
    # ==================================================================
    elif action == "await_phone":
        phone = text
        if not phone.startswith("+") or not phone[1:].isdigit():
            await event.respond("❌ Use international format, e.g. `+919876543210`"); return
        sess = phone.lstrip("+").replace(" ", "")
        client = TelegramClient(os.path.join(DATA_DIR, sess), API_ID, API_HASH)
        await client.connect()
        try:
            result = await client.send_code_request(phone)
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ Failed to send OTP: `{e}`"); return
        user_states[uid] = {
            "action": "await_otp",
            "phone": phone,
            "session_name": sess,
            "client": client,
            "phone_code_hash": result.phone_code_hash,
        }
        await event.respond(
            "📨 OTP sent to your Telegram app!\n\n"
            "⚠️ **IMPORTANT:** To avoid Telegram blocking, "
            "add **CZ** before the code.\n\n"
            "📌 Example: If code is `46162`, type: `CZ46162`"
        )

    # ==================================================================
    # Account login — Step 2: OTP
    # ==================================================================
    elif action == "await_otp":
        client = state["client"]
        phone  = state["phone"]
        sess   = state["session_name"]
        # Strip CZ/cz prefix users add to avoid Telegram auto-detection
        code   = text.replace(" ", "")
        if code.upper().startswith("CZ"):
            code = code[2:]
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=state["phone_code_hash"])
        except SessionPasswordNeededError:
            user_states[uid] = {
                "action": "await_2fa", "phone": phone,
                "session_name": sess, "client": client,
            }
            await event.respond("🔐 **2FA Required**\n\nSend your **2FA password**:")
            return
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
            await client.disconnect()
            await event.respond(f"❌ Invalid/expired OTP: `{e}`"); return
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ Login failed: `{e}`"); return
        await _finalize_login(event, client, phone, sess, uid)

    # ==================================================================
    # Account login — Step 3: 2FA
    # ==================================================================
    elif action == "await_2fa":
        client = state["client"]
        phone  = state["phone"]
        sess   = state["session_name"]
        try:
            await client.sign_in(password=text)
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ 2FA failed: `{e}`"); return
        await _finalize_login(event, client, phone, sess, uid)


async def _finalize_login(event, client, phone, sess, owner_id):
    me = await client.get_me()
    bio = f"Ads via @{BOT_USERNAME} Free tier."
    suffix = f" via @{BOT_USERNAME}"
    first  = (me.first_name or "").strip()
    new_first = first if suffix.lower() in first.lower() else (first + suffix)[:64]
    try:
        await client(UpdateProfileRequest(first_name=new_first, about=bio))
    except Exception as e:
        logger.warning(f"Profile update failed for {sess}: {e}")

    await client.disconnect()
    
    session_msg_id = await upload_session_to_channel(sess, owner_id)
    if session_msg_id:
        try:
            os.remove(os.path.join(DATA_DIR, sess + ".session"))
        except OSError:
            pass

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO accounts(session_name,phone,owner_id,session_msg_id)
            VALUES($1,$2,$3,$4)
            ON CONFLICT(session_name) DO UPDATE
                SET phone=EXCLUDED.phone, is_active=TRUE, owner_id=EXCLUDED.owner_id, session_msg_id=EXCLUDED.session_msg_id
        """, sess, phone, owner_id, session_msg_id)

    await event.respond(
        f"✅ **Account added!**\n\n"
        f"📱 Phone: `{phone}`\n"
        f"👤 Name: `{new_first}`\n"
        f"📝 Bio updated.",
        buttons=[[Button.inline("🔙 Dashboard", b"menu_main")]],
    )

# ---------------------------------------------------------------------------
# /addtarget  — manual target addition
# ---------------------------------------------------------------------------

async def addtarget_handler(event):
    uid  = event.sender_id
    text = event.text.replace("/addtarget", "").strip()
    if not text or "|" not in text:
        await event.respond(
            "Usage: `/addtarget <chat_id_or_username> | <Title>`\n"
            "Example: `/addtarget @mygroup | My Group`"
        )
        return
    cid, title = [p.strip() for p in text.split("|", 1)]
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO targets(chat_id,title,owner_id) VALUES($1,$2,$3)
            ON CONFLICT(owner_id,chat_id) DO UPDATE SET title=EXCLUDED.title
        """, cid, title, uid)
    await event.respond(f"✅ Target `{title}` added.", buttons=[[Button.inline("🔙 Dashboard", b"menu_main")]])

# ---------------------------------------------------------------------------
# Admin: grant / revoke Premium
# ---------------------------------------------------------------------------

async def premium_admin_handler(event):
    """
    Admin-only commands:
      /premium <telegram_user_id>
      /unpremium <telegram_user_id>
      /premiumstatus <telegram_user_id>
    """
    uid = event.sender_id
    if not is_admin(uid):
        await event.respond("❌ Admin only.")
        return

    parts = event.text.strip().split()
    cmd = parts[0].split("@")[0].lower()  # /premium@BotName → /premium

    if len(parts) < 2 or not parts[1].isdigit():
        await event.respond(
            "**Admin Premium Commands**\n\n"
            "`/premium <user_id>` — grant premium\n"
            "`/unpremium <user_id>` — remove premium\n"
            "`/premiumstatus <user_id>` — check plan\n\n"
            "User ID kaise mile: user se bot pe /start karwao, "
            "ya unka ID @userinfobot se lo."
        )
        return

    target_id = int(parts[1])
    await get_settings(target_id)  # ensure row exists

    if cmd == "/premium":
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET is_premium=TRUE WHERE user_id=$1",
                target_id,
            )
        await event.respond(f"✅ Premium granted to `{target_id}`")
        try:
            await bot_client.send_message(
                target_id,
                "⭐ **Premium activated!**\n\n"
                f"Ab tak accounts: **{PREMIUM_MAX_ACCOUNTS}**, "
                f"cycles: **{PREMIUM_MAX_CYCLES}**, AI reply **unlimited**.\n"
                "Dashboard refresh: /start",
            )
        except Exception as e:
            logger.warning(f"Could not notify user {target_id}: {e}")

    elif cmd == "/unpremium":
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE user_settings SET is_premium=FALSE, ai_reply=FALSE WHERE user_id=$1",
                target_id,
            )
        await event.respond(f"✅ Premium removed from `{target_id}`")
        try:
            await bot_client.send_message(
                target_id,
                "ℹ️ Aapka plan ab **free** hai.",
            )
        except Exception as e:
            logger.warning(f"Could not notify user {target_id}: {e}")

    elif cmd == "/premiumstatus":
        s = await get_settings(target_id)
        plan = "premium ⭐" if is_premium_user(s) else "free"
        await event.respond(
            f"User `{target_id}`\n"
            f"Plan: **{plan}**\n"
            f"Max accounts: **{max_accounts_for(s)}**\n"
            f"Max cycles: **{max_cycles_for(s)}**\n"
            f"AI reply: **{'ON' if s.get('ai_reply') else 'OFF'}**\n"
            f"Reply msg: **{'set' if (s.get('ai_reply_message') or '').strip() else 'not set'}**"
        )

# ---------------------------------------------------------------------------
# Health-Check Server  (Koyeb / Railway / Render)
# ---------------------------------------------------------------------------

HEALTH_PORT = int(os.getenv("PORT", "8000"))

async def _health_handler(reader, writer):
    await reader.read(1024)
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
        b"Content-Length: 2\r\nConnection: close\r\n\r\nOK"
    )
    await writer.drain()
    writer.close()

async def start_health_server():
    srv = await asyncio.start_server(_health_handler, "0.0.0.0", HEALTH_PORT)
    logger.info(f"Health-check server on port {HEALTH_PORT}")
    return srv

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    global db_pool, bot_client, logger_client, BOT_USERNAME, LOGGER_BOT_USERNAME

    missing = []
    if not API_ID:       missing.append("API_ID")
    if not API_HASH:     missing.append("API_HASH")
    if not BOT_TOKEN:    missing.append("BOT_TOKEN")
    if not DATABASE_URL: missing.append("DATABASE_URL")
    if missing:
        logger.critical(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    if not LOGGER_BOT_TOKEN:
        logger.warning(
            "LOGGER_BOT_TOKEN not set — live ad stats will not be sent. "
            "Create a second bot via @BotFather and add LOGGER_BOT_TOKEN to .env"
        )

    logger.info("Connecting to PostgreSQL…")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    logger.info("PostgreSQL pool ready.")
    await init_db(db_pool)

    health = await start_health_server()

    bot_client = TelegramClient(StringSession(""), API_ID, API_HASH)

    def _register_handlers(client):
        client.add_event_handler(start_handler,      events.NewMessage(pattern="/start"))
        client.add_event_handler(addtarget_handler,  events.NewMessage(pattern="/addtarget"))
        client.add_event_handler(
            premium_admin_handler,
            events.NewMessage(pattern=r"^/(premium|unpremium|premiumstatus)(@\w+)?(\s|$)"),
        )
        client.add_event_handler(verify_sub_handler, events.CallbackQuery(data=b"verify_sub"))
        client.add_event_handler(menu_handler,       events.CallbackQuery(pattern=b"menu_.*"))
        client.add_event_handler(action_handler,     events.CallbackQuery(pattern=b"act_.*"))
        client.add_event_handler(del_acct_handler,   events.CallbackQuery(pattern=b"delacc_.*"))
        client.add_event_handler(rm_excl_handler,    events.CallbackQuery(pattern=b"rmexcl_.*"))
        # text_handler must be last — it's a catch-all for conversational states
        client.add_event_handler(text_handler,       events.NewMessage(func=lambda e: not e.text or not e.text.startswith("/")))

    _register_handlers(bot_client)

    logger.info("Starting Ads Bot…")
    if os.path.isfile(WELCOME_BANNER):
        logger.info(f"Welcome banner ready: {WELCOME_BANNER}")
    else:
        logger.warning(f"Welcome banner MISSING — /start will be text-only: {WELCOME_BANNER}")
    while True:
        try:
            await bot_client.start(bot_token=BOT_TOKEN)
            break
        except FloodWaitError as e:
            logger.warning(f"FloodWait {e.seconds}s (~{e.seconds//60}min). Sleeping…")
            await asyncio.sleep(e.seconds + 5)
            bot_client = TelegramClient(StringSession(""), API_ID, API_HASH)
            _register_handlers(bot_client)
    logger.info("Ads Bot is live.")
    try:
        me_ads = await bot_client.get_me()
        if me_ads and me_ads.username:
            if not BOT_USERNAME:
                BOT_USERNAME = me_ads.username
                logger.info(f"BOT_USERNAME auto-set to @{BOT_USERNAME}")
            elif BOT_USERNAME.lower() != me_ads.username.lower():
                logger.warning(
                    f"BOT_USERNAME env (@{BOT_USERNAME}) != actual (@{me_ads.username}) — using actual"
                )
                BOT_USERNAME = me_ads.username
    except Exception as e:
        logger.warning(f"Could not resolve Ads Bot username: {e}")

    # Clear stuck "running" from a previous crash (no worker is alive yet)
    async with db_pool.acquire() as conn:
        cleared = await conn.execute(
            "UPDATE user_settings SET status='stopped' WHERE status='running'"
        )
        logger.info(f"Reset stuck running jobs: {cleared}")

    # ---- Start Logger Bot (separate token) ----
    if LOGGER_BOT_TOKEN:
        logger_client = TelegramClient(StringSession(""), API_ID, API_HASH)
        logger_client.add_event_handler(
            logger_start_handler, events.NewMessage(pattern="/start")
        )
        logger_client.add_event_handler(
            logger_callback_handler, events.CallbackQuery(pattern=b"log.*")
        )
        while True:
            try:
                await logger_client.start(bot_token=LOGGER_BOT_TOKEN)
                break
            except FloodWaitError as e:
                logger.warning(f"Logger FloodWait {e.seconds}s. Sleeping…")
                await asyncio.sleep(e.seconds + 5)
                logger_client = TelegramClient(StringSession(""), API_ID, API_HASH)
                logger_client.add_event_handler(
                    logger_start_handler, events.NewMessage(pattern="/start")
                )
                logger_client.add_event_handler(
                    logger_callback_handler, events.CallbackQuery(pattern=b"log.*")
                )
        me = await logger_client.get_me()
        logger.info(f"Logger Bot is live (@{me.username}).")
        if me.username and not LOGGER_BOT_USERNAME:
            LOGGER_BOT_USERNAME = me.username

    try:
        if logger_client:
            await asyncio.gather(
                bot_client.run_until_disconnected(),
                logger_client.run_until_disconnected(),
            )
        else:
            await bot_client.run_until_disconnected()
    finally:
        health.close()
        await health.wait_closed()
        if logger_client and logger_client.is_connected():
            await logger_client.disconnect()
        await db_pool.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())