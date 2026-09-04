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
    UserNotParticipantError,
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
)
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.functions.account import UpdateProfileRequest

# ---------------------------------------------------------------------------
# Environment & Config
# ---------------------------------------------------------------------------

load_dotenv()

API_ID      = int(os.getenv("API_ID", "0"))
API_HASH    = os.getenv("API_HASH", "")
BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")
ADMIN_IDS   = [int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()]
FORCE_SUB_CHANNELS = [x.strip() for x in os.getenv("FORCE_SUB_CHANNELS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL", "")

MIN_DELAY   = int(os.getenv("MIN_DELAY", "15"))
MAX_DELAY   = int(os.getenv("MAX_DELAY", "45"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# DATA_DIR used only for user Telethon .session files when running locally
DATA_DIR = os.path.abspath("data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.join(DATA_DIR, "app.log")

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
logger = logging.getLogger("CampaignBot")

# ---------------------------------------------------------------------------
# Global state  (bot_client is created inside main() — NOT at module level)
# ---------------------------------------------------------------------------

db_pool: asyncpg.Pool = None
bot_client: TelegramClient = None   # initialised in main() after env validation

active_campaign_task: asyncio.Task = None
campaign_stop_event: asyncio.Event = None

user_states: dict = {}

# ---------------------------------------------------------------------------
# Database — Initialisation
# ---------------------------------------------------------------------------

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                session_name TEXT PRIMARY KEY,
                phone        TEXT,
                is_active    BOOLEAN NOT NULL DEFAULT TRUE,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS targets (
                chat_id    TEXT PRIMARY KEY,
                title      TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                id         SERIAL PRIMARY KEY,
                name       TEXT UNIQUE NOT NULL,
                content    TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id           SERIAL PRIMARY KEY,
                name         TEXT UNIQUE NOT NULL,
                template_id  INTEGER REFERENCES templates(id),
                session_name TEXT    REFERENCES accounts(session_name),
                status       TEXT    NOT NULL DEFAULT 'stopped',
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_logs (
                id             SERIAL PRIMARY KEY,
                campaign_id    INTEGER REFERENCES campaigns(id),
                chat_id        TEXT,
                status         TEXT,
                error_message  TEXT,
                attempt_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (campaign_id, chat_id)
            )
        """)
    logger.info("Database tables verified / created.")

# ---------------------------------------------------------------------------
# Force Subscription Helpers
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
        except Exception as e:
            logger.warning(f"Error checking channel subscription for {channel}: {e}")
            return False
    return True


def get_force_sub_keyboard():
    buttons = []
    for i, ch in enumerate(FORCE_SUB_CHANNELS[:3], 1):
        clean_name = ch.replace("@", "")
        buttons.append([Button.url(f"🔗 Join Channel {i}", f"https://t.me/{clean_name}")])
    buttons.append([Button.inline("✅ I have joined all channels", b"verify_sub")])
    return buttons

# ---------------------------------------------------------------------------
# Control Panel Keyboard
# ---------------------------------------------------------------------------

def get_main_menu():
    return [
        [Button.inline("📊 Campaign Status", b"menu_status"), Button.inline("▶️ Start Campaign", b"menu_start")],
        [Button.inline("⏹️ Stop Campaign",   b"menu_stop"),   Button.inline("📈 View Stats",     b"menu_stats")],
        [Button.inline("🎯 Manage Targets",  b"menu_targets"),Button.inline("📝 Manage Templates",b"menu_templates")],
        [Button.inline("👤 Manage Accounts", b"menu_accounts")],
    ]

# ---------------------------------------------------------------------------
# Campaign Dispatch Engine
# ---------------------------------------------------------------------------

async def run_campaign_worker(campaign_id: int) -> None:
    global campaign_stop_event
    logger.info(f"Worker initiated for campaign ID: {campaign_id}")

    async with db_pool.acquire() as conn:
        camp = await conn.fetchrow(
            "SELECT template_id, session_name FROM campaigns WHERE id = $1", campaign_id
        )
        if not camp:
            logger.error(f"Campaign {campaign_id} does not exist.")
            return
        template_id, session_name = camp["template_id"], camp["session_name"]
        tmpl = await conn.fetchrow("SELECT content FROM templates WHERE id = $1", template_id)
        message_content = tmpl["content"] if tmpl else None

    if not message_content:
        logger.error("Message template is empty or missing — aborting.")
        return

    user_client = TelegramClient(os.path.join(DATA_DIR, session_name), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        logger.error(f"User session '{session_name}' is unauthorized.")
        await user_client.disconnect()
        return

    try:
        async with db_pool.acquire() as conn:
            all_targets = await conn.fetch("SELECT chat_id, title FROM targets")

        for row in all_targets:
            chat_id, title = row["chat_id"], row["title"]
            if campaign_stop_event.is_set():
                break

            async with db_pool.acquire() as conn:
                existing = await conn.fetchrow(
                    "SELECT status FROM campaign_logs WHERE campaign_id=$1 AND chat_id=$2",
                    campaign_id, chat_id,
                )
            if existing and existing["status"] == "sent":
                logger.info(f"Chat {chat_id} already received campaign #{campaign_id}. Skipping.")
                continue

            retries, success, error_msg = 0, False, None
            while retries <= MAX_RETRIES and not campaign_stop_event.is_set():
                try:
                    target_entity = (
                        int(chat_id) if chat_id.startswith("-100") or chat_id.lstrip("-").isdigit()
                        else chat_id
                    )
                    await user_client.send_message(target_entity, message_content)
                    success = True
                    break
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds); retries += 1
                except (ChatWriteForbiddenError, UserBannedInChannelError, ChannelPrivateError) as e:
                    error_msg = f"Forbidden: {e}"; break
                except Exception as e:
                    error_msg = str(e); retries += 1
                    await asyncio.sleep((2 ** retries) * 2)

            status_entry = "sent" if success else "failed"
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO campaign_logs (campaign_id, chat_id, status, error_message, attempt_at)
                    VALUES ($1,$2,$3,$4,NOW())
                    ON CONFLICT (campaign_id, chat_id) DO UPDATE
                        SET status=EXCLUDED.status, error_message=EXCLUDED.error_message, attempt_at=NOW()
                """, campaign_id, chat_id, status_entry, error_msg)

            if not campaign_stop_event.is_set():
                await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    finally:
        final_status = "stopped" if campaign_stop_event.is_set() else "completed"
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE campaigns SET status=$1 WHERE id=$2", final_status, campaign_id)
        await user_client.disconnect()
        logger.info(f"Campaign {campaign_id} finished: {final_status}.")

# ---------------------------------------------------------------------------
# Event Handler Functions  (registered via add_event_handler inside main())
# ---------------------------------------------------------------------------

async def start_handler(event):
    user_id = event.sender_id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await event.respond("🚫 Unauthorized access.")
        return
    if not await check_force_sub(user_id):
        await event.respond(
            "⚠️ **Access Required**\n\nJoin all channels to use this bot:",
            buttons=get_force_sub_keyboard()
        )
        return
    await event.respond(
        "🎛️ **Welcome to Campaign Management Control Panel**\n\n"
        "Use the buttons below to manage targets, templates, accounts, and campaigns.",
        buttons=get_main_menu()
    )


async def verify_sub_handler(event):
    if await check_force_sub(event.sender_id):
        await event.edit("✅ **Verified!** Welcome:", buttons=get_main_menu())
    else:
        await event.answer("❌ You haven't joined all channels yet!", alert=True)


async def menu_navigation_handler(event):
    user_id = event.sender_id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await event.answer("Unauthorized", alert=True)
        return

    data = event.data.decode("utf-8")

    if data == "menu_status":
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, name, status, session_name FROM campaigns ORDER BY id DESC LIMIT 5"
            )
        text = "📋 No campaigns found." if not rows else (
            "📋 **Recent Campaigns:**\n\n" +
            "".join(f"• **ID {r['id']} — {r['name']}**\n  Status: `{r['status']}` | Account: `{r['session_name']}`\n" for r in rows)
        )
        await event.edit(text, buttons=[[Button.inline("🔙 Back", b"menu_main")]])

    elif data == "menu_stats":
        async with db_pool.acquire() as conn:
            stats_rows = await conn.fetch("SELECT status, COUNT(*) AS cnt FROM campaign_logs GROUP BY status")
            target_count = await conn.fetchval("SELECT COUNT(*) FROM targets")
        stats = {r["status"]: r["cnt"] for r in stats_rows}
        text = (
            "📈 **Aggregated Statistics**\n\n"
            f"• **Targets:** `{target_count}`\n"
            f"• **Sent:** `{stats.get('sent', 0)}`\n"
            f"• **Failed:** `{stats.get('failed', 0)}`\n"
        )
        await event.edit(text, buttons=[[Button.inline("🔙 Back", b"menu_main")]])

    elif data == "menu_targets":
        async with db_pool.acquire() as conn:
            targets = await conn.fetch("SELECT chat_id, title FROM targets")
        text = f"🎯 **Targets ({len(targets)}):**\n\n"
        for r in targets[:10]:
            text += f"• `{r['chat_id']}`: {r['title']}\n"
        if len(targets) > 10:
            text += f"\n_…and {len(targets)-10} more._"
        await event.edit(text, buttons=[
            [Button.inline("➕ Add Target", b"act_add_target"), Button.inline("🗑️ Remove Target", b"act_rm_target")],
            [Button.inline("🔙 Back", b"menu_main")],
        ])

    elif data == "menu_templates":
        async with db_pool.acquire() as conn:
            templates = await conn.fetch("SELECT id, name FROM templates")
        text = f"📝 **Templates ({len(templates)}):**\n\n" + "".join(f"• **#{r['id']}**: {r['name']}\n" for r in templates)
        await event.edit(text, buttons=[
            [Button.inline("➕ Add Template", b"act_add_template")],
            [Button.inline("🔙 Back", b"menu_main")],
        ])

    elif data == "menu_accounts":
        async with db_pool.acquire() as conn:
            accounts = await conn.fetch("SELECT session_name, phone, is_active FROM accounts")
        text = f"👤 **Accounts ({len(accounts)}):**\n\n"
        for r in accounts:
            icon = "🟢" if r["is_active"] else "🔴"
            text += f"{icon} **{r['session_name']}** ({r['phone'] or 'N/A'})\n"
        await event.edit(text, buttons=[
            [Button.inline("➕ Add Account", b"act_add_account")],
            [Button.inline("🔙 Back", b"menu_main")],
        ])

    elif data == "menu_start":
        async with db_pool.acquire() as conn:
            camps = await conn.fetch("SELECT id, name FROM campaigns WHERE status IN ('stopped','pending')")
        if not camps:
            await event.edit(
                "ℹ️ No campaigns ready. Create one first.",
                buttons=[[Button.inline("➕ Create Campaign", b"act_create_campaign")],
                         [Button.inline("🔙 Back", b"menu_main")]]
            )
            return
        buttons = [[Button.inline(f"▶️ {r['name']}", f"run_camp_{r['id']}".encode())] for r in camps]
        buttons.append([Button.inline("🔙 Back", b"menu_main")])
        await event.edit("🚀 **Select a Campaign to Launch:**", buttons=buttons)

    elif data == "menu_stop":
        global active_campaign_task, campaign_stop_event
        if active_campaign_task and not active_campaign_task.done():
            campaign_stop_event.set()
            await event.edit("⏹️ Stop signal sent.", buttons=[[Button.inline("🔙 Back", b"menu_main")]])
        else:
            await event.edit("ℹ️ No campaign running.", buttons=[[Button.inline("🔙 Back", b"menu_main")]])

    elif data == "menu_main":
        await event.edit("🎛️ **Campaign Management Control Panel**", buttons=get_main_menu())


async def trigger_run_campaign(event):
    global active_campaign_task, campaign_stop_event
    cid = int(event.data.decode().split("_")[-1])
    if active_campaign_task and not active_campaign_task.done():
        await event.answer("⚠️ Another campaign is already running!", alert=True)
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE campaigns SET status='running' WHERE id=$1", cid)
    campaign_stop_event.clear()
    active_campaign_task = asyncio.create_task(run_campaign_worker(cid))
    await event.edit(
        f"🚀 **Campaign #{cid} launched.**",
        buttons=[[Button.inline("📊 Status", b"menu_status")], [Button.inline("⏹️ Stop", b"menu_stop")]]
    )


async def action_trigger(event):
    user_id = event.sender_id
    action = event.data.decode()

    if action == "act_add_target":
        user_states[user_id] = {"action": "await_target"}
        await event.respond("Send: `<chat_id_or_username> | <Title>`\nExample: `@group | My Group`")
        await event.answer()

    elif action == "act_rm_target":
        user_states[user_id] = {"action": "await_rm_target"}
        await event.respond("Send the Chat ID to remove:")
        await event.answer()

    elif action == "act_add_template":
        user_states[user_id] = {"action": "await_template"}
        await event.respond("Send: `<Name> :: <Content>`\nExample: `Promo :: Hello! Visit https://example.com`")
        await event.answer()

    elif action == "act_create_campaign":
        user_states[user_id] = {"action": "await_create_campaign"}
        await event.respond("Send: `<Name> | <TemplateID> | <SessionName>`\nExample: `Sale | 1 | main_account`")
        await event.answer()

    elif action == "act_add_account":
        user_states[user_id] = {"action": "await_account_phone"}
        await event.respond(
            "📱 **Add Telegram Account**\n\n"
            "Send the phone number in **international format**:\n"
            "Example: `+919876543210`"
        )
        await event.answer()


async def conversational_text_handler(event):
    user_id = event.sender_id
    if user_id not in user_states:
        return

    state = user_states.pop(user_id)
    action = state.get("action")
    text = event.text.strip()

    # --- Add Target ---
    if action == "await_target":
        if "|" not in text:
            await event.respond("❌ Invalid format. Use `<chat_id> | <Title>`.")
            return
        chat_id, title = [p.strip() for p in text.split("|", 1)]
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO targets (chat_id, title) VALUES ($1,$2)
                ON CONFLICT (chat_id) DO UPDATE SET title=EXCLUDED.title
            """, chat_id, title)
        await event.respond(f"✅ Target `{title}` saved.", buttons=get_main_menu())

    # --- Remove Target ---
    elif action == "await_rm_target":
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM targets WHERE chat_id=$1", text)
        await event.respond(f"✅ Target `{text}` removed.", buttons=get_main_menu())

    # --- Add Template ---
    elif action == "await_template":
        if "::" not in text:
            await event.respond("❌ Invalid format. Use `Name :: Content`.")
            return
        name, content = [p.strip() for p in text.split("::", 1)]
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO templates (name, content) VALUES ($1,$2)
                ON CONFLICT (name) DO UPDATE SET content=EXCLUDED.content
            """, name, content)
        await event.respond(f"✅ Template `{name}` saved.", buttons=get_main_menu())

    # --- Create Campaign ---
    elif action == "await_create_campaign":
        parts = [p.strip() for p in text.split("|")]
        if len(parts) != 3:
            await event.respond("❌ Expected `<Name> | <TemplateID> | <SessionName>`.")
            return
        name, tid_str, session = parts
        try:
            tid = int(tid_str)
        except ValueError:
            await event.respond("❌ TemplateID must be a number.")
            return
        async with db_pool.acquire() as conn:
            if not await conn.fetchval("SELECT 1 FROM templates WHERE id=$1", tid):
                await event.respond(f"❌ Template ID `{tid}` not found.")
                return
            if not await conn.fetchval("SELECT 1 FROM accounts WHERE session_name=$1", session):
                await event.respond(f"❌ Session `{session}` not found. Add the account first.")
                return
            await conn.execute(
                "INSERT INTO campaigns (name, template_id, session_name) VALUES ($1,$2,$3)",
                name, tid, session
            )
        await event.respond(f"✅ Campaign `{name}` created.", buttons=get_main_menu())

    # ------------------------------------------------------------------
    # Account login — Step 1: phone → request OTP
    # ------------------------------------------------------------------
    elif action == "await_account_phone":
        phone = text
        if not phone.startswith("+") or not phone[1:].isdigit():
            await event.respond("❌ Use international format e.g. `+919876543210`")
            return
        session_name = phone.lstrip("+").replace(" ", "")
        client = TelegramClient(os.path.join(DATA_DIR, session_name), API_ID, API_HASH)
        await client.connect()
        try:
            result = await client.send_code_request(phone)
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ Failed to send OTP: `{e}`")
            return
        user_states[user_id] = {
            "action": "await_account_otp",
            "phone": phone,
            "session_name": session_name,
            "client": client,
            "phone_code_hash": result.phone_code_hash,
        }
        await event.respond(
            "📨 OTP sent to your Telegram app!\n\n"
            "Send the **verification code**:\nExample: `12345`"
        )

    # ------------------------------------------------------------------
    # Account login — Step 2: OTP → sign in
    # ------------------------------------------------------------------
    elif action == "await_account_otp":
        client: TelegramClient = state["client"]
        phone = state["phone"]
        session_name = state["session_name"]
        code = text.replace(" ", "")
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=state["phone_code_hash"])
        except SessionPasswordNeededError:
            user_states[user_id] = {"action": "await_account_2fa", "phone": phone,
                                    "session_name": session_name, "client": client}
            await event.respond("🔐 **2FA Required**\n\nSend your **2FA password**:")
            return
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
            await client.disconnect()
            await event.respond(f"❌ Invalid/expired OTP: `{e}`\nStart again from Manage Accounts.")
            return
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ Login failed: `{e}`")
            return
        await _finalize_account_login(event, client, phone, session_name)

    # ------------------------------------------------------------------
    # Account login — Step 3: 2FA password
    # ------------------------------------------------------------------
    elif action == "await_account_2fa":
        client: TelegramClient = state["client"]
        phone = state["phone"]
        session_name = state["session_name"]
        try:
            await client.sign_in(password=text)
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ 2FA failed: `{e}`")
            return
        await _finalize_account_login(event, client, phone, session_name)


async def _finalize_account_login(event, client: TelegramClient, phone: str, session_name: str) -> None:
    me = await client.get_me()
    bio_text = (
        f"Ads via @{BOT_USERNAME} Free tier. "
        f"powered by {FORCE_SUB_CHANNELS[0] if len(FORCE_SUB_CHANNELS) > 0 else '@channel1'} "
        f"& {FORCE_SUB_CHANNELS[1] if len(FORCE_SUB_CHANNELS) > 1 else '@channel2'}"
    )
    suffix = f" via @{BOT_USERNAME}"
    current_first = (me.first_name or "").strip()
    new_first = current_first if suffix.lower() in current_first.lower() else (current_first + suffix)[:64]

    try:
        await client(UpdateProfileRequest(first_name=new_first, about=bio_text))
        logger.info(f"Profile updated for {session_name}: name='{new_first}'")
    except Exception as e:
        logger.warning(f"Could not update profile for {session_name}: {e}")

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO accounts (session_name, phone)
            VALUES ($1,$2)
            ON CONFLICT (session_name) DO UPDATE SET phone=EXCLUDED.phone, is_active=TRUE
        """, session_name, phone)

    await client.disconnect()
    await event.respond(
        f"✅ **Account added!**\n\n"
        f"📱 Phone: `{phone}`\n"
        f"👤 Name: `{new_first}`\n"
        f"📝 Bio updated.",
        buttons=get_main_menu()
    )

# ---------------------------------------------------------------------------
# CLI Account Auth Helper
# ---------------------------------------------------------------------------

async def cli_session_login() -> None:
    session_name = input("Session name (e.g. main_account): ").strip()
    phone        = input("Phone number with country code: ").strip()
    client = TelegramClient(os.path.join(DATA_DIR, session_name), API_ID, API_HASH)
    await client.start(phone=phone)
    logger.info(f"Authorized as '{session_name}'!")
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO accounts (session_name, phone) VALUES ($1,$2)
            ON CONFLICT (session_name) DO UPDATE SET phone=EXCLUDED.phone
        """, session_name, phone)
    await client.disconnect()
    print(f"[+] Session '{session_name}' saved.")

# ---------------------------------------------------------------------------
# Application Entry Point
# ---------------------------------------------------------------------------

async def main() -> None:
    global db_pool, bot_client, campaign_stop_event

    # --- Validate critical env vars before doing anything ---
    missing = []
    if not API_ID:       missing.append("API_ID")
    if not API_HASH:     missing.append("API_HASH")
    if not BOT_TOKEN:    missing.append("BOT_TOKEN")
    if not DATABASE_URL: missing.append("DATABASE_URL")
    if missing:
        logger.critical(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Set them in your .env file or in the platform's environment dashboard."
        )
        sys.exit(1)

    campaign_stop_event = asyncio.Event()

    # --- PostgreSQL pool ---
    logger.info("Connecting to PostgreSQL…")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    logger.info("PostgreSQL pool established.")
    await init_db(db_pool)

    if len(sys.argv) > 1 and sys.argv[1] == "--login":
        await cli_session_login()
        await db_pool.close()
        return

    # --- Create bot client HERE (after env validation) using StringSession ---
    # StringSession("") = in-memory session, re-authenticates via bot_token each start.
    # This makes the bot fully stateless — no local session files needed.
    bot_client = TelegramClient(StringSession(""), API_ID, API_HASH)

    # --- Register all event handlers ---
    bot_client.add_event_handler(start_handler,           events.NewMessage(pattern="/start"))
    bot_client.add_event_handler(verify_sub_handler,      events.CallbackQuery(data=b"verify_sub"))
    bot_client.add_event_handler(menu_navigation_handler, events.CallbackQuery(pattern=b"menu_.*"))
    bot_client.add_event_handler(trigger_run_campaign,    events.CallbackQuery(pattern=b"run_camp_.*"))
    bot_client.add_event_handler(action_trigger,          events.CallbackQuery(pattern=b"act_.*"))
    bot_client.add_event_handler(conversational_text_handler, events.NewMessage)

    logger.info("Starting Telegram Campaign Control Bot…")
    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("Bot is active and listening for events.")

    try:
        await bot_client.run_until_disconnected()
    finally:
        await db_pool.close()
        logger.info("PostgreSQL pool closed.")


if __name__ == "__main__":
    asyncio.run(main())