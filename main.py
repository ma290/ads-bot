import os
import sys
import asyncio
import logging
import random
from datetime import datetime
from dotenv import load_dotenv

import asyncpg
from telethon import TelegramClient, events, Button
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

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@")  # e.g. YourBotUsername
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()]
FORCE_SUB_CHANNELS = [x.strip() for x in os.getenv("FORCE_SUB_CHANNELS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL", "")

MIN_DELAY = int(os.getenv("MIN_DELAY", "15"))
MAX_DELAY = int(os.getenv("MAX_DELAY", "45"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# DATA_DIR is only used for Telethon session files (not the database)
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
# Global state
# ---------------------------------------------------------------------------

# asyncpg connection pool — initialised in main()
db_pool: asyncpg.Pool = None

# Telethon bot client
bot_client = TelegramClient(os.path.join(DATA_DIR, "bot_session"), API_ID, API_HASH)

# Active user client instances keyed by session_name
user_clients: dict = {}

# Bug-fix #3: asyncio.Event() must be created inside the running event loop.
# It is initialised inside main() rather than at module level.
active_campaign_task: asyncio.Task = None
campaign_stop_event: asyncio.Event = None

# Per-user conversation state: user_id -> {"action": "..."}
user_states: dict = {}

# ---------------------------------------------------------------------------
# Database — Initialisation
# ---------------------------------------------------------------------------

async def init_db(pool: asyncpg.Pool) -> None:
    """Create all tables if they do not already exist (PostgreSQL DDL)."""
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
    """Returns True only if the user has joined all configured channels."""
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
        url = f"https://t.me/{clean_name}"
        buttons.append([Button.url(f"🔗 Join Channel {i}", url)])
    buttons.append([Button.inline("✅ I have joined all channels", b"verify_sub")])
    return buttons

# ---------------------------------------------------------------------------
# Control Panel Keyboard
# ---------------------------------------------------------------------------

def get_main_menu():
    return [
        [Button.inline("📊 Campaign Status", b"menu_status"), Button.inline("▶️ Start Campaign", b"menu_start")],
        [Button.inline("⏹️ Stop Campaign", b"menu_stop"),    Button.inline("📈 View Stats", b"menu_stats")],
        [Button.inline("🎯 Manage Targets", b"menu_targets"), Button.inline("📝 Manage Templates", b"menu_templates")],
        [Button.inline("👤 Manage Accounts", b"menu_accounts")],
    ]

# ---------------------------------------------------------------------------
# Campaign Dispatch Engine
# ---------------------------------------------------------------------------

async def run_campaign_worker(campaign_id: int) -> None:
    """Dispatches a campaign to all configured targets using the linked session."""
    global campaign_stop_event
    logger.info(f"Worker initiated for campaign ID: {campaign_id}")

    # Fetch campaign + template in one query
    async with db_pool.acquire() as conn:
        camp = await conn.fetchrow(
            "SELECT template_id, session_name FROM campaigns WHERE id = $1",
            campaign_id,
        )
        if not camp:
            logger.error(f"Campaign {campaign_id} does not exist.")
            return

        template_id, session_name = camp["template_id"], camp["session_name"]

        tmpl = await conn.fetchrow(
            "SELECT content FROM templates WHERE id = $1", template_id
        )
        message_content = tmpl["content"] if tmpl else None

    if not message_content:
        logger.error("Message template is empty or missing — aborting.")
        return

    # Connect the Telethon user client for this session
    user_client = TelegramClient(os.path.join(DATA_DIR, session_name), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        logger.error(f"User session '{session_name}' is unauthorized. Aborting dispatch.")
        await user_client.disconnect()
        return

    try:
        # Fetch all targets
        async with db_pool.acquire() as conn:
            all_targets = await conn.fetch("SELECT chat_id, title FROM targets")

        for row in all_targets:
            chat_id, title = row["chat_id"], row["title"]

            if campaign_stop_event.is_set():
                logger.info("Graceful stop event received. Terminating campaign run.")
                break

            # Bug-fix #4: Skip targets that were already successfully sent in this campaign
            async with db_pool.acquire() as conn:
                existing = await conn.fetchrow(
                    "SELECT status FROM campaign_logs WHERE campaign_id = $1 AND chat_id = $2",
                    campaign_id,
                    chat_id,
                )
            if existing and existing["status"] == "sent":
                logger.info(f"Chat {chat_id} already received campaign #{campaign_id}. Skipping.")
                continue

            # Attempt dispatch with exponential backoff
            retries = 0
            success = False
            error_msg = None

            while retries <= MAX_RETRIES and not campaign_stop_event.is_set():
                try:
                    logger.info(f"Sending message to {title} ({chat_id})...")
                    # Resolve numeric chat IDs (supergroups/channels start with -100)
                    target_entity = (
                        int(chat_id)
                        if chat_id.startswith("-100") or chat_id.lstrip("-").isdigit()
                        else chat_id
                    )
                    await user_client.send_message(target_entity, message_content)
                    success = True
                    break
                except FloodWaitError as e:
                    logger.warning(f"FloodWait: sleeping {e.seconds}s.")
                    await asyncio.sleep(e.seconds)
                    retries += 1
                except (ChatWriteForbiddenError, UserBannedInChannelError, ChannelPrivateError) as e:
                    error_msg = f"Forbidden: {e}"
                    logger.error(f"Cannot post in {chat_id}: {error_msg}")
                    break
                except Exception as e:
                    error_msg = str(e)
                    retries += 1
                    backoff = (2 ** retries) * 2
                    logger.warning(f"Error on {chat_id} (attempt {retries}): {e}. Backoff {backoff}s.")
                    await asyncio.sleep(backoff)

            # Persist result — upsert using PostgreSQL ON CONFLICT syntax
            status_entry = "sent" if success else "failed"
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO campaign_logs (campaign_id, chat_id, status, error_message, attempt_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (campaign_id, chat_id) DO UPDATE
                        SET status        = EXCLUDED.status,
                            error_message = EXCLUDED.error_message,
                            attempt_at    = NOW()
                    """,
                    campaign_id,
                    chat_id,
                    status_entry,
                    error_msg,
                )

            # Pacing delay before next target
            if not campaign_stop_event.is_set():
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                logger.info(f"Pacing: waiting {delay:.2f}s before next target…")
                await asyncio.sleep(delay)

    finally:
        final_status = "stopped" if campaign_stop_event.is_set() else "completed"
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE campaigns SET status = $1 WHERE id = $2",
                final_status,
                campaign_id,
            )
        await user_client.disconnect()
        logger.info(f"Campaign {campaign_id} finished with status: {final_status}.")

# ---------------------------------------------------------------------------
# Bot Event Handlers
# ---------------------------------------------------------------------------

@bot_client.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    user_id = event.sender_id

    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await event.respond("🚫 Unauthorized access. You do not have permission to use this bot.")
        return

    if not await check_force_sub(user_id):
        await event.respond(
            "⚠️ **Access Required**\n\nYou must join all 3 channels below to use this bot:",
            buttons=get_force_sub_keyboard(),
        )
        return

    await event.respond(
        "🎛️ **Welcome to Campaign Management Control Panel**\n\n"
        "Use the buttons below to manage target groups, message templates, "
        "user sessions, and live dispatches.",
        buttons=get_main_menu(),
    )


@bot_client.on(events.CallbackQuery(data=b"verify_sub"))
async def verify_sub_handler(event):
    user_id = event.sender_id
    if await check_force_sub(user_id):
        await event.edit(
            "✅ **Verification successful!**\n\nWelcome to the Campaign Control Panel:",
            buttons=get_main_menu(),
        )
    else:
        await event.answer("❌ You have not joined all 3 channels yet!", alert=True)


@bot_client.on(events.CallbackQuery(pattern=b"menu_.*"))
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
        if not rows:
            text = "📋 No campaigns found."
        else:
            text = "📋 **Recent Campaigns:**\n\n"
            for r in rows:
                text += f"• **ID {r['id']} — {r['name']}**\n  Status: `{r['status']}` | Account: `{r['session_name']}`\n"
        await event.edit(text, buttons=[[Button.inline("🔙 Back", b"menu_main")]])

    elif data == "menu_stats":
        async with db_pool.acquire() as conn:
            stats_rows = await conn.fetch(
                "SELECT status, COUNT(*) AS cnt FROM campaign_logs GROUP BY status"
            )
            target_count = await conn.fetchval("SELECT COUNT(*) FROM targets")
        stats = {r["status"]: r["cnt"] for r in stats_rows}
        sent = stats.get("sent", 0)
        failed = stats.get("failed", 0)
        text = (
            "📈 **Aggregated Statistics**\n\n"
            f"• **Configured Targets:** `{target_count}`\n"
            f"• **Successful Dispatches:** `{sent}`\n"
            f"• **Failed Attempts:** `{failed}`\n"
        )
        await event.edit(text, buttons=[[Button.inline("🔙 Back", b"menu_main")]])

    elif data == "menu_targets":
        async with db_pool.acquire() as conn:
            targets = await conn.fetch("SELECT chat_id, title FROM targets")
        text = f"🎯 **Configured Targets ({len(targets)}):**\n\n"
        for r in targets[:10]:
            text += f"• `{r['chat_id']}`: {r['title']}\n"
        if len(targets) > 10:
            text += f"\n_…and {len(targets) - 10} more._"
        buttons = [
            [Button.inline("➕ Add Target", b"act_add_target"), Button.inline("🗑️ Remove Target", b"act_rm_target")],
            [Button.inline("🔙 Back", b"menu_main")],
        ]
        await event.edit(text, buttons=buttons)

    elif data == "menu_templates":
        async with db_pool.acquire() as conn:
            templates = await conn.fetch("SELECT id, name FROM templates")
        text = f"📝 **Configured Templates ({len(templates)}):**\n\n"
        for r in templates:
            text += f"• **#{r['id']}**: {r['name']}\n"
        buttons = [
            [Button.inline("➕ Add Template", b"act_add_template")],
            [Button.inline("🔙 Back", b"menu_main")],
        ]
        await event.edit(text, buttons=buttons)

    elif data == "menu_accounts":
        async with db_pool.acquire() as conn:
            accounts = await conn.fetch("SELECT session_name, phone, is_active FROM accounts")
        text = f"👤 **Configured Sessions ({len(accounts)}):**\n\n"
        for r in accounts:
            status_icon = "🟢" if r["is_active"] else "🔴"
            text += f"{status_icon} **{r['session_name']}** ({r['phone'] or 'N/A'})\n"
        buttons = [
            [Button.inline("➕ Add Account", b"act_add_account")],
            [Button.inline("🔙 Back", b"menu_main")],
        ]
        await event.edit(text, buttons=buttons)

    elif data == "menu_start":
        async with db_pool.acquire() as conn:
            camps = await conn.fetch(
                "SELECT id, name FROM campaigns WHERE status IN ('stopped', 'pending')"
            )
        if not camps:
            await event.edit(
                "ℹ️ No stopped or pending campaigns ready to start.\nCreate or configure one first.",
                buttons=[
                    [Button.inline("➕ Create Campaign", b"act_create_campaign")],
                    [Button.inline("🔙 Back", b"menu_main")],
                ],
            )
            return
        buttons = [
            [Button.inline(f"▶️ Start: {r['name']}", f"run_camp_{r['id']}".encode())]
            for r in camps
        ]
        buttons.append([Button.inline("🔙 Back", b"menu_main")])
        await event.edit("🚀 **Select a Campaign to Launch:**", buttons=buttons)

    elif data == "menu_stop":
        global active_campaign_task, campaign_stop_event
        if active_campaign_task and not active_campaign_task.done():
            campaign_stop_event.set()
            await event.edit(
                "⏹️ Stop signal sent. Finishing current message/wait…",
                buttons=[[Button.inline("🔙 Back", b"menu_main")]],
            )
        else:
            await event.edit(
                "ℹ️ No campaign is currently running.",
                buttons=[[Button.inline("🔙 Back", b"menu_main")]],
            )

    elif data == "menu_main":
        await event.edit("🎛️ **Campaign Management Control Panel**", buttons=get_main_menu())


@bot_client.on(events.CallbackQuery(pattern=b"run_camp_.*"))
async def trigger_run_campaign(event):
    global active_campaign_task, campaign_stop_event
    cid = int(event.data.decode("utf-8").split("_")[-1])

    if active_campaign_task and not active_campaign_task.done():
        await event.answer("⚠️ Another campaign is already in progress!", alert=True)
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE campaigns SET status = 'running' WHERE id = $1", cid
        )

    campaign_stop_event.clear()
    active_campaign_task = asyncio.create_task(run_campaign_worker(cid))
    await event.edit(
        f"🚀 **Campaign #{cid} is now running in the background.**",
        buttons=[
            [Button.inline("📊 Check Status", b"menu_status")],
            [Button.inline("⏹️ Stop", b"menu_stop")],
        ],
    )

# ---------------------------------------------------------------------------
# Conversational Action Handlers
# ---------------------------------------------------------------------------

@bot_client.on(events.CallbackQuery(pattern=b"act_.*"))
async def action_trigger(event):
    user_id = event.sender_id
    action = event.data.decode("utf-8")

    if action == "act_add_target":
        user_states[user_id] = {"action": "await_target"}
        await event.respond(
            "Send the target Chat ID or Username with a title:\n"
            "`<chat_id_or_username> | <Title>`\n\n"
            "Example:\n`@groupusername | Crypto Group`"
        )
        await event.answer()

    elif action == "act_rm_target":
        user_states[user_id] = {"action": "await_rm_target"}
        await event.respond("Send the target Chat ID to remove:")
        await event.answer()

    elif action == "act_add_template":
        user_states[user_id] = {"action": "await_template"}
        await event.respond(
            "Send your template name and content separated by `::`:\n\n"
            "Example:\n`PromoMsg :: Hello everyone! Check out our project at https://example.com`"
        )
        await event.answer()

    elif action == "act_create_campaign":
        user_states[user_id] = {"action": "await_create_campaign"}
        await event.respond(
            "Send campaign details:\n`<CampaignName> | <TemplateID> | <SessionName>`\n\n"
            "Example:\n`SummerSale | 1 | main_account`"
        )
        await event.answer()

    elif action == "act_add_account":
        user_states[user_id] = {"action": "await_account_phone"}
        await event.respond(
            "📱 **Add Telegram Account**\n\n"
            "Send the phone number in **international format**:\n"
            "Example: `+919876543210`"
        )
        await event.answer()


@bot_client.on(events.NewMessage)
async def conversational_text_handler(event):
    user_id = event.sender_id
    if user_id not in user_states:
        return

    state = user_states.pop(user_id)
    action = state.get("action")
    text = event.text.strip()

    if action == "await_target":
        if "|" not in text:
            await event.respond("❌ Invalid format. Operation canceled.")
            return
        parts = [p.strip() for p in text.split("|", 1)]
        chat_id, title = parts[0], parts[1]
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO targets (chat_id, title)
                VALUES ($1, $2)
                ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title
                """,
                chat_id,
                title,
            )
        await event.respond(f"✅ Target `{title}` (`{chat_id}`) saved.", buttons=get_main_menu())

    elif action == "await_rm_target":
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM targets WHERE chat_id = $1", text)
        await event.respond(f"✅ Target `{text}` removed.", buttons=get_main_menu())

    elif action == "await_template":
        if "::" not in text:
            await event.respond("❌ Invalid format. Use `Name :: Content`.")
            return
        name, content = [p.strip() for p in text.split("::", 1)]
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO templates (name, content)
                VALUES ($1, $2)
                ON CONFLICT (name) DO UPDATE SET content = EXCLUDED.content
                """,
                name,
                content,
            )
        await event.respond(f"✅ Template `{name}` saved.", buttons=get_main_menu())

    elif action == "await_create_campaign":
        parts = [p.strip() for p in text.split("|")]
        if len(parts) != 3:
            await event.respond("❌ Invalid format. Expected `<Name> | <TemplateID> | <SessionName>`.")
            return
        name, tid_str, session = parts[0], parts[1], parts[2]
        try:
            tid = int(tid_str)
        except ValueError:
            await event.respond("❌ TemplateID must be a number.")
            return
        # Validate template and session exist before inserting (fix FK violation)
        async with db_pool.acquire() as conn:
            tmpl_exists = await conn.fetchval("SELECT 1 FROM templates WHERE id = $1", tid)
            sess_exists = await conn.fetchval("SELECT 1 FROM accounts WHERE session_name = $1", session)
            if not tmpl_exists:
                await event.respond(f"❌ Template ID `{tid}` does not exist. Check available templates in Manage Templates.")
                return
            if not sess_exists:
                await event.respond(f"❌ Session `{session}` does not exist. Add the account first via Manage Accounts.")
                return
            await conn.execute(
                "INSERT INTO campaigns (name, template_id, session_name) VALUES ($1, $2, $3)",
                name,
                tid,
                session,
            )
        await event.respond(f"✅ Campaign `{name}` created.", buttons=get_main_menu())

    # ------------------------------------------------------------------
    # Account login flow — Step 1: phone received, send OTP
    # ------------------------------------------------------------------
    elif action == "await_account_phone":
        phone = text.strip()
        if not phone.startswith("+") or not phone[1:].isdigit():
            await event.respond(
                "❌ Invalid format. Please send the number in international format, e.g. `+919876543210`"
            )
            return
        # Derive a session name from the phone (strip leading +)
        session_name = phone.lstrip("+").replace(" ", "")
        client = TelegramClient(os.path.join(DATA_DIR, session_name), API_ID, API_HASH)
        await client.connect()
        try:
            result = await client.send_code_request(phone)
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ Failed to send OTP: `{e}`")
            return
        # Store client object between steps
        user_states[user_id] = {
            "action": "await_account_otp",
            "phone": phone,
            "session_name": session_name,
            "client": client,
            "phone_code_hash": result.phone_code_hash,
        }
        await event.respond(
            "📨 OTP sent to your Telegram app!\n\n"
            "Please send the **verification code** you received:\n"
            "Example: `12345`"
        )

    # ------------------------------------------------------------------
    # Account login flow — Step 2: OTP received, sign in
    # ------------------------------------------------------------------
    elif action == "await_account_otp":
        client: TelegramClient = state.get("client")
        phone = state.get("phone")
        session_name = state.get("session_name")
        phone_code_hash = state.get("phone_code_hash")
        code = text.strip().replace(" ", "")
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        except SessionPasswordNeededError:
            # 2FA is enabled — move to next step, keep client alive
            user_states[user_id] = {
                "action": "await_account_2fa",
                "phone": phone,
                "session_name": session_name,
                "client": client,
            }
            await event.respond(
                "🔐 **Two-Factor Authentication Required**\n\n"
                "Send your **2FA password**:"
            )
            return
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
            await client.disconnect()
            await event.respond(f"❌ Invalid or expired OTP: `{e}`\nPlease start again from Manage Accounts.")
            return
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ Login failed: `{e}`")
            return
        # Sign-in succeeded — save and update profile
        await _finalize_account_login(event, client, phone, session_name)

    # ------------------------------------------------------------------
    # Account login flow — Step 3: 2FA password received
    # ------------------------------------------------------------------
    elif action == "await_account_2fa":
        client: TelegramClient = state.get("client")
        phone = state.get("phone")
        session_name = state.get("session_name")
        password = text.strip()
        try:
            await client.sign_in(password=password)
        except Exception as e:
            await client.disconnect()
            await event.respond(f"❌ 2FA failed: `{e}`")
            return
        # Sign-in succeeded — save and update profile
        await _finalize_account_login(event, client, phone, session_name)


async def _finalize_account_login(
    event, client: TelegramClient, phone: str, session_name: str
) -> None:
    """Save account to DB, update bio/name, disconnect client."""
    me = await client.get_me()

    # Build new bio and name
    bio_text = (
        f"Ads via @{BOT_USERNAME} Free tier. "
        f"powered by {FORCE_SUB_CHANNELS[0] if len(FORCE_SUB_CHANNELS) > 0 else '@channel1'} "
        f"& {FORCE_SUB_CHANNELS[1] if len(FORCE_SUB_CHANNELS) > 1 else '@channel2'}"
    )
    current_first = (me.first_name or "").strip()
    suffix = f" via @{BOT_USERNAME}"
    # Avoid double-appending the suffix
    if suffix.lower() not in current_first.lower():
        new_first = (current_first + suffix)[:64]  # Telegram first name limit is 64 chars
    else:
        new_first = current_first

    try:
        await client(UpdateProfileRequest(
            first_name=new_first,
            about=bio_text,
        ))
        logger.info(f"Updated profile for {session_name}: name='{new_first}', bio set.")
    except Exception as e:
        logger.warning(f"Could not update profile for {session_name}: {e}")

    # Persist account in PostgreSQL
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO accounts (session_name, phone)
            VALUES ($1, $2)
            ON CONFLICT (session_name) DO UPDATE SET phone = EXCLUDED.phone, is_active = TRUE
            """,
            session_name,
            phone,
        )

    await client.disconnect()
    await event.respond(
        f"✅ **Account added successfully!**\n\n"
        f"📱 Phone: `{phone}`\n"
        f"👤 Name updated to: `{new_first}`\n"
        f"📝 Bio updated with ad attribution.",
        buttons=get_main_menu(),
    )

# ---------------------------------------------------------------------------
# CLI Account Auth Helper
# ---------------------------------------------------------------------------

async def cli_session_login() -> None:
    """Interactively authenticate and persist a Telethon session."""
    session_name = input("Enter a name for this session (e.g. main_account): ").strip()
    phone = input("Enter account phone number with country code: ").strip()

    client = TelegramClient(os.path.join(DATA_DIR, session_name), API_ID, API_HASH)
    await client.start(phone=phone)
    logger.info(f"Authorized successfully as '{session_name}'!")

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO accounts (session_name, phone)
            VALUES ($1, $2)
            ON CONFLICT (session_name) DO UPDATE SET phone = EXCLUDED.phone
            """,
            session_name,
            phone,
        )

    await client.disconnect()
    print(f"\n[+] Session '{session_name}' saved to '{DATA_DIR}/' and registered in PostgreSQL.")

# ---------------------------------------------------------------------------
# Application Entry Point
# ---------------------------------------------------------------------------

async def main() -> None:
    global db_pool, campaign_stop_event

    if not DATABASE_URL:
        logger.critical(
            "DATABASE_URL is not set. Please configure it in your .env file.\n"
            "Example: DATABASE_URL=postgresql://user:pass@localhost:5432/campaign_db"
        )
        sys.exit(1)

    # Bug-fix #3: Create asyncio.Event inside the running event loop
    campaign_stop_event = asyncio.Event()

    # Establish PostgreSQL connection pool
    logger.info("Connecting to PostgreSQL…")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    logger.info("PostgreSQL pool established.")

    await init_db(db_pool)

    if len(sys.argv) > 1 and sys.argv[1] == "--login":
        await cli_session_login()
        await db_pool.close()
        return

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