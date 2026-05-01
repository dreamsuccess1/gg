
import asyncio
import logging
import io
import os
import time
import re
from datetime import datetime, timedelta

import openpyxl
from telegram import (
    Update, Poll,
    InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, PollAnswerHandler,
    ConversationHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

import database as db
from pdf_generator import generate_result_pdf
from config import (
    BOT_TOKEN, ADMIN_IDS, BOT_NAME,
    BOT_USER, TARGET_TXT, TIMERS,
)
MAX_QUESTIONS_PER_SET = 500  # FIX #1

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Conversation States ──────────────────────────────────────────────────────
(
    MANUAL_QUESTION, MANUAL_OPTION_A, MANUAL_OPTION_B,
    MANUAL_OPTION_C, MANUAL_OPTION_D, MANUAL_CORRECT,
    MANUAL_EXPLANATION, MANUAL_TIMER, SET_NAME,
    BROADCAST_MSG, SCHEDULE_SET, SCHEDULE_TIME,
    RENAME_SET, SET_TIMER_VAL,
) = range(14)

# ── ✅ Auto-detect helper — parse ✅-marked question text ─────────────────────


def _normalize_checkmark(text: str) -> str:
    """✅️ (U+2705 + U+FE0F variation selector) ko ✅ (U+2705) mein normalize karo."""
    return text.replace("\u2705\uFE0F", "\u2705")


def parse_checkmark_question(text: str):
    """
    Question text parse karta hai jisme kisi option mein ✅ laga ho.

    Supported formats:
      - (A) text ✅  /  A. text ✅  /  A) text ✅
      - 1. text ✅  /  1) text ✅
      - plain text ✅  (bina prefix ke)
      - plain text ✅️  (variation selector ke saath)

    Returns: (question_str, options_list, correct_index)  ya  None
    """
    # ✅️ (with variation selector U+FE0F) aur ✅ dono normalize karo
    text = _normalize_checkmark(text)

    if "\u2705" not in text:
        return None

    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if len(lines) < 3:
        return None

    # Matches: (A) / A. / A) / 1. / 1) etc.
    OPT_PREFIX = re.compile(r"^(?:[(]?[A-Ea-e1-4][).]\s*|[A-Ea-e1-4]\.\s*)")

    def has_checkmark(line: str) -> bool:
        return "\u2705" in line

    def clean_option(line: str) -> str:
        line = OPT_PREFIX.sub("", line).strip()
        line = line.replace("\u2705", "").strip()
        return line

    # ✅ wali line ka index
    ck_idx = next((i for i, l in enumerate(lines) if has_checkmark(l)), None)
    if ck_idx is None:
        return None

    # Check karo: kya options mein prefix hai?
    has_prefix = bool(OPT_PREFIX.match(lines[ck_idx]))
    # Upar ki ek-do lines bhi check karo
    if not has_prefix and ck_idx > 0:
        has_prefix = bool(OPT_PREFIX.match(lines[ck_idx - 1]))

    if has_prefix:
        # Prefix mode: sirf prefix wali ya ✅ wali lines options hain
        def is_option_line(line: str) -> bool:
            return bool(OPT_PREFIX.match(line)) or has_checkmark(line)
    else:
        # Prefix-less mode: ✅ wali line ke aas-paas saari non-question lines options hain
        # Heuristic: question last wali long line hai jo '?' pe khatam ho ya pehli line ho
        # Approach: pehli line se shuru karke jab tak lines "question-like" hain skip karo
        # Simple: ✅ se pehle ki saari lines options banao jab tak last line '?' pe khatam ho
        # Best approach: ✅ ke upar ki lines mein se pehchanein kaunsi question hain
        # Agar sirf ek line ✅ se pehle hai → woh question
        # Agar multiple hain → pehli wali question, baaki options
        def is_option_line(line: str) -> bool:
            # Prefix-less mein sab kuch option hai (question baad mein separate karo)
            return True

    opt_start = ck_idx
    opt_end   = ck_idx

    while opt_start > 0 and is_option_line(lines[opt_start - 1]):
        opt_start -= 1
    while opt_end < len(lines) - 1 and is_option_line(lines[opt_end + 1]):
        opt_end += 1

    if not has_prefix:
        # Prefix-less: pehli line question hai, baaki options
        # opt_start ab 0 par hoga, question = lines[0]
        # Lekin agar question multi-line ho sakta hai:
        # Jo bhi lines '?' pe khatam hoti hain ya '।' pe khatam hoti hain woh question ka hissa
        question_end = 0
        for i in range(len(lines) - 1):
            line = lines[i]
            if line.endswith("?") or line.endswith("।") or line.endswith("?"):
                question_end = i
                break
            # Agar koi line bahut lambi hai (>30 chars) aur '✅' nahi, likely question
            if len(line) > 30 and not has_checkmark(line):
                question_end = i
            else:
                break
        opt_start = question_end + 1
        opt_lines = lines[opt_start:opt_end + 1]
        question  = " ".join(lines[:opt_start]).strip()
    else:
        opt_lines = lines[opt_start:opt_end + 1]
        question  = " ".join(lines[:opt_start]).strip()

    if len(opt_lines) < 2:
        return None
    if not question:
        return None

    correct_idx = None
    clean_opts  = []
    for i, opt in enumerate(opt_lines):
        if has_checkmark(opt):
            correct_idx = i
        clean_opts.append(clean_option(opt))

    if correct_idx is None:
        return None

    return question, clean_opts, correct_idx

# ── Shared: build set-selector keyboard ──────────────────────────────────────

def _set_selector_kb(prefix: str) -> InlineKeyboardMarkup:
    sets = db.get_all_sets()
    btns = []
    for s in sets:
        btns.append([InlineKeyboardButton(
            f"📂 {s['name']} ({s['count']} सवाल)",
            callback_data=f"{prefix}_{s['id']}"
        )])
    btns.append([InlineKeyboardButton("➕ नया Set बनाएं", callback_data=f"{prefix}_new")])
    btns.append([InlineKeyboardButton("❌ Cancel",         callback_data=f"{prefix}_cancel")])
    return InlineKeyboardMarkup(btns)

# ── Global Poll Registry ─────────────────────────────────────────────────────
POLL_TO_CHAT: dict = {}

# Per-user lock: ek user ke messages ek-ek karke process honge
import asyncio as _asyncio
_AQ_LOCKS: dict = {}

# ── Helpers ──────────────────────────────────────────────────────────────────

def is_admin(uid: int) -> bool:
    return int(uid) in [int(a) for a in ADMIN_IDS]

def fmt_time(sec: float) -> str:
    m, s = divmod(int(sec), 60)
    return f"{m}m {s}s"

def calc_acc(correct: int, total: int) -> int:
    return round((correct / total) * 100) if total > 0 else 0

def timer_kb():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"⏱ {t}s", callback_data=f"timer_{t}")
        for t in TIMERS
    ]])

def option_kb(options: list, prefix="correct"):
    labels = ["A","B","C","D"]
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"{labels[i]}: {str(o)[:20]}",
            callback_data=f"{prefix}_{i}"
        )
    ] for i, o in enumerate(options)])

def sets_kb(sets: list, prefix="startset") -> InlineKeyboardMarkup:
    btns = []
    for s in sets:
        lock = "🔒 " if s.get("is_private") else ""
        btns.append([InlineKeyboardButton(
            f"{lock}{s['name']} ({s['count']} सवाल)",
            callback_data=f"{prefix}_{s['id']}"
        )])
    return InlineKeyboardMarkup(btns)

# ── /start ───────────────────────────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if db.is_banned(user.id):
        await update.message.reply_text("❌ आप banned हैं।")
        return
    db.register_user(user.id, user.full_name, user.username)

    if is_admin(user.id):
        text = (
            f"🎯 *{BOT_NAME} — Admin Panel*\n\n"
            "🔧 *Set Management:*\n"
            "/sets — Sets dekhein aur quiz shuru karein\n"
            "/addquestion — ✅ wale questions add karein\n"
            "/manageset — Set rename/delete/shuffle\n"
            "/newquiz — Manually question add karein\n"
            "/bulkupload — Excel upload\n"
            "/txtupload — TXT file upload\n\n"
            "🚀 *Quiz:*\n"
            "/startquiz — Quiz shuru karein\n"
            "/stopquiz — Quiz rokein\n"
            "/schedule — Quiz schedule karein\n"
            "/schedules — Scheduled quizzes\n\n"
            "📊 *Stats:*\n"
            "/leaderboard — Rankings\n"
            "/myrank — Apni rank\n"
            "/stats — Bot stats\n"
            "/resetscores — Scores reset\n\n"
            "👥 *Users:*\n"
            "/broadcast — Sabko message\n"
            "/ban — User ban\n"
            "/unban — User unban"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    else:
        # User ko sets dikhao — seedha quiz choose kar sake
        sets = db.get_all_sets()
        if not sets:
            await update.message.reply_text(
                f"🎯 *{BOT_NAME} mein swagat hai!*\n\n"
                "Abhi koi Quiz Set available nahi hai।\n"
                "Thodi der mein dobara try karein।",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        btns = []
        for s in sets:
            btns.append([InlineKeyboardButton(
                f"▶️ {s['name']}  ({s['count']} सवाल)",
                callback_data=f"userquiz_{s['id']}"
            )])
        await update.message.reply_text(
            f"🎯 *{BOT_NAME} mein swagat hai, {user.first_name}!*\n\n"
            "📚 Neeche se apna Quiz Set chunein aur shuru karein:\n\n"
            "_PDF result quiz khatam hone ke baad milega।_",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode=ParseMode.MARKDOWN
        )

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await start(update, ctx)

# ── /myrank ──────────────────────────────────────────────────────────────────

async def my_rank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    rank = db.get_user_rank(chat_id, user.id)
    if not rank:
        await update.message.reply_text(
            "आपने अभी कोई quiz नहीं दी। Quiz दें और rank पाएं! 🎯"
        )
        return
    acc = calc_acc(rank["correct"], rank["correct"] + rank["wrong"])
    await update.message.reply_text(
        f"📊 *आपकी Rank*\n\n"
        f"👤 {rank['name']}\n"
        f"🏆 Rank: #{rank['rank']}\n"
        f"💯 Total Score: {rank['score']}\n"
        f"✅ Correct: {rank['correct']}\n"
        f"❌ Wrong: {rank['wrong']}\n"
        f"🎯 Accuracy: {acc}%\n"
        f"📚 Quizzes दी: {rank['quizzes']}",
        parse_mode=ParseMode.MARKDOWN
    )

# ── /stats ───────────────────────────────────────────────────────────────────

async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    s = db.get_global_stats()
    await update.message.reply_text(
        f"📊 *Bot Stats*\n\n"
        f"👥 Total Users: {s['users']}\n"
        f"📚 Total Sets: {s['sets']}\n"
        f"❓ Total Questions: {s['questions']}\n"
        f"📝 Total Answers: {s['answers']}\n"
        f"🤖 Bot: {BOT_NAME}\n"
        f"⏰ Time: {datetime.now().strftime('%d %b %Y, %I:%M %p')}",
        parse_mode=ParseMode.MARKDOWN
    )

# ── /ban /unban ──────────────────────────────────────────────────────────────

async def ban_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    try:
        uid = int(ctx.args[0])
        db.ban_user(uid)
        await update.message.reply_text(f"✅ User {uid} banned।")
    except:
        await update.message.reply_text("❌ Invalid user ID।")

async def unban_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    try:
        uid = int(ctx.args[0])
        db.unban_user(uid)
        await update.message.reply_text(f"✅ User {uid} unbanned।")
    except:
        await update.message.reply_text("❌ Invalid user ID।")

# ── /broadcast ───────────────────────────────────────────────────────────────

async def broadcast_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    await update.message.reply_text(
        "📢 *Broadcast Message*\n\n"
        "वो message टाइप करें जो सभी users को भेजना है:\n\n"
        "/cancel — रद्द करें",
        parse_mode=ParseMode.MARKDOWN
    )
    return BROADCAST_MSG

async def broadcast_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg_text = update.message.text
    users    = db.get_all_users()
    sent, failed = 0, 0

    status_msg = await update.message.reply_text(
        f"📤 Sending to {len(users)} users..."
    )

    for user in users:
        try:
            await ctx.bot.send_message(
                chat_id    = user["id"],
                text       = f"📢 *{BOT_NAME}*\n\n{msg_text}",
                parse_mode = ParseMode.MARKDOWN
            )
            sent += 1
            await asyncio.sleep(0.05)
        except TelegramError:
            failed += 1

    await status_msg.edit_text(
        f"✅ *Broadcast पूरा!*\n\n"
        f"✔️ Sent: {sent}\n"
        f"❌ Failed: {failed}",
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END

# ── /schedule ────────────────────────────────────────────────────────────────

async def schedule_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    sets = db.get_all_sets()
    if not sets:
        await update.message.reply_text("कोई Set नहीं। /newquiz से बनाएं।")
        return ConversationHandler.END
    await update.message.reply_text(
        "⏰ *Quiz Schedule करें*\n\nकौन सा Set schedule करना है?",
        reply_markup=sets_kb(sets, prefix="schedset"),
        parse_mode=ParseMode.MARKDOWN
    )
    return SCHEDULE_SET

async def schedule_set_chosen(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.user_data["sched_set_id"] = int(query.data.split("_")[1])
    await query.message.reply_text(
        "📅 कब चलाएं? Format: `DD/MM/YYYY HH:MM`\n\nExample: `30/04/2026 08:00`",
        parse_mode=ParseMode.MARKDOWN
    )
    return SCHEDULE_TIME

async def schedule_time_set(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        run_at = datetime.strptime(text, "%d/%m/%Y %H:%M")
        if run_at < datetime.now():
            await update.message.reply_text("❌ यह time पहले ही गुज़र चुका है। Future time दें।")
            return SCHEDULE_TIME
        set_id  = ctx.user_data["sched_set_id"]
        chat_id = update.effective_chat.id
        db.schedule_quiz(chat_id, set_id, run_at.strftime("%Y-%m-%d %H:%M"), update.effective_user.id)
        set_info = db.get_set(set_id)
        await update.message.reply_text(
            f"✅ *Quiz Scheduled!*\n\n"
            f"📚 Set: {set_info['name']}\n"
            f"⏰ Time: {run_at.strftime('%d %b %Y, %I:%M %p')}",
            parse_mode=ParseMode.MARKDOWN
        )
    except ValueError:
        await update.message.reply_text("❌ Format गलत है। Example: `30/04/2026 08:00`")
        return SCHEDULE_TIME
    ctx.user_data.clear()
    return ConversationHandler.END

async def list_schedules(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    scheds = db.get_all_schedules(update.effective_chat.id)
    if not scheds:
        await update.message.reply_text("कोई scheduled quiz नहीं है।")
        return
    text = "⏰ *Scheduled Quizzes:*\n\n"
    btns = []
    for s in scheds:
        run_at = datetime.strptime(s["run_at"], "%Y-%m-%d %H:%M")
        text  += f"📚 {s['set_name']} — {run_at.strftime('%d %b, %I:%M %p')}\n"
        btns.append([InlineKeyboardButton(
            f"❌ Cancel: {s['set_name']}",
            callback_data=f"delsched_{s['id']}"
        )])
    await update.message.reply_text(
        text, reply_markup=InlineKeyboardMarkup(btns),
        parse_mode=ParseMode.MARKDOWN
    )

async def delete_schedule_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    sched_id = int(query.data.split("_")[1])
    db.delete_schedule(sched_id)
    await query.message.edit_text("✅ Schedule cancel हो गया।")

# ── /manageset ───────────────────────────────────────────────────────────────

async def manage_set_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    sets = db.get_all_sets()
    if not sets:
        await update.message.reply_text("कोई Set नहीं।")
        return
    await update.message.reply_text(
        "🔧 *Set Manage करें — कौन सा Set?*",
        reply_markup=sets_kb(sets, prefix="mgset"),
        parse_mode=ParseMode.MARKDOWN
    )

async def sets_addq_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """'/sets' se 'Questions Add' button → /addquestion jaisi flow start karo."""
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    ctx.user_data.clear()
    ctx.user_data["aq_mode"] = True
    sets = db.get_all_sets()
    if sets:
        kb = _set_selector_kb("aqpreset")
        await query.message.edit_text(
            "\U0001f4c2 *Pehle Set choose karein jisme questions save honge:*",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        ctx.user_data["aq_waiting_presetname"] = True
        await query.message.edit_text(
            "\U0001f4dd Koi set nahi mila। Naye Set ka naam type karein:"
        )

async def manage_set_chosen(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        set_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.message.edit_text("\u274c Invalid. /manageset dobara karein.")
        return
    set_info = db.get_set(set_id)
    if not set_info:
        await query.message.edit_text("❌ Set नहीं मिला।")
        return

    qs    = db.get_questions(set_id)
    btns  = [
        [InlineKeyboardButton("\u2795 Questions Add karo",  callback_data=f"mgaddq_{set_id}")],
        [InlineKeyboardButton("\U0001f500 Shuffle karo",    callback_data=f"shuffle_{set_id}")],
        [InlineKeyboardButton("\u270f\ufe0f Rename karo",   callback_data=f"renameset_{set_id}")],
        [InlineKeyboardButton("\u23f1 Timer badlo",         callback_data=f"settimer_{set_id}")],
        [InlineKeyboardButton("\U0001f5d1 Set Delete karo", callback_data=f"delset_{set_id}")],
        [InlineKeyboardButton("\u25b6\ufe0f Quiz shuru karo", callback_data=f"startset_{set_id}")],
    ]
    await query.message.edit_text(
        f"🔧 *{set_info['name']}*\n\n"
        f"❓ सवाल: {len(qs)}\n\n"
        f"क्या करना है?",
        reply_markup=InlineKeyboardMarkup(btns),
        parse_mode=ParseMode.MARKDOWN
    )

async def mgaddq_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Manage Set → Questions Add karo — preset set set karke aq_mode shuru karo."""
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    set_id   = int(query.data.split("_")[1])
    set_info = db.get_set(set_id)
    ctx.user_data.clear()
    ctx.user_data["aq_mode"]       = True
    ctx.user_data["aq_preset_set"] = set_id
    await query.message.edit_text(
        f"\u2705 Set: *{set_info['name']}*\n\n"
        f"\U0001f4cc *Ab question bhejein* jisme sahi option ke aage *\u2705* laga ho:\n\n"
        f"`Question?\nOption1\nOption2\u2705\nOption3\nOption4`\n\n"
        f"_Ek ke baad ek bhejte jaein \u2014 seedha save hote jayenge\u0964 /done \u2014 khatam karein\u0964_",
        parse_mode=ParseMode.MARKDOWN
    )

async def shuffle_set_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    set_id = int(query.data.split("_")[1])
    db.shuffle_set(set_id)
    await query.message.edit_text("✅ Set shuffle हो गया!")

async def rename_set_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    set_id = int(query.data.split("_")[1])
    ctx.user_data["rename_set_id"] = set_id
    await query.message.reply_text("नया नाम टाइप करें:")
    return RENAME_SET

async def rename_set_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    new_name = update.message.text.strip()
    set_id   = ctx.user_data.get("rename_set_id")
    if set_id:
        db.rename_set(set_id, new_name)
        await update.message.reply_text(f"✅ Set का नाम बदलकर *{new_name}* हो गया!", parse_mode=ParseMode.MARKDOWN)
    ctx.user_data.clear()
    return ConversationHandler.END

async def settimer_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    set_id = int(query.data.split("_")[1])
    ctx.user_data["timer_set_id"] = set_id
    await query.message.reply_text(
        "नया timer चुनें (पूरे set के लिए):",
        reply_markup=timer_kb()
    )
    return SET_TIMER_VAL

async def settimer_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    timer  = int(query.data.split("_")[1])
    set_id = ctx.user_data.get("timer_set_id")
    if set_id:
        db.update_question_timer(set_id, timer)
        await query.message.edit_text(f"✅ पूरे Set का timer {timer}s हो गया!")
    ctx.user_data.clear()
    return ConversationHandler.END

async def delete_set_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    set_id = int(query.data.split("_")[1])
    db.delete_set(set_id)
    await query.message.edit_text("✅ Set delete हो गया।")

# ── Manual Question Creation ─────────────────────────────────────────────────

async def newquiz_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    ctx.user_data.clear()
    await update.message.reply_text(
        "📝 *नया सवाल बनाएं*\n\n"
        "सवाल टाइप करें\n"
        "_(Photo के साथ — photo भेजें, caption में सवाल)_\n\n"
        "/cancel — रद्द करें",
        parse_mode=ParseMode.MARKDOWN
    )
    return MANUAL_QUESTION

async def recv_question(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.photo:
        ctx.user_data["photo_id"] = msg.photo[-1].file_id
        ctx.user_data["question"] = msg.caption or ""
    else:
        ctx.user_data["question"] = msg.text.strip()
    await msg.reply_text("✅ सवाल मिला!\n\n*Option A* टाइप करें:", parse_mode=ParseMode.MARKDOWN)
    return MANUAL_OPTION_A

async def recv_option_a(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["options"] = [update.message.text.strip()]
    await update.message.reply_text("*Option B* टाइप करें:", parse_mode=ParseMode.MARKDOWN)
    return MANUAL_OPTION_B

async def recv_option_b(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["options"].append(update.message.text.strip())
    await update.message.reply_text("*Option C* टाइप करें:", parse_mode=ParseMode.MARKDOWN)
    return MANUAL_OPTION_C

async def recv_option_c(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["options"].append(update.message.text.strip())
    await update.message.reply_text("*Option D* टाइप करें:", parse_mode=ParseMode.MARKDOWN)
    return MANUAL_OPTION_D

async def recv_option_d(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["options"].append(update.message.text.strip())
    await update.message.reply_text(
        "✅ चारों options मिले!\n\nसही जवाब चुनें:",
        reply_markup=option_kb(ctx.user_data["options"]),
        parse_mode=ParseMode.MARKDOWN
    )
    return MANUAL_CORRECT

async def recv_correct(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.user_data["correct"] = int(query.data.split("_")[1])
    await query.message.reply_text(
        "📖 Explanation लिखें:\n_(नहीं चाहिए तो /skip करें)_",
        parse_mode=ParseMode.MARKDOWN
    )
    return MANUAL_EXPLANATION

async def recv_explanation(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    ctx.user_data["explanation"] = "" if txt == "/skip" else txt
    await update.message.reply_text("⏱ Timer चुनें:", reply_markup=timer_kb())
    return MANUAL_TIMER

async def recv_timer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ctx.user_data["timer"] = int(query.data.split("_")[1])
    sets = db.get_all_sets()
    if sets:
        btns = [[InlineKeyboardButton(s["name"], callback_data=f"addtoset_{s['id']}")] for s in sets]
        btns.append([InlineKeyboardButton("➕ नया Set", callback_data="newset")])
        await query.message.reply_text("किस Set में जोड़ें?", reply_markup=InlineKeyboardMarkup(btns))
    else:
        await query.message.reply_text("नए Set का नाम टाइप करें:")
    return SET_NAME

async def recv_set_choice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "newset":
        await query.message.reply_text("📝 नए Set का नाम टाइप करें:")
        ctx.user_data["waiting_newset"] = True
        return SET_NAME
    # addtoset_<id>
    try:
        set_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.message.reply_text("❌ Invalid। /newquiz फिर से करें।")
        return ConversationHandler.END
    return await _save_question(query.message, ctx, set_id)

async def recv_set_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name   = update.message.text.strip()
    set_id = db.create_set(name, owner_id=update.effective_user.id)
    return await _save_question(update.message, ctx, set_id)

async def _save_question(msg, ctx, set_id: int):
    d = ctx.user_data
    db.add_question(
        set_id=set_id, question=d.get("question",""),
        options=d.get("options",[]), correct=d.get("correct",0),
        explanation=d.get("explanation",""),
        timer=d.get("timer",20), photo_id=d.get("photo_id"),
    )
    await msg.reply_text("✅ *सवाल save हो गया!*", parse_mode=ParseMode.MARKDOWN)
    ctx.user_data.clear()
    return ConversationHandler.END


# ── /addquestion — ✅-marked question se auto save ───────────────────────────

async def addquestion_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin /addquestion bheje → pehle set choose karo, phir ✅ wala question bhejo."""
    if not is_admin(update.effective_user.id):
        return
    ctx.user_data.clear()
    ctx.user_data["aq_mode"] = True

    sets = db.get_all_sets()
    if sets:
        kb = _set_selector_kb("aqpreset")
        await update.message.reply_text(
            "📂 *Pehle Set choose karein jisme questions save honge:*",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        ctx.user_data["aq_waiting_setname"] = True
        await update.message.reply_text(
            "📝 Koi set nahi mila। Naye Set ka naam type karein:",
            parse_mode=ParseMode.MARKDOWN
        )

async def addquestion_done(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Agar koi pending question pada hai toh pehle save karo
    pending_q      = ctx.user_data.get("aq_q")
    pending_set    = ctx.user_data.get("aq_preset_set")
    if pending_q and pending_set:
        await _do_save_aq(update.message, ctx, pending_set)
    if any(ctx.user_data.get(k) for k in (
        "aq_mode","aq_preset_set","aq_waiting_setname","aq_waiting_presetname","aq_waiting_fwdsetname"
    )):
        ctx.user_data.clear()
        await update.message.reply_text("\u2705 Auto-Save mode band.")
    else:
        await update.message.reply_text("Koi active mode nahi.")

async def handle_aq_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """✅ wala text — sirf aq_mode active hone par hi kaam karo."""
    if not is_admin(update.effective_user.id):
        return
    uid = update.effective_user.id
    if uid not in _AQ_LOCKS:
        _AQ_LOCKS[uid] = _asyncio.Lock()
    async with _AQ_LOCKS[uid]:
        await _handle_aq_inner(update, ctx)

async def _handle_aq_inner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Actual handler — per-user lock ke andar chalta hai."""
    # ── Guard: agar user /newquiz conversation mein hai to yahan kuch mat karo ──
    conv_keys = {"question", "options", "correct", "explanation", "timer",
                 "waiting_newset", "photo_id"}
    if any(k in ctx.user_data for k in conv_keys):
        return

    # FIX #5: Admin ke liye always-on — /addquestion ke bina bhi ✅ text detect ho
    # Agar koi aq state active nahi hai, toh bhi ✅ check karo
    aq_active = (
        ctx.user_data.get("aq_mode") or
        ctx.user_data.get("aq_preset_set") or
        ctx.user_data.get("aq_waiting_setname") or
        ctx.user_data.get("aq_waiting_presetname") or
        ctx.user_data.get("aq_waiting_fwdsetname")
    )
    # Always check ✅ for admin — aq_active sirf flow control ke liye

    # If waiting for new set name at /addquestion start
    if ctx.user_data.get("aq_waiting_presetname"):
        name   = update.message.text.strip()
        set_id = db.create_set(name, owner_id=update.effective_user.id)
        ctx.user_data.pop("aq_waiting_presetname", None)
        ctx.user_data["aq_preset_set"] = set_id
        await update.message.reply_text(
            f"\u2705 Set *{name}* ban gaya!\n\n"
            f"\U0001f4cc *Ab question bhejein* jisme sahi option ke aage *\u2705* laga ho:\n\n"
            f"_/done \u2014 khatam karein_",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    # If waiting for set name after choosing "new set" (per-question)
    if ctx.user_data.get("aq_waiting_setname"):
        name   = update.message.text.strip()
        set_id = db.create_set(name, owner_id=update.effective_user.id)
        ctx.user_data.pop("aq_waiting_setname", None)
        ctx.user_data["aq_preset_set"] = set_id
        await _do_save_aq(update.message, ctx, set_id)
        return
    if ctx.user_data.get("aq_waiting_fwdsetname"):
        name   = update.message.text.strip()
        set_id = db.create_set(name, owner_id=update.effective_user.id)
        ctx.user_data.pop("aq_waiting_fwdsetname", None)
        await _do_save_fwd(update.message, ctx, set_id)
        return
    # ✅ sirf tab process karo jab aq_mode active ho
    if not aq_active:
        return
    text = update.message.text or update.message.caption or ""
    text = _normalize_checkmark(text)   # ✅️ → ✅ (unicode-safe)
    if "\u2705" not in text:
        return
    parsed = parse_checkmark_question(text)
    if not parsed:
        # Sirf aq_mode mein error dikhao, warna silently return
        if aq_active:
            await update.message.reply_text(
                "⚠️ Format detect नहीं हुआ।\n"
                "Question alag line mein aur sahi option pe ✅ laga hona chahiye।\n\n"
                "Example:\n`सवाल?\nOption A\nOption B✅\nOption C\nOption D`"
            )
        return
    question, options, correct_idx = parsed
    labels = ["A","B","C","D","E"]
    opts_preview = "\n".join(
        f"{'✅' if i==correct_idx else '➖'} {labels[i] if i<len(labels) else i+1}: {o}"
        for i, o in enumerate(options)
    )
    ctx.user_data["aq_q"]       = question
    ctx.user_data["aq_opts"]    = options
    ctx.user_data["aq_correct"] = correct_idx
    ctx.user_data["aq_photo"]   = None

    # Agar preset set already choose hua hai (/addquestion flow) → seedha save
    preset_set = ctx.user_data.get("aq_preset_set")
    if preset_set:
        await _do_save_aq(update.message, ctx, preset_set)
        return

    # Warna per-question set selector dikhaao
    sets = db.get_all_sets()
    if sets:
        kb = _set_selector_kb("aqset")
        await update.message.reply_text(
            f"✅ *Question detect हुआ!*\n\n"
            f"❓ {question}\n\n"
            f"{opts_preview}\n\n"
            f"📂 *किस Set में save करें?*",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        ctx.user_data["aq_waiting_setname"] = True
        await update.message.reply_text(
            f"✅ *Question detect हुआ!*\n\n"
            f"❓ {question}\n\n"
            f"{opts_preview}\n\n"
            f"📝 नए Set का नाम टाइप करें:",
            parse_mode=ParseMode.MARKDOWN
        )

async def aqpreset_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Pehle set choose karo (/addquestion start mein) — phir question bhejne ka instruction do."""
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    data = query.data  # aqpreset_<id> | aqpreset_new | aqpreset_cancel
    if data == "aqpreset_cancel":
        ctx.user_data.clear()
        await query.message.edit_text("❌ /addquestion cancel kiya।")
        return
    if data == "aqpreset_new":
        ctx.user_data["aq_waiting_presetname"] = True
        await query.message.edit_text("📝 Naye Set ka naam type karein:")
        return
    try:
        set_id = int(data.split("_")[1])
    except (IndexError, ValueError):
        await query.message.edit_text("\u274c Invalid callback. /addquestion dobara karein.")
        return
    set_info = db.get_set(set_id)
    if not set_info:
        await query.message.edit_text("\u274c Set nahi mila. /addquestion dobara karein.")
        return
    ctx.user_data["aq_preset_set"] = set_id
    await query.message.edit_text(
        f"✅ Set select hua: *{set_info['name']}*\n\n"
        f"📌 *Ab question bhejein* jisme sahi option ke aage *✅* laga ho:\n\n"
        f"Example:\n"
        f"`भारत की राजधानी?\nमुंबई\nदिल्ली✅\nकोलकाता\nचेन्नई`\n\n"
        f"_Ek ke baad ek bhejte jaein। /done — khatam karein।_",
        parse_mode=ParseMode.MARKDOWN
    )

async def aqset_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Set selection for /addquestion flow (per-question set choose)."""
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    data = query.data  # aqset_<id> | aqset_new | aqset_cancel
    if data == "aqset_cancel":
        ctx.user_data.pop("aq_q", None)
        ctx.user_data.pop("aq_opts", None)
        ctx.user_data.pop("aq_correct", None)
        await query.message.edit_text("❌ इस question को skip किया।\n\nअगला question भेजें या /done करें।")
        return
    if data == "aqset_new":
        ctx.user_data["aq_waiting_setname"] = True
        await query.message.edit_text("📝 नए Set का नाम टाइप करें:")
        return
    set_id = int(data.split("_")[1])
    await _do_save_aq(query.message, ctx, set_id)

async def _do_save_aq(msg, ctx, set_id: int):
    q       = ctx.user_data.get("aq_q", "")
    opts    = ctx.user_data.get("aq_opts", [])
    correct = ctx.user_data.get("aq_correct", 0)
    photo   = ctx.user_data.get("aq_photo")
    if not q or not opts:
        await msg.reply_text("\u26a0\ufe0f Question data missing. Dobara bhejein.")
        return
    db.add_question(
        set_id=set_id, question=q, options=opts,
        correct=correct, explanation="", timer=20, photo_id=photo
    )
    set_info = db.get_set(set_id)
    labels   = ["A","B","C","D","E"]
    # Clear question data, keep aq_mode and preset_set for next question
    for k in ["aq_q","aq_opts","aq_correct","aq_photo"]:
        ctx.user_data.pop(k, None)
    correct_label = labels[correct] if correct < len(labels) else str(correct+1)
    correct_text  = opts[correct] if correct < len(opts) else "?"
    set_name      = set_info["name"] if set_info else str(set_id)
    text = (
        f"\u2705 *\u0938\u0935\u093e\u0932 save \u0939\u094b \u0917\u092f\u093e!*\n\n"
        f"\u2753 {q}\n"
        f"\u2714\ufe0f \u0938\u0939\u0940 \u091c\u0935\u093e\u092c: *{correct_label}: {correct_text}*\n"
        f"\U0001f4c2 Set: *{set_name}*\n\n"
        f"_\u0905\u0917\u0932\u093e question \u092d\u0947\u091c\u0947\u0902 \u092f\u093e /done \u0915\u0930\u0947\u0902\u0964_"
    )
    try:
        await msg.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await msg.reply_text("\u2705 Saved! Agla question bhejein ya /done karein.")

# ── Forwarded Poll → set selector ────────────────────────────────────────────

async def handle_forwarded_poll_new(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    msg  = update.message
    poll = msg.poll
    if not poll:
        return
    if poll.type != Poll.QUIZ:
        await msg.reply_text("⚠️ Sirf Quiz polls forward karein।")
        return
    if poll.correct_option_id is None:
        await msg.reply_text("⚠️ Is poll mein sahi jawab nahi hai।")
        return
    question = re.sub(r'\[\d+/\d+\]', '', poll.question).strip()
    options  = [o.text for o in poll.options]
    correct  = poll.correct_option_id
    expl     = poll.explanation or ""
    labels   = ["A","B","C","D"]
    opts_preview = "\n".join(
        f"{'✅' if i==correct else '➖'} {labels[i]}: {o}"
        for i, o in enumerate(options)
    )
    ctx.user_data["fwd_q"]       = question
    ctx.user_data["fwd_opts"]    = options
    ctx.user_data["fwd_correct"] = correct
    ctx.user_data["fwd_expl"]    = expl
    await msg.reply_text(
        f"📋 *Forwarded Poll!*\n\n"
        f"❓ {question}\n\n"
        f"{opts_preview}\n\n"
        f"📂 *किस Set में save करें?*",
        reply_markup=_set_selector_kb("fwdset"),
        parse_mode=ParseMode.MARKDOWN
    )

async def fwdset_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        return
    data = query.data
    if data == "fwdset_cancel":
        ctx.user_data.pop("fwd_q", None)
        ctx.user_data.pop("fwd_opts", None)
        ctx.user_data.pop("fwd_correct", None)
        await query.message.edit_text("\u274c Skip kiya.")
        return
    if data == "fwdset_new":
        ctx.user_data["aq_waiting_fwdsetname"] = True
        await query.message.edit_text("\U0001f4dd Naye Set ka naam type karein:")
        return
    # FIX #6: "fwdset_new" aur "fwdset_cancel" upar handle ho chuke hain
    # Ab sirf numeric ID wale cases bache hain
    try:
        parts = data.split("_")
        set_id = int(parts[1])
    except (IndexError, ValueError):
        await query.message.edit_text("\u274c Invalid. Dobara try karein.")
        return
    await _do_save_fwd(query.message, ctx, set_id)

async def _do_save_fwd(msg, ctx, set_id: int):
    q       = ctx.user_data.get("fwd_q","")
    opts    = ctx.user_data.get("fwd_opts",[])
    correct = ctx.user_data.get("fwd_correct",0)
    expl    = ctx.user_data.get("fwd_expl","")
    db.add_question(
        set_id=set_id, question=q, options=opts,
        correct=correct, explanation=expl, timer=20
    )
    set_info = db.get_set(set_id)
    labels   = ["A","B","C","D"]
    for k in ["fwd_q","fwd_opts","fwd_correct","fwd_expl"]:
        ctx.user_data.pop(k, None)
    text = (
        f"✅ *Poll save हो गया!*\n\n"
        f"❓ {q}\n"
        f"✔️ सही: *{labels[correct]}: {opts[correct]}*\n"
        f"📂 Set: *{set_info['name']}*\n\n"
        f"_अगला poll भेजें या /done करें।_"
    )
    try:
        await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await msg.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def cancel_conv(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "❌ *रद्द कर दिया गया।*\n\nकुछ और करना हो तो /start करें।",
        parse_mode=ParseMode.MARKDOWN
    )
    return ConversationHandler.END

# ── Poll Forwarding — moved above (handle_forwarded_poll_new) ──────────────────

# ── TXT File Import ───────────────────────────────────────────────────────────

async def txt_upload_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "📄 *TXT File Upload*\n\n"
        "Format:\n"
        "```\n"
        "Q: सवाल यहाँ\n"
        "A: Option A\n"
        "B: Option B\n"
        "C: Option C\n"
        "D: Option D\n"
        "Ans: B\n"
        "Exp: Explanation यहाँ\n"
        "```\n\n"
        "अब .txt file भेजें:",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_txt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    doc = update.message.document
    if not doc or not (doc.file_name.endswith(".txt")):
        return

    await update.message.reply_text("⏳ TXT process हो रही है...")
    file = await ctx.bot.get_file(doc.file_id)
    buf  = io.BytesIO()
    await file.download_to_memory(buf)
    content = buf.getvalue().decode("utf-8", errors="ignore")

    set_name = doc.file_name.replace(".txt","")
    set_id   = db.create_set(set_name, owner_id=update.effective_user.id)

    # Parse TXT
    blocks  = re.split(r'\n\s*\n', content.strip())
    count, errors = 0, 0
    labels_map = {"A":0,"B":1,"C":2,"D":3}

    for block in blocks:
        try:
            lines = {}
            for line in block.strip().split("\n"):
                if ":" in line:
                    key, val = line.split(":", 1)
                    lines[key.strip().upper()] = val.strip()

            q       = lines.get("Q","").strip()
            opts    = [lines.get("A","").strip(), lines.get("B","").strip(),
                       lines.get("C","").strip(), lines.get("D","").strip()]
            ans_key = lines.get("ANS","A").upper().strip()
            correct = labels_map.get(ans_key, 0)
            expl    = lines.get("EXP","").strip()

            # Skip if question ya koi bhi option missing
            if not q or not all(opts):
                errors += 1
                continue

            db.add_question(
                set_id=set_id, question=q, options=opts,
                correct=correct, explanation=expl, timer=20
            )
            count += 1
        except Exception as e:
            logger.warning(f"TXT block error: {e}")
            errors += 1

    await update.message.reply_text(
        f"✅ *TXT Upload पूरा!*\n"
        f"📂 {set_name}\n"
        f"✔️ {count} सवाल | ❌ {errors} errors",
        parse_mode=ParseMode.MARKDOWN
    )

# ── Bulk Excel Upload ─────────────────────────────────────────────────────────

async def bulk_upload_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "📊 *Excel Bulk Upload*\n\n"
        "Format: `Question|A|B|C|D|Correct(0-3)|Explanation|Timer`\n\n"
        "अब .xlsx file भेजें:",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_excel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    doc = update.message.document
    if not doc or not doc.file_name.endswith(".xlsx"):
        return

    await update.message.reply_text("⏳ Process हो रही है...")
    file = await ctx.bot.get_file(doc.file_id)
    buf  = io.BytesIO()
    await file.download_to_memory(buf)
    buf.seek(0)

    wb       = openpyxl.load_workbook(buf)
    ws       = wb.active
    set_name = doc.file_name.replace(".xlsx","")
    set_id   = db.create_set(set_name, owner_id=update.effective_user.id)
    count, errors = 0, 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        try:
            vals = list(row) + [None]*8
            q,a,b,c,d,correct,expl,timer = vals[:8]
            if not q:
                continue
            db.add_question(
                set_id=set_id, question=str(q),
                options=[str(a),str(b),str(c),str(d)],
                correct=int(correct),
                explanation=str(expl or ""),
                timer=int(timer or 20)
            )
            count += 1
        except Exception as e:
            logger.warning(f"Row error: {e}")
            errors += 1

    await update.message.reply_text(
        f"✅ *Upload पूरा!*\n📂 {set_name}\n✔️ {count} सवाल | ❌ {errors} errors",
        parse_mode=ParseMode.MARKDOWN
    )

# ── Quiz Engine ───────────────────────────────────────────────────────────────

async def list_sets(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sets = db.get_all_sets()
    if not sets:
        msg = "कोई Set नहीं।"
        if is_admin(update.effective_user.id):
            msg += "\n/addquestion — questions add karein\n/newquiz — manually banayein"
        await update.message.reply_text(msg)
        return
    if is_admin(update.effective_user.id):
        btns = []
        for s in sets:
            btns.append([
                InlineKeyboardButton(f"\u25b6\ufe0f {s['name']} ({s['count']})", callback_data=f"startset_{s['id']}"),
                InlineKeyboardButton("\u2699\ufe0f Manage", callback_data=f"mgset_{s['id']}"),
            ])
        btns.append([InlineKeyboardButton("\u2795 Questions Add karo", callback_data="sets_addq")])
        await update.message.reply_text(
            "\U0001f4da *Saare Quiz Sets:*",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        btns = [[InlineKeyboardButton(
            f"\u25b6\ufe0f {s['name']}  ({s['count']} \u0938\u0935\u093e\u0932)",
            callback_data=f"userquiz_{s['id']}"
        )] for s in sets]
        await update.message.reply_text(
            "\U0001f4da *Quiz Sets — chunein aur shuru karein:*",
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode=ParseMode.MARKDOWN
        )

async def startquiz_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await list_sets(update, ctx)

async def userquiz_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """User /start pe set choose kare → us chat mein quiz shuru karo."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    if db.is_banned(user.id):
        await query.message.edit_text("❌ Aap banned hain.")
        return
    try:
        set_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.message.edit_text("❌ Invalid. /start dobara karein.")
        return
    chat_id   = query.message.chat_id
    questions = db.get_questions(set_id)
    if not questions:
        await query.message.edit_text("❌ Is set mein koi sawaal nahi.")
        return

    # Agar is chat mein pehle se quiz chal rahi hai
    existing = ctx.chat_data.get("quiz")
    if existing and existing.get("active") and not existing.get("finished"):
        await query.message.edit_text("⚠️ Is chat mein pehle se quiz chal rahi hai!")
        return

    set_info = db.get_set(set_id)
    now_str  = datetime.now().strftime("%d %b %Y, %I:%M %p IST")
    quiz = {
        "questions"      : questions,
        "scores"         : {},
        "active"         : True,
        "finished"       : False,
        "poll_map"       : {},
        "start_times"    : {},
        "student_answers": {},
        "set_name"       : set_info["name"] if set_info else "Quiz",
        "quiz_date"      : now_str,
        "total_q"        : len(questions),
        "chat_id"        : chat_id,
        "started_by"     : user.full_name,
    }
    ctx.chat_data["quiz"] = quiz
    await query.message.edit_text(
        f"🚀 *Quiz शुरू!*\n"
        f"📚 {set_info['name'] if set_info else ''}\n"
        f"❓ {len(questions)} सवाल\n"
        f"👤 {user.first_name} ne shuru kiya\n\n"
        f"_PDF result quiz khatam hone ke baad milega!_",
        parse_mode=ParseMode.MARKDOWN
    )
    asyncio.create_task(run_quiz(ctx.bot, chat_id, quiz))

async def start_quiz_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin /sets ya /manageset se quiz start kare."""
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.answer("Sirf admin quiz start kar sakte hain.", show_alert=True)
        return
    try:
        set_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.message.reply_text("\u274c Invalid set. Dobara try karein.")
        return
    chat_id   = query.message.chat_id
    questions = db.get_questions(set_id)
    if not questions:
        await query.message.reply_text("❌ Set में कोई सवाल नहीं।")
        return
    set_info = db.get_set(set_id)
    now_str  = datetime.now().strftime("%d %b %Y, %I:%M %p IST")
    quiz = {
        "questions"      : questions,
        "scores"         : {},
        "active"         : True,
        "finished"       : False,
        "poll_map"       : {},
        "start_times"    : {},
        "student_answers": {},
        "set_name"       : set_info["name"] if set_info else "Quiz",
        "quiz_date"      : now_str,
        "total_q"        : len(questions),
        "chat_id"        : chat_id,
    }
    ctx.chat_data["quiz"] = quiz
    await query.message.reply_text(
        f"🚀 *Quiz शुरू!*\n"
        f"📚 {set_info['name']}\n"
        f"❓ {len(questions)} सवाल\n\n"
        f"_सभी students पहले bot को /start करें!_",
        parse_mode=ParseMode.MARKDOWN
    )
    asyncio.create_task(run_quiz(ctx.bot, chat_id, quiz))

async def run_quiz(bot, chat_id: int, quiz: dict):
    for idx, q in enumerate(quiz["questions"]):
        if not quiz.get("active"):
            break
        timer = q.get("timer", 20)
        if q.get("photo_id"):
            try:
                await bot.send_photo(
                    chat_id=chat_id, photo=q["photo_id"],
                    caption=f"❓ *Q{idx+1}:* {q['question']}",
                    parse_mode=ParseMode.MARKDOWN,
                    protect_content=True,
                )
            except TelegramError as e:
                logger.warning(f"Photo failed Q{idx+1}: {e}")
        try:
            sent = await bot.send_poll(
                chat_id=chat_id,
                question=f"Q{idx+1}: {q['question'][:295]}",
                options=[o[:100] for o in q["options"]],
                type=Poll.QUIZ,
                correct_option_id=q["correct"],
                explanation=(q.get("explanation","") or "")[:200] or None,
                open_period=timer,
                is_anonymous=False,
                protect_content=True,
            )
            poll_id = sent.poll.id
            quiz["poll_map"][poll_id]    = idx
            quiz["start_times"][poll_id] = time.time()
            POLL_TO_CHAT[poll_id]        = chat_id
        except TelegramError as e:
            logger.error(f"Poll failed Q{idx+1}: {e}")
            continue
        try:
            await asyncio.sleep(timer + 3)
        except asyncio.CancelledError:
            break
    if quiz.get("active") and not quiz.get("finished"):
        await finish_quiz(bot, chat_id, quiz)

async def handle_poll_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    answer  = update.poll_answer
    poll_id = answer.poll_id
    chat_id = POLL_TO_CHAT.get(poll_id)
    if not chat_id:
        return
    quiz = ctx.application.chat_data.get(chat_id, {}).get("quiz")
    if not quiz or poll_id not in quiz.get("poll_map", {}):
        return
    uid    = answer.user.id
    name   = answer.user.full_name
    idx    = quiz["poll_map"][poll_id]
    q      = quiz["questions"][idx]
    taken  = round(time.time() - quiz["start_times"].get(poll_id, time.time()), 1)
    chosen = answer.option_ids[0] if answer.option_ids else None
    if chosen is None:
        return  # User ne retract kiya
    correct= q["correct"]
    if uid not in quiz["scores"]:
        quiz["scores"][uid] = {"name":name,"score":0,"correct":0,"wrong":0,"time":0.0,"answered":0}
    e = quiz["scores"][uid]
    is_correct = (chosen == correct)
    if is_correct:
        e["score"]   += 1
        e["correct"] += 1
    else:
        e["wrong"] += 1
    e["time"]     += taken
    e["answered"] += 1
    if uid not in quiz["student_answers"]:
        quiz["student_answers"][uid] = {}
    quiz["student_answers"][uid][idx] = chosen
    db.record_answer(uid, name, poll_id, chosen, correct, taken)

async def finish_quiz(bot, chat_id: int, quiz: dict):
    if quiz.get("finished"):
        return
    quiz["finished"] = True
    quiz["active"]   = False
    scores = quiz["scores"]
    if not scores:
        await bot.send_message(chat_id, "⚠️ Quiz खत्म — कोई जवाब नहीं मिला।")
        return
    total_q       = quiz.get("total_q", len(quiz["questions"]))
    sorted_scores = sorted(scores.items(), key=lambda x: (-x[1]["score"], x[1]["time"]))
    total_students= len(sorted_scores)
    medals        = ["🥇","🥈","🥉"]
    # FIX #11: 4096 char limit ke liye chunks mein bhejo
    header = "🏆 *Final Leaderboard*\n" + "─"*30 + "\n"
    chunks = [header]
    current = header
    for rank, (uid, s) in enumerate(sorted_scores, 1):
        medal = medals[rank-1] if rank <= 3 else f"#{rank}"
        acc   = calc_acc(s["correct"], s["answered"])
        line  = (
            f"{medal} *{s['name']}*\n"
            f"   💯 {s['score']}/{total_q} | ✅ {s['correct']} | "
            f"❌ {s['wrong']} | 🎯 {acc}% | ⏱ {fmt_time(s['time'])}\n\n"
        )
        if len(current) + len(line) > 3800:
            chunks.append(current)
            current = line
        else:
            current += line
    if current and current != header:
        chunks.append(current)
    # Pehla chunk header ke saath already hai
    for chunk in chunks[1:] if len(chunks) > 1 else chunks:
        try:
            await bot.send_message(chat_id, chunk, parse_mode=ParseMode.MARKDOWN, protect_content=True)
        except Exception:
            await bot.send_message(chat_id, chunk, protect_content=True)

    questions = quiz["questions"]
    now_str   = quiz.get("quiz_date", datetime.now().strftime("%d %b %Y, %I:%M %p IST"))
    set_name  = quiz.get("set_name","Quiz")
    lb_for_pdf= []
    for rank, (uid, s) in enumerate(sorted_scores, 1):
        acc = calc_acc(s["correct"], s["answered"])
        lb_for_pdf.append({"rank":rank,"name":s["name"],"score":s["score"],
                           "wrong":s["wrong"],"acc":acc,"time":fmt_time(s["time"])})

    await bot.send_message(chat_id, "📄 *PDF भेजी जा रही है...*", parse_mode=ParseMode.MARKDOWN)
    sent, failed = 0, []
    for rank, (uid, s) in enumerate(sorted_scores, 1):
        try:
            acc     = calc_acc(s["correct"], s["answered"])
            std_ans = quiz.get("student_answers", {}).get(uid, {})
            pdf_buf = generate_result_pdf(
                quiz_title=set_name, quiz_day=BOT_USER,
                quiz_date=now_str, total_questions=total_q,
                scoring="+1 / -0", leaderboard=lb_for_pdf,
                questions=questions, student_answers=std_ans,
                student_name=s["name"],
            )
            await bot.send_document(
                chat_id=uid, document=pdf_buf,
                filename=f"Result_{s['name'].replace(' ','_')}.pdf",
                caption=(
                    f"🎯 *आपका Result*\n\n"
                    f"🏆 Rank: #{rank}/{total_students}\n"
                    f"💯 {s['score']}/{total_q} | ✅ {s['correct']} | ❌ {s['wrong']}\n"
                    f"🎯 Accuracy: {acc}% | ⏱ {fmt_time(s['time'])}"
                ),
                parse_mode=ParseMode.MARKDOWN, protect_content=True,
            )
            sent += 1
            await asyncio.sleep(0.05)
        except TelegramError as e:
            logger.warning(f"PDF failed {s['name']}: {e}")
            failed.append(s["name"])

    msg = f"✅ *{sent}/{total_students} students को PDF मिली!*"
    if failed:
        msg += f"\n\n⚠️ *इन्हें नहीं मिली* (/start करें):\n" + "\n".join(f"• {n}" for n in failed[:15])
    await bot.send_message(chat_id, msg, parse_mode=ParseMode.MARKDOWN)
    db.save_leaderboard(chat_id, sorted_scores)
    db.cleanup_old_answers()
    for pid in list(POLL_TO_CHAT.keys()):
        if POLL_TO_CHAT[pid] == chat_id:
            del POLL_TO_CHAT[pid]

async def stop_quiz(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    quiz = ctx.chat_data.get("quiz")
    if quiz and quiz.get("active") and not quiz.get("finished"):
        await finish_quiz(ctx.bot, update.effective_chat.id, quiz)
    else:
        await update.message.reply_text("कोई Quiz नहीं चल रही।")

async def leaderboard_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = db.get_leaderboard(update.effective_chat.id, limit=20)
    if not rows:
        await update.message.reply_text("अभी कोई score नहीं है।")
        return
    medals = ["🥇","🥈","🥉"]
    text   = "🏆 *Overall Leaderboard*\n\n"
    for i, r in enumerate(rows, 1):
        medal = medals[i-1] if i <= 3 else f"#{i}"
        acc   = calc_acc(r["correct"], r.get("answered", r["correct"]+r["wrong"]))
        text += f"{medal} *{r['name']}* — {r['score']} pts | 🎯{acc}%\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, protect_content=True)

async def reset_scores(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    db.reset_leaderboard(update.effective_chat.id)
    await update.message.reply_text("✅ Scores reset हो गए।")

# ── Scheduler Task ────────────────────────────────────────────────────────────

async def scheduler_task(app: Application):
    """Har minute check karo scheduled quizzes."""
    while True:
        try:
            pending = db.get_pending_schedules()
            for sched in pending:
                chat_id   = sched["chat_id"]
                set_id    = sched["set_id"]
                questions = db.get_questions(set_id)
                set_info  = db.get_set(set_id)
                if not questions:
                    db.mark_schedule_done(sched["id"])
                    continue
                now_str = datetime.now().strftime("%d %b %Y, %I:%M %p IST")
                quiz = {
                    "questions"      : questions,
                    "scores"         : {},
                    "active"         : True,
                    "finished"       : False,
                    "poll_map"       : {},
                    "start_times"    : {},
                    "student_answers": {},
                    "set_name"       : set_info["name"] if set_info else "Quiz",
                    "quiz_date"      : now_str,
                    "total_q"        : len(questions),
                    "chat_id"        : chat_id,
                }
                if chat_id not in app.chat_data:
                    app.chat_data[chat_id] = {}
                app.chat_data[chat_id]["quiz"] = quiz
                await app.bot.send_message(
                    chat_id,
                    f"⏰ *Scheduled Quiz शुरू!*\n📚 {set_info['name']}\n❓ {len(questions)} सवाल",
                    parse_mode=ParseMode.MARKDOWN
                )
                asyncio.create_task(run_quiz(app.bot, chat_id, quiz))
                db.mark_schedule_done(sched["id"])
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        await asyncio.sleep(60)

async def on_startup(app: Application):
    asyncio.create_task(scheduler_task(app))

# ── App Build ─────────────────────────────────────────────────────────────────

def build_app():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    # Conversations
    _cancel_handler = CommandHandler("cancel", cancel_conv)
    manual_conv = ConversationHandler(
        entry_points=[CommandHandler("newquiz", newquiz_start)],
        states={
            MANUAL_QUESTION   : [_cancel_handler, MessageHandler(filters.TEXT & ~filters.COMMAND | filters.PHOTO, recv_question)],
            MANUAL_OPTION_A   : [_cancel_handler, MessageHandler(filters.TEXT & ~filters.COMMAND, recv_option_a)],
            MANUAL_OPTION_B   : [_cancel_handler, MessageHandler(filters.TEXT & ~filters.COMMAND, recv_option_b)],
            MANUAL_OPTION_C   : [_cancel_handler, MessageHandler(filters.TEXT & ~filters.COMMAND, recv_option_c)],
            MANUAL_OPTION_D   : [_cancel_handler, MessageHandler(filters.TEXT & ~filters.COMMAND, recv_option_d)],
            MANUAL_CORRECT    : [_cancel_handler, CallbackQueryHandler(recv_correct, pattern=r"^correct_")],
            MANUAL_EXPLANATION: [_cancel_handler, MessageHandler(filters.TEXT & ~filters.COMMAND, recv_explanation)],
            MANUAL_TIMER      : [_cancel_handler, CallbackQueryHandler(recv_timer, pattern=r"^timer_")],
            SET_NAME          : [
                _cancel_handler,
                MessageHandler(filters.TEXT & ~filters.COMMAND, recv_set_name),
                CallbackQueryHandler(recv_set_choice, pattern=r"^(addtoset_|newset$)"),
            ],
        },
        fallbacks=[_cancel_handler],
        per_chat=False,
        allow_reentry=True,
    )

    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("broadcast", broadcast_start)],
        states={BROADCAST_MSG: [MessageHandler(filters.TEXT, broadcast_send)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_chat=False,
    )

    schedule_conv = ConversationHandler(
        entry_points=[CommandHandler("schedule", schedule_start)],
        states={
            SCHEDULE_SET : [CallbackQueryHandler(schedule_set_chosen, pattern=r"^schedset_")],
            SCHEDULE_TIME: [MessageHandler(filters.TEXT, schedule_time_set)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_chat=False,
    )

    rename_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(rename_set_cb, pattern=r"^renameset_")],
        states={RENAME_SET: [MessageHandler(filters.TEXT, rename_set_done)]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_chat=False,
    )

    settimer_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(settimer_cb, pattern=r"^settimer_")],
        states={SET_TIMER_VAL: [CallbackQueryHandler(settimer_done, pattern=r"^timer_")]},
        fallbacks=[CommandHandler("cancel", cancel_conv)],
        per_chat=False,
    )

    # Handlers
    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("help",        help_cmd))
    app.add_handler(CommandHandler("sets",        list_sets))
    app.add_handler(CommandHandler("startquiz",   startquiz_cmd))
    app.add_handler(CommandHandler("stopquiz",    stop_quiz))
    app.add_handler(CommandHandler("leaderboard", leaderboard_cmd))
    app.add_handler(CommandHandler("resetscores", reset_scores))
    app.add_handler(CommandHandler("bulkupload",  bulk_upload_start))
    app.add_handler(CommandHandler("txtupload",   txt_upload_start))
    app.add_handler(CommandHandler("manageset",   manage_set_cmd))
    app.add_handler(CommandHandler("myrank",      my_rank))
    app.add_handler(CommandHandler("stats",       stats_cmd))
    app.add_handler(CommandHandler("ban",         ban_cmd))
    app.add_handler(CommandHandler("unban",       unban_cmd))
    app.add_handler(CommandHandler("schedules",   list_schedules))

    app.add_handler(manual_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(schedule_conv)
    app.add_handler(rename_conv)
    app.add_handler(settimer_conv)

    app.add_handler(CallbackQueryHandler(userquiz_callback,    pattern=r"^userquiz_"))
    app.add_handler(CallbackQueryHandler(start_quiz_callback,  pattern=r"^startset_"))
    app.add_handler(CallbackQueryHandler(manage_set_chosen,    pattern=r"^mgset_"))
    app.add_handler(CallbackQueryHandler(mgaddq_callback,      pattern=r"^mgaddq_"))
    app.add_handler(CallbackQueryHandler(sets_addq_callback,   pattern=r"^sets_addq"))
    app.add_handler(CallbackQueryHandler(shuffle_set_cb,       pattern=r"^shuffle_"))
    app.add_handler(CallbackQueryHandler(delete_set_cb,        pattern=r"^delset_"))
    app.add_handler(CallbackQueryHandler(delete_schedule_cb,   pattern=r"^delsched_"))

    # ✅ /addquestion — preset set selector (start mein)
    app.add_handler(CallbackQueryHandler(aqpreset_callback, pattern=r"^aqpreset_"))
    # ✅ /addquestion — Auto ✅ detect set selector (per-question)
    app.add_handler(CallbackQueryHandler(aqset_callback,    pattern=r"^aqset_"))
    # 📋 Forwarded poll set selector
    app.add_handler(CallbackQueryHandler(fwdset_callback, pattern=r"^fwdset_"))

    # Forwarded poll (new — with set selector)
    app.add_handler(MessageHandler(filters.FORWARDED, handle_forwarded_poll_new))
    app.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), handle_excel))
    app.add_handler(MessageHandler(filters.Document.FileExtension("txt"),  handle_txt))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    # /addquestion + /done commands
    app.add_handler(CommandHandler("addquestion", addquestion_start))
    app.add_handler(CommandHandler("done",        addquestion_done))

    # ✅ Text handler for /addquestion mode (set-name input + ✅ detection)
    # Must be AFTER ConversationHandlers so they take priority
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & ~filters.FORWARDED,
        handle_aq_text
    ))

    return app

if __name__ == "__main__":
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ config.py mein BOT_TOKEN set karo!")
        exit(1)
    if ADMIN_IDS == [123456789]:
        print("⚠️ config.py mein ADMIN_IDS set karo!")

    app = build_app()
    logger.info(f"🚀 {BOT_NAME} चालू हो रहा है...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
