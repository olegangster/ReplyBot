"""
Единый сервер:
- /           → Веб-страница авторизации (ввод телефона + кода)
- /userbot    → Запуск основного userbot после авторизации
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string

from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from pyrogram.errors import FloodWait, ChatWriteForbidden, UserBannedInChannel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Переменные окружения ─────────────────────────────────────────────────────
API_ID       = int(os.environ["API_ID"])
API_HASH     = os.environ["API_HASH"]
BOT_TOKEN    = os.environ["BOT_TOKEN"]
OWNER_ID     = int(os.environ["OWNER_ID"])
SESSION_STR  = os.environ.get("SESSION_STRING", "").strip()
PORT         = int(os.environ.get("PORT", 8080))

DATA_FILE    = "data.json"
SESSION_FILE = "session.json"

# ─── Данные ───────────────────────────────────────────────────────────────────

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "chats": {},
        "messages": {},
        "cycles": {},
        "settings": {"delay_between_sends": 3}
    }

def save_data(d):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

data = load_data()

# ─── Flask ────────────────────────────────────────────────────────────────────
app = Flask(__name__)

AUTH_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Telegram Userbot — Авторизация</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0f0f1a;
    color: #e0e0e0;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 20px;
  }
  .card {
    background: #1a1a2e;
    border: 1px solid #2a2a4a;
    border-radius: 16px;
    padding: 40px;
    width: 100%;
    max-width: 460px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
  }
  .logo {
    font-size: 48px;
    text-align: center;
    margin-bottom: 8px;
  }
  h1 {
    text-align: center;
    font-size: 22px;
    color: #fff;
    margin-bottom: 6px;
  }
  .subtitle {
    text-align: center;
    color: #888;
    font-size: 14px;
    margin-bottom: 32px;
  }
  .step {
    display: none;
  }
  .step.active {
    display: block;
  }
  label {
    display: block;
    font-size: 13px;
    color: #aaa;
    margin-bottom: 8px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  input {
    width: 100%;
    padding: 14px 16px;
    background: #0f0f1a;
    border: 1px solid #2a2a4a;
    border-radius: 10px;
    color: #fff;
    font-size: 16px;
    outline: none;
    transition: border-color 0.2s;
    margin-bottom: 20px;
  }
  input:focus { border-color: #5865f2; }
  button {
    width: 100%;
    padding: 14px;
    background: linear-gradient(135deg, #5865f2, #4752c4);
    border: none;
    border-radius: 10px;
    color: #fff;
    font-size: 16px;
    font-weight: 600;
    cursor: pointer;
    transition: opacity 0.2s, transform 0.1s;
  }
  button:hover { opacity: 0.9; transform: translateY(-1px); }
  button:active { transform: translateY(0); }
  button:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
  .info-box {
    background: #0f0f1a;
    border: 1px solid #2a2a4a;
    border-radius: 10px;
    padding: 14px 16px;
    font-size: 14px;
    color: #aaa;
    margin-bottom: 20px;
    line-height: 1.6;
  }
  .info-box strong { color: #fff; }
  .session-box {
    background: #0f0f1a;
    border: 1px solid #5865f2;
    border-radius: 10px;
    padding: 14px 16px;
    font-size: 11px;
    color: #5865f2;
    word-break: break-all;
    margin-bottom: 20px;
    font-family: monospace;
    line-height: 1.5;
    max-height: 120px;
    overflow-y: auto;
  }
  .error {
    color: #f04747;
    font-size: 14px;
    margin-bottom: 16px;
    padding: 12px;
    background: rgba(240,71,71,0.1);
    border-radius: 8px;
    border: 1px solid rgba(240,71,71,0.3);
  }
  .success-icon { font-size: 64px; text-align: center; margin-bottom: 16px; }
  .steps-indicator {
    display: flex;
    justify-content: center;
    gap: 8px;
    margin-bottom: 28px;
  }
  .dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #2a2a4a;
    transition: background 0.3s;
  }
  .dot.active { background: #5865f2; }
  .dot.done { background: #57f287; }
  .copy-btn {
    background: #2a2a4a;
    padding: 10px;
    font-size: 14px;
    margin-bottom: 12px;
  }
  .warning {
    color: #faa61a;
    font-size: 13px;
    text-align: center;
    margin-bottom: 16px;
  }
</style>
</head>
<body>
<div class="card">
  <div class="logo">📡</div>
  <h1>Telegram Userbot</h1>
  <p class="subtitle">Авторизация аккаунта для рассылки</p>

  <div class="steps-indicator">
    <div class="dot active" id="dot1"></div>
    <div class="dot" id="dot2"></div>
    <div class="dot" id="dot3"></div>
  </div>

  <!-- Шаг 1: Телефон -->
  <div class="step active" id="step1">
    <div class="info-box">
      Введи номер телефона Telegram-аккаунта, <strong>от имени которого</strong> будет идти рассылка.
    </div>
    <label>Номер телефона</label>
    <input type="tel" id="phone" placeholder="+380501234567" />
    <div id="err1" class="error" style="display:none"></div>
    <button id="btn1" onclick="sendPhone()">Отправить код →</button>
  </div>

  <!-- Шаг 2: Код -->
  <div class="step" id="step2">
    <div class="info-box">
      Telegram отправил код в приложение.<br>
      <strong>Открой Telegram → Saved Messages</strong> или уведомление.
    </div>
    <label>Код из Telegram</label>
    <input type="text" id="code" placeholder="12345" maxlength="10" />
    <div id="err2" class="error" style="display:none"></div>
    <button id="btn2" onclick="sendCode()">Подтвердить →</button>
  </div>

  <!-- Шаг 3: Готово -->
  <div class="step" id="step3">
    <div class="success-icon">✅</div>
    <div class="info-box" style="text-align:center; margin-bottom:16px;">
      Авторизация успешна! Скопируй SESSION_STRING и добавь в переменные окружения на Render.
    </div>
    <label>SESSION_STRING</label>
    <div class="session-box" id="session_display"></div>
    <p class="warning">⚠️ Никому не показывай эту строку!</p>
    <button class="copy-btn" onclick="copySession()">📋 Скопировать</button>
    <div class="info-box" style="font-size:13px;">
      <strong>Следующий шаг:</strong><br>
      1. На Render → Environment Variables<br>
      2. Добавь <strong>SESSION_STRING</strong> = скопированная строка<br>
      3. Перезапусти сервис — бот начнёт работать!
    </div>
  </div>
</div>

<script>
let phoneHash = '';

async function sendPhone() {
  const phone = document.getElementById('phone').value.trim();
  if (!phone) { showErr('err1', 'Введи номер телефона'); return; }
  
  const btn = document.getElementById('btn1');
  btn.disabled = true;
  btn.textContent = 'Отправляю...';
  hideErr('err1');

  try {
    const r = await fetch('/send_code', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({phone})
    });
    const d = await r.json();
    if (d.ok) {
      phoneHash = d.phone_code_hash;
      goStep(2);
    } else {
      showErr('err1', d.error || 'Ошибка. Проверь номер.');
      btn.disabled = false;
      btn.textContent = 'Отправить код →';
    }
  } catch(e) {
    showErr('err1', 'Сетевая ошибка: ' + e.message);
    btn.disabled = false;
    btn.textContent = 'Отправить код →';
  }
}

async function sendCode() {
  const phone = document.getElementById('phone').value.trim();
  const code = document.getElementById('code').value.trim();
  if (!code) { showErr('err2', 'Введи код из Telegram'); return; }

  const btn = document.getElementById('btn2');
  btn.disabled = true;
  btn.textContent = 'Проверяю...';
  hideErr('err2');

  try {
    const r = await fetch('/verify_code', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({phone, code, phone_code_hash: phoneHash})
    });
    const d = await r.json();
    if (d.ok) {
      document.getElementById('session_display').textContent = d.session_string;
      goStep(3);
    } else {
      showErr('err2', d.error || 'Неверный код. Попробуй ещё раз.');
      btn.disabled = false;
      btn.textContent = 'Подтвердить →';
    }
  } catch(e) {
    showErr('err2', 'Сетевая ошибка: ' + e.message);
    btn.disabled = false;
    btn.textContent = 'Подтвердить →';
  }
}

function copySession() {
  const text = document.getElementById('session_display').textContent;
  navigator.clipboard.writeText(text).then(() => {
    const btn = event.target;
    btn.textContent = '✅ Скопировано!';
    setTimeout(() => btn.textContent = '📋 Скопировать', 2000);
  });
}

function goStep(n) {
  document.querySelectorAll('.step').forEach((el, i) => {
    el.classList.toggle('active', i === n - 1);
  });
  document.querySelectorAll('.dot').forEach((dot, i) => {
    dot.classList.remove('active', 'done');
    if (i < n - 1) dot.classList.add('done');
    else if (i === n - 1) dot.classList.add('active');
  });
}

function showErr(id, msg) {
  const el = document.getElementById(id);
  el.textContent = msg;
  el.style.display = 'block';
}

function hideErr(id) {
  document.getElementById(id).style.display = 'none';
}

// Enter для отправки
document.addEventListener('keydown', e => {
  if (e.key !== 'Enter') return;
  const s1 = document.getElementById('step1');
  const s2 = document.getElementById('step2');
  if (s1.classList.contains('active')) sendPhone();
  else if (s2.classList.contains('active')) sendCode();
});
</script>
</body>
</html>
"""

# Временный клиент для авторизации
_auth_client = None
_auth_loop   = None

def get_auth_loop():
    global _auth_loop
    if _auth_loop is None or _auth_loop.is_closed():
        _auth_loop = asyncio.new_event_loop()
    return _auth_loop

@app.route("/")
def index():
    if SESSION_STR:
        return "<h2 style='font-family:sans-serif;text-align:center;margin-top:80px;color:#57f287'>✅ Бот уже авторизован и работает!</h2>"
    return render_template_string(AUTH_HTML)

@app.route("/send_code", methods=["POST"])
def send_code():
    global _auth_client
    payload = request.get_json()
    phone   = payload.get("phone", "").strip()
    
    loop = get_auth_loop()

    async def _do():
        global _auth_client
        _auth_client = Client(
            "auth_temp",
            api_id=API_ID,
            api_hash=API_HASH,
            in_memory=True,
            no_updates=True
        )
        await _auth_client.connect()
        sent = await _auth_client.send_code(phone)
        return sent.phone_code_hash

    try:
        future = asyncio.run_coroutine_threadsafe(_do(), loop)
        phone_code_hash = future.result(timeout=30)
        return jsonify({"ok": True, "phone_code_hash": phone_code_hash})
    except Exception as e:
        logger.error(f"send_code error: {e}")
        return jsonify({"ok": False, "error": str(e)})

@app.route("/verify_code", methods=["POST"])
def verify_code():
    global _auth_client
    payload         = request.get_json()
    phone           = payload.get("phone", "").strip()
    code            = payload.get("code", "").strip()
    phone_code_hash = payload.get("phone_code_hash", "").strip()

    loop = get_auth_loop()

    async def _do():
        global _auth_client
        await _auth_client.sign_in(phone, phone_code_hash, code)
        session_string = await _auth_client.export_session_string()
        await _auth_client.disconnect()
        return session_string

    try:
        future = asyncio.run_coroutine_threadsafe(_do(), loop)
        session_string = future.result(timeout=30)
        return jsonify({"ok": True, "session_string": session_string})
    except Exception as e:
        logger.error(f"verify_code error: {e}")
        return jsonify({"ok": False, "error": str(e)})

@app.route("/health")
def health():
    return jsonify({"status": "ok", "authorized": bool(SESSION_STR)})


# ══════════════════════════════════════════════════════════════════════════════
#  USERBOT + УПРАВЛЯЮЩИЙ БОТ
# ══════════════════════════════════════════════════════════════════════════════

scheduler = AsyncIOScheduler()

userbot = None
bot     = None

if SESSION_STR:
    userbot = Client(
        "userbot_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STR,
    )
    bot = Client(
        "bot_session",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    )


# ─── FSM ─────────────────────────────────────────────────────────────────────
user_states: dict[int, dict] = {}

def set_state(uid, state, **kw):  user_states[uid] = {"state": state, **kw}
def get_state(uid):               return user_states.get(uid, {})
def clear_state(uid):             user_states.pop(uid, None)


def main_menu_kb():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📢 Чаты"),       KeyboardButton("✉️ Сообщения")],
        [KeyboardButton("🚀 Разослать"),  KeyboardButton("🔄 Циклы")],
        [KeyboardButton("⚙️ Настройки"), KeyboardButton("📊 Статистика")],
    ], resize_keyboard=True)

def next_id(d): return str(max([int(k) for k in d.keys() if k.isdigit()], default=0) + 1)

async def smart_delay():
    import random
    d = data["settings"]["delay_between_sends"]
    await asyncio.sleep(max(1, d + random.uniform(-0.3, 1.0)))


# ─── РАССЫЛКА ─────────────────────────────────────────────────────────────────

async def do_broadcast(msg_id: str, status_chat_id=None):
    if msg_id not in data["messages"]: return 0, 0
    text = data["messages"][msg_id]["text"]
    name = data["messages"][msg_id]["name"]
    active = {cid: inf for cid, inf in data["chats"].items() if inf.get("active")}
    if not active:
        if status_chat_id: await bot.send_message(status_chat_id, "⚠️ Нет активных чатов!")
        return 0, 0

    ok = fail = 0
    status_msg = None
    if status_chat_id:
        status_msg = await bot.send_message(status_chat_id,
            f"📤 Рассылка **{name}**...\n⏳ 0/{len(active)}")

    for i, (cid, info) in enumerate(active.items(), 1):
        try:
            await userbot.send_message(int(cid), text)
            ok += 1
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            try:
                await userbot.send_message(int(cid), text); ok += 1
            except: fail += 1
        except Exception as e:
            logger.error(f"Broadcast error {cid}: {e}"); fail += 1
        await smart_delay()
        if status_msg and (i % 5 == 0 or i == len(active)):
            try: await status_msg.edit_text(f"📤 **{name}**\n✅{ok} ❌{fail} ⏳{i}/{len(active)}")
            except: pass

    if status_chat_id:
        await bot.send_message(status_chat_id,
            f"🎉 **{name}** завершена!\n✅ {ok} успешно\n❌ {fail} ошибок")
    return ok, fail

async def cycle_run(cycle_id: str):
    if cycle_id not in data["cycles"] or not data["cycles"][cycle_id].get("active"): return
    cyc = data["cycles"][cycle_id]
    ok, fail = await do_broadcast(cyc["msg_id"])
    try: await bot.send_message(OWNER_ID, f"⏰ Цикл **{cyc['name']}**\n✅{ok} ❌{fail}")
    except: pass

def reg_cycle(cid):
    cyc = data["cycles"][cid]
    scheduler.add_job(cycle_run, "interval", hours=cyc["interval_hours"],
                      id=f"c_{cid}", replace_existing=True, args=[cid])


# ─── /start ───────────────────────────────────────────────────────────────────

if bot:
    @bot.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, m: Message):
        clear_state(m.from_user.id)
        await m.reply(
            "👋 **Бот рассылки от твоего аккаунта**\n\n"
            "📢 Чаты · ✉️ Сообщения · 🚀 Разослать · 🔄 Циклы",
            reply_markup=main_menu_kb()
        )

    # ── ЧАТЫ ──────────────────────────────────────────────────────────────────

    @bot.on_message(filters.regex("^📢 Чаты$") & filters.private)
    async def sec_chats(_, m: Message):
        clear_state(m.from_user.id)
        g = sum(1 for c in data["chats"].values() if c.get("type")=="group"   and c.get("active"))
        ch= sum(1 for c in data["chats"].values() if c.get("type")=="channel" and c.get("active"))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить",   callback_data="add_chat")],
            [InlineKeyboardButton("📋 Список",     callback_data="list_chats")],
            [InlineKeyboardButton("🗑 Удалить",    callback_data="del_chat_menu")],
        ])
        await m.reply(f"📢 **Чаты**\n👥 Групп: {g} · 📣 Каналов: {ch} · Всего: {len(data['chats'])}", reply_markup=kb)

    @bot.on_callback_query(filters.regex("^add_chat$"))
    async def cb_add_chat(_, cq):
        set_state(cq.from_user.id, "add_chat")
        await cq.message.reply(
            "➕ Перешли сюда любое сообщение из нужной группы или канала.\n\n"
            "Или введи username (`@channel`) / ID (`-1001234567890`)\n\n"
            "⚠️ Твой аккаунт должен быть **участником** группы или **админом** канала."
        )
        await cq.answer()

    @bot.on_callback_query(filters.regex("^list_chats$"))
    async def cb_list_chats(_, cq):
        if not data["chats"]:
            await cq.message.reply("📭 Чатов нет."); await cq.answer(); return
        lines = ["📋 **Чаты:**\n"]
        btns  = []
        for cid, inf in data["chats"].items():
            e = "📣" if inf.get("type")=="channel" else "👥"
            s = "✅" if inf.get("active") else "⏸"
            lines.append(f"{s}{e} **{inf['title']}** `{cid}`")
            t = "⏸" if inf.get("active") else "▶️"
            btns.append([InlineKeyboardButton(f"{t} {inf['title'][:24]}", callback_data=f"tog:{cid}")])
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("\n".join(lines), reply_markup=InlineKeyboardMarkup(btns))
        await cq.answer()

    @bot.on_callback_query(filters.regex(r"^tog:"))
    async def cb_tog(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["chats"]:
            data["chats"][cid]["active"] = not data["chats"][cid].get("active", True)
            save_data(data)
            s = "✅" if data["chats"][cid]["active"] else "⏸"
            await cq.answer(f"{s} {data['chats'][cid]['title']}")

    @bot.on_callback_query(filters.regex("^del_chat_menu$"))
    async def cb_del_chat_menu(_, cq):
        if not data["chats"]: await cq.answer("Нет чатов"); return
        btns = [[InlineKeyboardButton(f"🗑 {i['title']}", callback_data=f"dc:{c}")]
                for c, i in data["chats"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns))
        await cq.answer()

    @bot.on_callback_query(filters.regex(r"^dc:"))
    async def cb_dc(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["chats"]:
            t = data["chats"].pop(cid)["title"]; save_data(data)
            await cq.answer(f"Удалён: {t}"); await cq.message.reply(f"🗑 **{t}** удалён.")

    # ── СООБЩЕНИЯ ─────────────────────────────────────────────────────────────

    @bot.on_message(filters.regex("^✉️ Сообщения$") & filters.private)
    async def sec_msgs(_, m: Message):
        clear_state(m.from_user.id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Новое",        callback_data="add_msg")],
            [InlineKeyboardButton("📋 Список",       callback_data="list_msgs")],
            [InlineKeyboardButton("✏️ Редактировать",callback_data="edit_msg_menu")],
            [InlineKeyboardButton("🗑 Удалить",      callback_data="del_msg_menu")],
        ])
        await m.reply(f"✉️ **Сообщения** — {len(data['messages'])} шт.", reply_markup=kb)

    @bot.on_callback_query(filters.regex("^add_msg$"))
    async def cb_add_msg(_, cq):
        set_state(cq.from_user.id, "msg_text")
        await cq.message.reply("✉️ Напиши текст сообщения для рассылки:"); await cq.answer()

    @bot.on_callback_query(filters.regex("^list_msgs$"))
    async def cb_list_msgs(_, cq):
        if not data["messages"]: await cq.message.reply("📭 Пусто."); await cq.answer(); return
        lines = ["✉️ **Сообщения:**\n"]
        for mid, inf in data["messages"].items():
            p = inf["text"][:70]+"…" if len(inf["text"])>70 else inf["text"]
            lines.append(f"📌 **{inf['name']}** (ID:`{mid}`)\n└ {p}\n")
        await cq.message.reply("\n".join(lines)); await cq.answer()

    @bot.on_callback_query(filters.regex("^edit_msg_menu$"))
    async def cb_edit_msg_menu(_, cq):
        if not data["messages"]: await cq.answer("Нет сообщений"); return
        btns = [[InlineKeyboardButton(f"✏️ {i['name']}", callback_data=f"em:{mid}")]
                for mid, i in data["messages"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @bot.on_callback_query(filters.regex(r"^em:"))
    async def cb_em(_, cq):
        mid = cq.data.split(":")[1]
        set_state(cq.from_user.id, "edit_msg", msg_id=mid)
        await cq.message.reply(
            f"✏️ **{data['messages'][mid]['name']}**\n\n{data['messages'][mid]['text']}\n\nНовый текст:"
        ); await cq.answer()

    @bot.on_callback_query(filters.regex("^del_msg_menu$"))
    async def cb_del_msg_menu(_, cq):
        if not data["messages"]: await cq.answer("Нет сообщений"); return
        btns = [[InlineKeyboardButton(f"🗑 {i['name']}", callback_data=f"dm:{mid}")]
                for mid, i in data["messages"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @bot.on_callback_query(filters.regex(r"^dm:"))
    async def cb_dm(_, cq):
        mid = cq.data.split(":")[1]
        if mid in data["messages"]:
            n = data["messages"].pop(mid)["name"]; save_data(data)
            await cq.answer(f"Удалено: {n}"); await cq.message.reply(f"🗑 **{n}** удалено.")

    # ── РАЗОСЛАТЬ ─────────────────────────────────────────────────────────────

    @bot.on_message(filters.regex("^🚀 Разослать$") & filters.private)
    async def sec_send(_, m: Message):
        clear_state(m.from_user.id)
        if not data["messages"]: await m.reply("❌ Нет сообщений."); return
        if not data["chats"]:   await m.reply("❌ Нет чатов."); return
        btns = [[InlineKeyboardButton(f"📤 {i['name']}", callback_data=f"sn:{mid}")]
                for mid, i in data["messages"].items()]
        btns.append([InlineKeyboardButton("📤 Все сообщения сразу", callback_data="sa")])
        await m.reply("Выбери сообщение:", reply_markup=InlineKeyboardMarkup(btns))

    @bot.on_callback_query(filters.regex(r"^sn:"))
    async def cb_sn(_, cq):
        await cq.answer("⏳ Начинаю..."); await do_broadcast(cq.data.split(":")[1], cq.message.chat.id)

    @bot.on_callback_query(filters.regex("^sa$"))
    async def cb_sa(_, cq):
        await cq.answer("⏳ Отправляю все...")
        for mid in list(data["messages"].keys()):
            await do_broadcast(mid, cq.message.chat.id)

    # ── ЦИКЛЫ ─────────────────────────────────────────────────────────────────

    @bot.on_message(filters.regex("^🔄 Циклы$") & filters.private)
    async def sec_cycles(_, m: Message):
        clear_state(m.from_user.id)
        active = sum(1 for c in data["cycles"].values() if c.get("active"))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Создать",      callback_data="add_cycle")],
            [InlineKeyboardButton("📋 Список",       callback_data="list_cycles")],
            [InlineKeyboardButton("⏸/▶️ Вкл/Откл",  callback_data="tog_cycles")],
            [InlineKeyboardButton("🗑 Удалить",      callback_data="del_cycle_menu")],
        ])
        await m.reply(f"🔄 **Циклы**\nАктивных: {active}/{len(data['cycles'])}", reply_markup=kb)

    @bot.on_callback_query(filters.regex("^add_cycle$"))
    async def cb_add_cycle(_, cq):
        if not data["messages"]: await cq.answer("Сначала добавь сообщение!"); return
        btns = [[InlineKeyboardButton(i["name"], callback_data=f"cm:{mid}")]
                for mid, i in data["messages"].items()]
        await cq.message.reply("Шаг 1 — выбери сообщение:", reply_markup=InlineKeyboardMarkup(btns))
        await cq.answer()

    @bot.on_callback_query(filters.regex(r"^cm:"))
    async def cb_cm(_, cq):
        mid = cq.data.split(":")[1]
        set_state(cq.from_user.id, "cycle_interval", msg_id=mid)
        await cq.message.reply("Шаг 2 — каждые сколько часов? (например `6` или `24`)"); await cq.answer()

    @bot.on_callback_query(filters.regex("^list_cycles$"))
    async def cb_list_cycles(_, cq):
        if not data["cycles"]: await cq.message.reply("📭 Циклов нет."); await cq.answer(); return
        lines = ["🔄 **Циклы:**\n"]
        for cid, cyc in data["cycles"].items():
            s = "✅" if cyc.get("active") else "⏸"
            mn = data["messages"].get(cyc["msg_id"], {}).get("name", "?")
            lines.append(f"{s} **{cyc['name']}** — каждые {cyc['interval_hours']}ч\n└ {mn}\n")
        await cq.message.reply("\n".join(lines)); await cq.answer()

    @bot.on_callback_query(filters.regex("^tog_cycles$"))
    async def cb_tog_cycles(_, cq):
        if not data["cycles"]: await cq.answer("Нет циклов"); return
        btns = [[InlineKeyboardButton(
            f"{'⏸' if c.get('active') else '▶️'} {c['name']}", callback_data=f"tc:{cid}"
        )] for cid, c in data["cycles"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Управление:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @bot.on_callback_query(filters.regex(r"^tc:"))
    async def cb_tc(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["cycles"]:
            data["cycles"][cid]["active"] = not data["cycles"][cid].get("active", True)
            save_data(data)
            s = "▶️ запущен" if data["cycles"][cid]["active"] else "⏸ пауза"
            await cq.answer(f"{data['cycles'][cid]['name']}: {s}")

    @bot.on_callback_query(filters.regex("^del_cycle_menu$"))
    async def cb_del_cycle_menu(_, cq):
        if not data["cycles"]: await cq.answer("Нет циклов"); return
        btns = [[InlineKeyboardButton(f"🗑 {c['name']}", callback_data=f"dlc:{cid}")]
                for cid, c in data["cycles"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @bot.on_callback_query(filters.regex(r"^dlc:"))
    async def cb_dlc(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["cycles"]:
            n = data["cycles"].pop(cid)["name"]; save_data(data)
            try: scheduler.remove_job(f"c_{cid}")
            except: pass
            await cq.answer(f"Удалён: {n}"); await cq.message.reply(f"🗑 Цикл **{n}** удалён.")

    # ── НАСТРОЙКИ ─────────────────────────────────────────────────────────────

    @bot.on_message(filters.regex("^⚙️ Настройки$") & filters.private)
    async def sec_settings(_, m: Message):
        clear_state(m.from_user.id)
        d = data["settings"]["delay_between_sends"]
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"⏱ Задержка: {d}с → изменить", callback_data="chd")]])
        await m.reply(f"⚙️ **Настройки**\n\nЗадержка между отправками: **{d} сек.**", reply_markup=kb)

    @bot.on_callback_query(filters.regex("^chd$"))
    async def cb_chd(_, cq):
        set_state(cq.from_user.id, "chdelay")
        await cq.message.reply("⏱ Введи задержку в секундах (2–5 рекомендуется):"); await cq.answer()

    # ── СТАТИСТИКА ────────────────────────────────────────────────────────────

    @bot.on_message(filters.regex("^📊 Статистика$") & filters.private)
    async def sec_stats(_, m: Message):
        g  = sum(1 for c in data["chats"].values() if c.get("type")=="group")
        ch = sum(1 for c in data["chats"].values() if c.get("type")=="channel")
        ac = sum(1 for c in data["chats"].values() if c.get("active"))
        cyc= sum(1 for c in data["cycles"].values() if c.get("active"))
        await m.reply(
            f"📊 **Статистика**\n\n"
            f"📣 Каналов: {ch} · 👥 Групп: {g}\n"
            f"✅ Активных чатов: {ac}/{len(data['chats'])}\n"
            f"✉️ Сообщений: {len(data['messages'])}\n"
            f"🔄 Активных циклов: {cyc}/{len(data['cycles'])}\n"
            f"⏱ Задержка: {data['settings']['delay_between_sends']} сек."
        )

    @bot.on_callback_query(filters.regex("^bk$"))
    async def cb_bk(_, cq):
        await cq.message.reply("Главное меню:", reply_markup=main_menu_kb()); await cq.answer()

    # ── FSM текст ─────────────────────────────────────────────────────────────

    @bot.on_message(filters.private & filters.forwarded)
    async def handle_fwd(_, m: Message):
        st = get_state(m.from_user.id)
        if st.get("state") != "add_chat": return
        fc = m.forward_from_chat
        if not fc: await m.reply("❌ Не могу определить чат."); clear_state(m.from_user.id); return
        cid   = str(fc.id)
        ctype = "channel" if fc.type.value == "channel" else "group"
        title = fc.title or cid
        emoji = "📣" if ctype == "channel" else "👥"
        if cid in data["chats"]:
            await m.reply(f"⚠️ {emoji} **{title}** уже добавлен.")
        else:
            data["chats"][cid] = {"title": title, "type": ctype, "active": True, "added": str(datetime.now())}
            save_data(data)
            await m.reply(f"✅ {emoji} **{title}** добавлен!\nID: `{cid}`", reply_markup=main_menu_kb())
        clear_state(m.from_user.id)

    @bot.on_message(filters.private & filters.text)
    async def handle_text(_, m: Message):
        uid = m.from_user.id
        st  = get_state(uid)
        if not st: return
        s = st.get("state")

        if s == "add_chat":
            try:
                ci = await userbot.get_chat(m.text.strip())
                cid   = str(ci.id)
                ctype = "channel" if ci.type.value == "channel" else "group"
                title = ci.title or cid
                emoji = "📣" if ctype == "channel" else "👥"
                if cid in data["chats"]:
                    await m.reply(f"⚠️ {emoji} **{title}** уже добавлен.")
                else:
                    data["chats"][cid] = {"title": title, "type": ctype, "active": True, "added": str(datetime.now())}
                    save_data(data)
                    await m.reply(f"✅ {emoji} **{title}** добавлен!", reply_markup=main_menu_kb())
            except Exception as e:
                await m.reply(f"❌ Ошибка: {e}")
            clear_state(uid)

        elif s == "msg_text":
            set_state(uid, "msg_name", text=m.text)
            await m.reply("📝 Название для этого сообщения:")

        elif s == "msg_name":
            mid = next_id(data["messages"])
            data["messages"][mid] = {"name": m.text, "text": st["text"]}
            save_data(data)
            await m.reply(f"✅ **{m.text}** сохранено!", reply_markup=main_menu_kb())
            clear_state(uid)

        elif s == "edit_msg":
            data["messages"][st["msg_id"]]["text"] = m.text
            save_data(data)
            await m.reply("✅ Обновлено!", reply_markup=main_menu_kb())
            clear_state(uid)

        elif s == "cycle_interval":
            try:
                h = float(m.text.strip().replace(",", "."))
                if h <= 0: raise ValueError
            except:
                await m.reply("❌ Введи число часов, например `6`"); return
            set_state(uid, "cycle_name", msg_id=st["msg_id"], interval=h)
            await m.reply(f"Каждые **{h} ч.** ✅\n\nШаг 3 — название цикла:")

        elif s == "cycle_name":
            cid = next_id(data["cycles"])
            data["cycles"][cid] = {
                "name": m.text, "msg_id": st["msg_id"],
                "interval_hours": st["interval"], "active": True,
                "created": str(datetime.now())
            }
            save_data(data)
            reg_cycle(cid)
            mn = data["messages"].get(st["msg_id"], {}).get("name", "?")
            await m.reply(
                f"✅ Цикл **{m.text}** создан!\n📨 {mn}\n⏱ Каждые {st['interval']} ч.",
                reply_markup=main_menu_kb()
            )
            clear_state(uid)

        elif s == "chdelay":
            try:
                d = float(m.text.strip())
                if d < 0: raise ValueError
            except:
                await m.reply("❌ Введи число секунд"); return
            data["settings"]["delay_between_sends"] = d
            save_data(data)
            await m.reply(f"✅ Задержка: **{d} сек.**", reply_markup=main_menu_kb())
            clear_state(uid)


# ══════════════════════════════════════════════════════════════════════════════
#  Запуск
# ══════════════════════════════════════════════════════════════════════════════

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)

async def run_bots():
    if not SESSION_STR:
        logger.info("SESSION_STRING не задан — работает только страница авторизации")
        return

    for cid, cyc in data.get("cycles", {}).items():
        if cyc.get("active"):
            reg_cycle(cid)

    scheduler.start()

    await asyncio.gather(userbot.start(), bot.start())
    me = await userbot.get_me()
    bme = await bot.get_me()
    logger.info(f"✅ Userbot: {me.first_name} | Bot: @{bme.username}")
    try:
        await bot.send_message(OWNER_ID,
            f"✅ Бот запущен!\n👤 Userbot: **{me.first_name}**\n🤖 Управление: @{bme.username}")
    except: pass
    await asyncio.Event().wait()

def main():
    # Flask в отдельном потоке
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    # Отдельный event loop для авторизационного клиента
    auth_loop = asyncio.new_event_loop()

    def start_auth_loop():
        asyncio.set_event_loop(auth_loop)
        auth_loop.run_forever()

    at = threading.Thread(target=start_auth_loop, daemon=True)
    at.start()

    global _auth_loop
    _auth_loop = auth_loop

    # Основной event loop для botов
    asyncio.run(run_bots())

if __name__ == "__main__":
    main()
