"""
Telegram Userbot Broadcaster
- Telethon для userbot (рассылка от твоего аккаунта)
- aiogram для управляющего бота
- Flask для страницы авторизации в браузере
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime

from flask import Flask, request, jsonify

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── ENV ──────────────────────────────────────────────────────────────────────
API_ID      = int(os.environ.get("API_ID", "0"))
API_HASH    = os.environ.get("API_HASH", "")
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
OWNER_ID    = int(os.environ.get("OWNER_ID", "0"))
SESSION_STR = os.environ.get("SESSION_STRING", "").strip()
PORT        = int(os.environ.get("PORT", "8080"))

DATA_FILE = "data.json"

# ─── Данные ───────────────────────────────────────────────────────────────────
def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"chats": {}, "messages": {}, "cycles": {}, "settings": {"delay": 3}}

def save_data(d):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

data = load_data()

# ══════════════════════════════════════════════════════════════════════════════
#  FLASK — страница авторизации
# ══════════════════════════════════════════════════════════════════════════════
flask_app = Flask(__name__)

AUTH_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Userbot — Авторизация</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:#0d0d1a;color:#e0e0e0;min-height:100vh;display:flex;
  align-items:center;justify-content:center;padding:16px}
.card{background:#161625;border:1px solid #252540;border-radius:20px;
  padding:36px 32px;width:100%;max-width:440px;box-shadow:0 24px 64px rgba(0,0,0,.5)}
.logo{font-size:52px;text-align:center;margin-bottom:10px}
h1{text-align:center;font-size:22px;font-weight:700;color:#fff;margin-bottom:6px}
.sub{text-align:center;color:#666;font-size:14px;margin-bottom:28px}
.dots{display:flex;justify-content:center;gap:10px;margin-bottom:28px}
.dot{width:32px;height:4px;border-radius:2px;background:#252540;transition:background .3s}
.dot.on{background:#5b6ef5}.dot.done{background:#43b581}
label{display:block;font-size:12px;color:#888;font-weight:600;
  text-transform:uppercase;letter-spacing:.6px;margin-bottom:8px}
input{width:100%;padding:13px 15px;background:#0d0d1a;border:1px solid #252540;
  border-radius:12px;color:#fff;font-size:16px;outline:none;
  transition:border-color .2s;margin-bottom:18px}
input:focus{border-color:#5b6ef5}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#5b6ef5,#4457d4);
  border:none;border-radius:12px;color:#fff;font-size:15px;font-weight:600;
  cursor:pointer;transition:opacity .2s;margin-bottom:10px}
.btn:hover{opacity:.9}.btn:disabled{opacity:.4;cursor:not-allowed}
.btn.gray{background:#252540}
.hint{background:#0d0d1a;border:1px solid #252540;border-radius:12px;
  padding:13px 15px;font-size:13px;color:#888;line-height:1.7;margin-bottom:18px}
.hint b{color:#ccc}
.err{color:#f04747;font-size:13px;padding:11px 14px;background:rgba(240,71,71,.1);
  border:1px solid rgba(240,71,71,.3);border-radius:10px;margin-bottom:14px;display:none}
.sess{background:#0d0d1a;border:2px solid #43b581;border-radius:12px;
  padding:14px;font-size:11px;color:#43b581;word-break:break-all;
  font-family:monospace;line-height:1.5;max-height:110px;overflow-y:auto;margin-bottom:14px}
.warn{color:#faa61a;font-size:13px;text-align:center;margin-bottom:14px}
.step{display:none}.step.on{display:block}
.big{font-size:56px;text-align:center;margin-bottom:14px}
</style>
</head>
<body><div class="card">
<div class="logo">📡</div>
<h1>Telegram Userbot</h1>
<p class="sub">Авторизация для рассылки от твоего аккаунта</p>
<div class="dots">
  <div class="dot on" id="d1"></div>
  <div class="dot"    id="d2"></div>
  <div class="dot"    id="d3"></div>
</div>

<div class="step on" id="s1">
  <div class="hint">Введи номер телефона аккаунта Telegram, <b>от имени которого</b> будет идти рассылка.</div>
  <label>Номер телефона</label>
  <input id="phone" type="tel" placeholder="+380501234567" autofocus>
  <div class="err" id="e1"></div>
  <button class="btn" id="b1" onclick="doPhone()">Получить код →</button>
</div>

<div class="step" id="s2">
  <div class="hint">Telegram прислал код в приложение.<br>
    Открой <b>Telegram → Saved Messages (Избранное)</b> — там будет код 5 цифр.</div>
  <label>Код из Telegram</label>
  <input id="code" type="number" placeholder="12345" maxlength="10">
  <div class="err" id="e2"></div>
  <button class="btn" id="b2" onclick="doCode()">Войти →</button>
  <button class="btn gray" onclick="goStep(1)">← Назад</button>
</div>

<div class="step" id="s3">
  <div class="big">🎉</div>
  <div class="hint" style="text-align:center;margin-bottom:16px">
    <b>Авторизация успешна!</b><br>Скопируй строку ниже — это твой SESSION_STRING
  </div>
  <div class="sess" id="sval"></div>
  <p class="warn">⚠️ Никому не показывай эту строку!</p>
  <button class="btn" onclick="doCopy()">📋 Скопировать SESSION_STRING</button>
  <div class="hint" style="margin-top:14px;font-size:13px">
    <b>Что дальше на Render:</b><br>
    1. Твой сервис → вкладка <b>Environment</b><br>
    2. Найди <b>SESSION_STRING</b> → вставь строку → <b>Save Changes</b><br>
    3. Сервис сам перезапустится — бот напишет тебе ✅
  </div>
</div>
</div>

<script>
let hash='';
async function doPhone(){
  const phone=document.getElementById('phone').value.trim();
  if(!phone){showErr('e1','Введи номер');return;}
  busy('b1',true,'Отправляю...');hide('e1');
  try{
    const r=await post('/api/send_code',{phone});
    if(r.ok){hash=r.hash;goStep(2);}
    else showErr('e1',r.error||'Ошибка. Проверь номер.');
  }catch(e){showErr('e1','Ошибка: '+e.message);}
  busy('b1',false,'Получить код →');
}
async function doCode(){
  const phone=document.getElementById('phone').value.trim();
  const code=document.getElementById('code').value.trim();
  if(!code){showErr('e2','Введи код');return;}
  busy('b2',true,'Проверяю...');hide('e2');
  try{
    const r=await post('/api/verify_code',{phone,code,hash});
    if(r.ok){document.getElementById('sval').textContent=r.session;goStep(3);}
    else showErr('e2',r.error||'Неверный код.');
  }catch(e){showErr('e2','Ошибка: '+e.message);}
  busy('b2',false,'Войти →');
}
function doCopy(){
  navigator.clipboard.writeText(document.getElementById('sval').textContent).then(()=>{
    const b=event.target;b.textContent='✅ Скопировано!';
    setTimeout(()=>b.textContent='📋 Скопировать SESSION_STRING',2500);
  });
}
async function post(url,body){
  const r=await fetch(url,{method:'POST',
    headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  return r.json();
}
function goStep(n){
  [1,2,3].forEach(i=>{
    document.getElementById('s'+i).classList.toggle('on',i===n);
    const d=document.getElementById('d'+i);
    d.classList.remove('on','done');
    if(i<n)d.classList.add('done');else if(i===n)d.classList.add('on');
  });
  if(n===2)setTimeout(()=>document.getElementById('code').focus(),100);
}
function showErr(id,msg){const e=document.getElementById(id);e.textContent=msg;e.style.display='block';}
function hide(id){document.getElementById(id).style.display='none';}
function busy(id,b,txt){const el=document.getElementById(id);el.disabled=b;el.textContent=txt;}
document.addEventListener('keydown',e=>{
  if(e.key!=='Enter')return;
  if(document.getElementById('s1').classList.contains('on'))doPhone();
  else if(document.getElementById('s2').classList.contains('on'))doCode();
});
</script></body></html>"""

DONE_HTML = """<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>Бот работает</title>
<style>body{font-family:sans-serif;background:#0d0d1a;color:#e0e0e0;display:flex;
  align-items:center;justify-content:center;min-height:100vh;padding:20px}
.card{background:#161625;border:1px solid #252540;border-radius:20px;
  padding:40px;max-width:400px;text-align:center}
.ico{font-size:64px;margin-bottom:16px}h1{color:#43b581;font-size:22px;margin-bottom:10px}
p{color:#888;font-size:14px;line-height:1.7}</style></head>
<body><div class="card"><div class="ico">✅</div><h1>Бот запущен!</h1>
<p>Userbot авторизован и работает.<br>Управляй рассылкой через Telegram бота.</p>
</div></body></html>"""

# Telethon клиент для авторизации (живёт в отдельном loop)
_auth_loop: asyncio.AbstractEventLoop | None = None
_auth_tl:   TelegramClient | None = None

def get_auth_loop():
    global _auth_loop
    if _auth_loop is None or _auth_loop.is_closed():
        _auth_loop = asyncio.new_event_loop()
        threading.Thread(target=_auth_loop.run_forever, daemon=True).start()
    return _auth_loop

@flask_app.route("/")
def index():
    return DONE_HTML if SESSION_STR else AUTH_HTML

@flask_app.route("/api/send_code", methods=["POST"])
def api_send_code():
    global _auth_tl
    if not API_ID or not API_HASH:
        return jsonify({"ok": False, "error": "API_ID и API_HASH не заданы в переменных окружения!"})
    payload = request.get_json() or {}
    phone   = payload.get("phone", "").strip()
    loop    = get_auth_loop()

    async def _do():
        global _auth_tl
        _auth_tl = TelegramClient(StringSession(), API_ID, API_HASH, loop=loop)
        await _auth_tl.connect()
        result = await _auth_tl.send_code_request(phone)
        return result.phone_code_hash

    try:
        h = asyncio.run_coroutine_threadsafe(_do(), loop).result(timeout=30)
        return jsonify({"ok": True, "hash": h})
    except Exception as e:
        logger.error(f"send_code: {e}")
        return jsonify({"ok": False, "error": str(e)})

@flask_app.route("/api/verify_code", methods=["POST"])
def api_verify_code():
    global _auth_tl
    payload = request.get_json() or {}
    phone   = payload.get("phone", "").strip()
    code    = payload.get("code",  "").strip()
    h       = payload.get("hash",  "").strip()
    loop    = get_auth_loop()

    async def _do():
        from telethon.tl.functions.auth import SignInRequest
        await _auth_tl(SignInRequest(phone_number=phone, phone_code_hash=h, phone_code=code))
        session_str = _auth_tl.session.save()
        await _auth_tl.disconnect()
        return session_str

    try:
        s = asyncio.run_coroutine_threadsafe(_do(), loop).result(timeout=30)
        return jsonify({"ok": True, "session": s})
    except Exception as e:
        logger.error(f"verify_code: {e}")
        return jsonify({"ok": False, "error": str(e)})

@flask_app.route("/health")
def health():
    return jsonify({"ok": True, "has_session": bool(SESSION_STR)})

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)


# ══════════════════════════════════════════════════════════════════════════════
#  AIOGRAM — управляющий бот
# ══════════════════════════════════════════════════════════════════════════════

class ST(StatesGroup):
    add_chat    = State()
    msg_text    = State()
    msg_name    = State()
    edit_msg    = State()
    cyc_int     = State()
    cyc_name    = State()
    chdelay     = State()

def main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📢 Чаты"),      KeyboardButton(text="✉️ Сообщения")],
        [KeyboardButton(text="🚀 Разослать"), KeyboardButton(text="🔄 Циклы")],
        [KeyboardButton(text="⚙️ Настройки"),KeyboardButton(text="📊 Статистика")],
    ], resize_keyboard=True)

def nid(d):
    return str(max([int(k) for k in d if k.isdigit()], default=0) + 1)

# ─── Рассылка через Telethon ──────────────────────────────────────────────────
userbot_tl: TelegramClient | None = None
ctrl_bot:   Bot | None = None
scheduler = AsyncIOScheduler()

async def do_broadcast(msg_id: str, reply_chat=None):
    if msg_id not in data["messages"]: return 0, 0
    text   = data["messages"][msg_id]["text"]
    name   = data["messages"][msg_id]["name"]
    active = {c: i for c, i in data["chats"].items() if i.get("active")}
    if not active:
        if reply_chat: await ctrl_bot.send_message(reply_chat, "⚠️ Нет активных чатов!")
        return 0, 0

    ok = fail = 0
    sm = None
    if reply_chat:
        sm = await ctrl_bot.send_message(reply_chat, f"📤 <b>{name}</b>\n⏳ 0/{len(active)}", parse_mode="HTML")

    import random
    for i, (cid, info) in enumerate(active.items(), 1):
        try:
            entity = int(cid)
            await userbot_tl.send_message(entity, text)
            ok += 1
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            try: await userbot_tl.send_message(entity, text); ok += 1
            except: fail += 1
        except Exception as e:
            logger.error(f"broadcast {cid}: {e}"); fail += 1

        delay = data["settings"]["delay"] + random.uniform(0, 1.5)
        await asyncio.sleep(max(1, delay))

        if sm and (i % 5 == 0 or i == len(active)):
            try:
                await ctrl_bot.edit_message_text(
                    f"📤 <b>{name}</b>\n✅{ok} ❌{fail} ⏳{i}/{len(active)}",
                    chat_id=sm.chat.id, message_id=sm.message_id, parse_mode="HTML"
                )
            except: pass

    if reply_chat:
        await ctrl_bot.send_message(reply_chat,
            f"🎉 <b>{name}</b> завершена!\n✅ Доставлено: {ok}\n❌ Ошибок: {fail}", parse_mode="HTML")
    return ok, fail

async def cycle_run(cid: str):
    if cid not in data["cycles"] or not data["cycles"][cid].get("active"): return
    cyc = data["cycles"][cid]
    ok, fail = await do_broadcast(cyc["msg_id"])
    if OWNER_ID:
        try: await ctrl_bot.send_message(OWNER_ID, f"⏰ Цикл <b>{cyc['name']}</b> ✅{ok} ❌{fail}", parse_mode="HTML")
        except: pass

def reg_cycle(cid):
    scheduler.add_job(cycle_run, "interval", hours=data["cycles"][cid]["interval_hours"],
                      id=f"c_{cid}", replace_existing=True, args=[cid])

# ─── Инициализация aiogram ────────────────────────────────────────────────────
dp = Dispatcher(storage=MemoryStorage())

# ─── /start ───────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    await m.answer(
        "👋 <b>Бот рассылки — управление</b>\n\n"
        "📢 <i>Чаты</i> — добавь группы и каналы\n"
        "✉️ <i>Сообщения</i> — шаблоны текстов\n"
        "🚀 <i>Разослать</i> — отправить прямо сейчас\n"
        "🔄 <i>Циклы</i> — авторассылка каждые N часов",
        reply_markup=main_kb(), parse_mode="HTML"
    )

# ─── ЧАТЫ ─────────────────────────────────────────────────────────────────────
@dp.message(F.text == "📢 Чаты")
async def sec_chats(m: types.Message, state: FSMContext):
    await state.clear()
    g  = sum(1 for c in data["chats"].values() if c.get("type")=="group"   and c.get("active"))
    ch = sum(1 for c in data["chats"].values() if c.get("type")=="channel" and c.get("active"))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить чат/канал", callback_data="add_chat")],
        [InlineKeyboardButton(text="📋 Список чатов",       callback_data="lst_chats")],
        [InlineKeyboardButton(text="🗑 Удалить чат",        callback_data="del_chat_m")],
    ])
    await m.answer(f"📢 <b>Чаты для рассылки</b>\n\n👥 Групп: {g}  📣 Каналов: {ch}\nВсего: {len(data['chats'])}", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "add_chat")
async def cb_add_chat(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(ST.add_chat)
    await cq.message.answer(
        "➕ <b>Как добавить чат или канал:</b>\n\n"
        "<b>Вариант 1 (проще):</b>\nПерешли сюда любое сообщение из нужной группы или канала\n\n"
        "<b>Вариант 2:</b>\nВведи @username или числовой ID\n\n"
        "⚠️ Твой аккаунт должен быть <b>участником</b> группы или <b>админом</b> канала",
        parse_mode="HTML"
    )
    await cq.answer()

@dp.callback_query(F.data == "lst_chats")
async def cb_lst_chats(cq: types.CallbackQuery):
    if not data["chats"]:
        await cq.message.answer("📭 Чатов нет. Нажми ➕ Добавить"); await cq.answer(); return
    lines, btns = ["📋 <b>Список чатов:</b>\n"], []
    for cid, inf in data["chats"].items():
        e = "📣" if inf.get("type")=="channel" else "👥"
        s = "✅" if inf.get("active") else "⏸"
        lines.append(f"{s} {e} <b>{inf['title']}</b>")
        tog = "⏸ Откл" if inf.get("active") else "▶️ Вкл"
        btns.append([InlineKeyboardButton(text=f"{tog} — {inf['title'][:22]}", callback_data=f"tog:{cid}")])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode="HTML")
    await cq.answer()

@dp.callback_query(F.data.startswith("tog:"))
async def cb_tog(cq: types.CallbackQuery):
    cid = cq.data.split(":")[1]
    if cid in data["chats"]:
        data["chats"][cid]["active"] = not data["chats"][cid].get("active", True)
        save_data(data)
        s = "✅ включён" if data["chats"][cid]["active"] else "⏸ отключён"
        await cq.answer(f"{data['chats'][cid]['title']}: {s}")

@dp.callback_query(F.data == "del_chat_m")
async def cb_del_chat_m(cq: types.CallbackQuery):
    if not data["chats"]: await cq.answer("Нет чатов"); return
    btns = [[InlineKeyboardButton(text=f"🗑 {i['title']}", callback_data=f"dc:{c}")]
            for c, i in data["chats"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await cq.answer()

@dp.callback_query(F.data.startswith("dc:"))
async def cb_dc(cq: types.CallbackQuery):
    cid = cq.data.split(":")[1]
    if cid in data["chats"]:
        t = data["chats"].pop(cid)["title"]; save_data(data)
        await cq.answer(f"Удалён: {t}")
        await cq.message.answer(f"🗑 <b>{t}</b> удалён.", parse_mode="HTML")

# ─── СООБЩЕНИЯ ────────────────────────────────────────────────────────────────
@dp.message(F.text == "✉️ Сообщения")
async def sec_msgs(m: types.Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новое сообщение", callback_data="add_msg")],
        [InlineKeyboardButton(text="📋 Список",          callback_data="lst_msgs")],
        [InlineKeyboardButton(text="✏️ Редактировать",   callback_data="edit_msg_m")],
        [InlineKeyboardButton(text="🗑 Удалить",         callback_data="del_msg_m")],
    ])
    await m.answer(f"✉️ <b>Сообщения</b> — сохранено: {len(data['messages'])}", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "add_msg")
async def cb_add_msg(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(ST.msg_text)
    await cq.message.answer("✉️ Напиши текст сообщения для рассылки:")
    await cq.answer()

@dp.callback_query(F.data == "lst_msgs")
async def cb_lst_msgs(cq: types.CallbackQuery):
    if not data["messages"]: await cq.message.answer("📭 Сообщений нет."); await cq.answer(); return
    lines = ["✉️ <b>Сообщения:</b>\n"]
    for mid, inf in data["messages"].items():
        p = inf["text"][:80]+"…" if len(inf["text"])>80 else inf["text"]
        lines.append(f"📌 <b>{inf['name']}</b>\n└ {p}\n")
    await cq.message.answer("\n".join(lines), parse_mode="HTML"); await cq.answer()

@dp.callback_query(F.data == "edit_msg_m")
async def cb_edit_msg_m(cq: types.CallbackQuery):
    if not data["messages"]: await cq.answer("Нет сообщений"); return
    btns = [[InlineKeyboardButton(text=f"✏️ {i['name']}", callback_data=f"em:{mid}")]
            for mid, i in data["messages"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("em:"))
async def cb_em(cq: types.CallbackQuery, state: FSMContext):
    mid = cq.data.split(":")[1]
    await state.set_state(ST.edit_msg)
    await state.update_data(mid=mid)
    await cq.message.answer(
        f"✏️ <b>{data['messages'][mid]['name']}</b>\n\n{data['messages'][mid]['text']}\n\nНовый текст:",
        parse_mode="HTML"
    ); await cq.answer()

@dp.callback_query(F.data == "del_msg_m")
async def cb_del_msg_m(cq: types.CallbackQuery):
    if not data["messages"]: await cq.answer("Нет сообщений"); return
    btns = [[InlineKeyboardButton(text=f"🗑 {i['name']}", callback_data=f"dm:{mid}")]
            for mid, i in data["messages"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("dm:"))
async def cb_dm(cq: types.CallbackQuery):
    mid = cq.data.split(":")[1]
    if mid in data["messages"]:
        n = data["messages"].pop(mid)["name"]; save_data(data)
        await cq.answer(f"Удалено: {n}"); await cq.message.answer(f"🗑 <b>{n}</b> удалено.", parse_mode="HTML")

# ─── РАЗОСЛАТЬ ────────────────────────────────────────────────────────────────
@dp.message(F.text == "🚀 Разослать")
async def sec_send(m: types.Message, state: FSMContext):
    await state.clear()
    if not data["messages"]: await m.answer("❌ Нет сообщений. Добавь в ✉️ Сообщения"); return
    if not data["chats"]:   await m.answer("❌ Нет чатов. Добавь в 📢 Чаты"); return
    btns = [[InlineKeyboardButton(text=f"📤 {i['name']}", callback_data=f"sn:{mid}")]
            for mid, i in data["messages"].items()]
    btns.append([InlineKeyboardButton(text="📤 Все сообщения подряд", callback_data="sa")])
    await m.answer("Выбери сообщение:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))

@dp.callback_query(F.data.startswith("sn:"))
async def cb_sn(cq: types.CallbackQuery):
    await cq.answer("⏳ Начинаю рассылку...")
    await do_broadcast(cq.data.split(":")[1], cq.message.chat.id)

@dp.callback_query(F.data == "sa")
async def cb_sa(cq: types.CallbackQuery):
    await cq.answer("⏳ Отправляю все...")
    for mid in list(data["messages"]): await do_broadcast(mid, cq.message.chat.id)

# ─── ЦИКЛЫ ────────────────────────────────────────────────────────────────────
@dp.message(F.text == "🔄 Циклы")
async def sec_cycles(m: types.Message, state: FSMContext):
    await state.clear()
    ac = sum(1 for c in data["cycles"].values() if c.get("active"))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать цикл",   callback_data="add_cyc")],
        [InlineKeyboardButton(text="📋 Список",         callback_data="lst_cyc")],
        [InlineKeyboardButton(text="⏸/▶️ Вкл / Откл", callback_data="tog_cyc")],
        [InlineKeyboardButton(text="🗑 Удалить",        callback_data="del_cyc_m")],
    ])
    await m.answer(f"🔄 <b>Циклы</b> — активных: {ac}/{len(data['cycles'])}", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "add_cyc")
async def cb_add_cyc(cq: types.CallbackQuery):
    if not data["messages"]: await cq.answer("Сначала добавь сообщение!"); return
    btns = [[InlineKeyboardButton(text=i["name"], callback_data=f"cm:{mid}")]
            for mid, i in data["messages"].items()]
    await cq.message.answer("Шаг 1 — выбери сообщение:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await cq.answer()

@dp.callback_query(F.data.startswith("cm:"))
async def cb_cm(cq: types.CallbackQuery, state: FSMContext):
    mid = cq.data.split(":")[1]
    await state.set_state(ST.cyc_int)
    await state.update_data(mid=mid)
    await cq.message.answer("Шаг 2 — каждые сколько часов?\n\nПримеры: <code>1</code>, <code>6</code>, <code>12</code>, <code>24</code>", parse_mode="HTML")
    await cq.answer()

@dp.callback_query(F.data == "lst_cyc")
async def cb_lst_cyc(cq: types.CallbackQuery):
    if not data["cycles"]: await cq.message.answer("📭 Циклов нет."); await cq.answer(); return
    lines = ["🔄 <b>Циклы:</b>\n"]
    for cid, c in data["cycles"].items():
        s  = "✅" if c.get("active") else "⏸"
        mn = data["messages"].get(c["msg_id"],{}).get("name","?")
        lines.append(f"{s} <b>{c['name']}</b> — каждые {c['interval_hours']}ч\n└ {mn}\n")
    await cq.message.answer("\n".join(lines), parse_mode="HTML"); await cq.answer()

@dp.callback_query(F.data == "tog_cyc")
async def cb_tog_cyc(cq: types.CallbackQuery):
    if not data["cycles"]: await cq.answer("Нет циклов"); return
    btns = [[InlineKeyboardButton(
        text=f"{'⏸' if c.get('active') else '▶️'} {c['name']}", callback_data=f"tc:{cid}"
    )] for cid, c in data["cycles"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Управление:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("tc:"))
async def cb_tc(cq: types.CallbackQuery):
    cid = cq.data.split(":")[1]
    if cid in data["cycles"]:
        data["cycles"][cid]["active"] = not data["cycles"][cid].get("active", True)
        save_data(data)
        s = "▶️ запущен" if data["cycles"][cid]["active"] else "⏸ пауза"
        await cq.answer(f"{data['cycles'][cid]['name']}: {s}")

@dp.callback_query(F.data == "del_cyc_m")
async def cb_del_cyc_m(cq: types.CallbackQuery):
    if not data["cycles"]: await cq.answer("Нет циклов"); return
    btns = [[InlineKeyboardButton(text=f"🗑 {c['name']}", callback_data=f"dlc:{cid}")]
            for cid, c in data["cycles"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("dlc:"))
async def cb_dlc(cq: types.CallbackQuery):
    cid = cq.data.split(":")[1]
    if cid in data["cycles"]:
        n = data["cycles"].pop(cid)["name"]; save_data(data)
        try: scheduler.remove_job(f"c_{cid}")
        except: pass
        await cq.answer(f"Удалён: {n}"); await cq.message.answer(f"🗑 <b>{n}</b> удалён.", parse_mode="HTML")

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────
@dp.message(F.text == "⚙️ Настройки")
async def sec_settings(m: types.Message, state: FSMContext):
    await state.clear()
    d = data["settings"]["delay"]
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"⏱ Задержка: {d}с — изменить", callback_data="chd")
    ]])
    await m.answer(f"⚙️ <b>Настройки</b>\n\nЗадержка: <b>{d} сек.</b>\n<i>рекомендуется 3–5</i>", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "chd")
async def cb_chd(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(ST.chdelay)
    await cq.message.answer("Введи задержку в секундах (3–5 рекомендуется):"); await cq.answer()

# ─── СТАТИСТИКА ───────────────────────────────────────────────────────────────
@dp.message(F.text == "📊 Статистика")
async def sec_stats(m: types.Message):
    g  = sum(1 for c in data["chats"].values() if c.get("type")=="group")
    ch = sum(1 for c in data["chats"].values() if c.get("type")=="channel")
    ac = sum(1 for c in data["chats"].values() if c.get("active"))
    cy = sum(1 for c in data["cycles"].values() if c.get("active"))
    await m.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"📣 Каналов: {ch} · 👥 Групп: {g}\n"
        f"✅ Активных: {ac} / {len(data['chats'])}\n\n"
        f"✉️ Сообщений: {len(data['messages'])}\n"
        f"🔄 Активных циклов: {cy} / {len(data['cycles'])}\n\n"
        f"⏱ Задержка: {data['settings']['delay']} сек.",
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "bk")
async def cb_bk(cq: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.answer("Главное меню:", reply_markup=main_kb()); await cq.answer()

# ─── FSM: пересланные сообщения ───────────────────────────────────────────────
@dp.message(StateFilter(ST.add_chat), F.forward_from_chat)
async def handle_fwd_chat(m: types.Message, state: FSMContext):
    fc    = m.forward_from_chat
    cid   = str(fc.id)
    ctype = "channel" if fc.type == "channel" else "group"
    title = fc.title or cid
    emoji = "📣" if ctype == "channel" else "👥"
    if cid in data["chats"]:
        await m.answer(f"⚠️ {emoji} <b>{title}</b> уже в списке.", parse_mode="HTML")
    else:
        data["chats"][cid] = {"title": title, "type": ctype, "active": True, "added": str(datetime.now())}
        save_data(data)
        await m.answer(f"✅ {emoji} <b>{title}</b> добавлен!\n\nМожешь перешли ещё или нажми 🚀 Разослать",
                       reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()

# ─── FSM: текст ───────────────────────────────────────────────────────────────
@dp.message(StateFilter(ST.add_chat))
async def handle_add_chat_text(m: types.Message, state: FSMContext):
    try:
        entity = await userbot_tl.get_entity(m.text.strip())
        cid    = str(entity.id)
        ctype  = "channel" if hasattr(entity, "broadcast") and entity.broadcast else "group"
        title  = getattr(entity, "title", cid)
        emoji  = "📣" if ctype == "channel" else "👥"
        if cid in data["chats"]:
            await m.answer(f"⚠️ {emoji} <b>{title}</b> уже в списке.", parse_mode="HTML")
        else:
            data["chats"][cid] = {"title": title, "type": ctype, "active": True, "added": str(datetime.now())}
            save_data(data)
            await m.answer(f"✅ {emoji} <b>{title}</b> добавлен!", reply_markup=main_kb(), parse_mode="HTML")
    except Exception as e:
        await m.answer(f"❌ Не нашёл чат. Попробуй переслать сообщение.\n\n<code>{e}</code>", parse_mode="HTML")
    await state.clear()

@dp.message(StateFilter(ST.msg_text))
async def fsm_msg_text(m: types.Message, state: FSMContext):
    await state.update_data(text=m.text)
    await state.set_state(ST.msg_name)
    await m.answer("Придумай короткое название для этого сообщения:")

@dp.message(StateFilter(ST.msg_name))
async def fsm_msg_name(m: types.Message, state: FSMContext):
    d = await state.get_data()
    mid = nid(data["messages"])
    data["messages"][mid] = {"name": m.text, "text": d["text"]}
    save_data(data)
    await m.answer(f"✅ Сообщение <b>{m.text}</b> сохранено!", reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()

@dp.message(StateFilter(ST.edit_msg))
async def fsm_edit_msg(m: types.Message, state: FSMContext):
    d = await state.get_data()
    data["messages"][d["mid"]]["text"] = m.text
    save_data(data)
    await m.answer("✅ Текст обновлён!", reply_markup=main_kb())
    await state.clear()

@dp.message(StateFilter(ST.cyc_int))
async def fsm_cyc_int(m: types.Message, state: FSMContext):
    try:
        h = float(m.text.strip().replace(",", "."))
        if h <= 0: raise ValueError
    except:
        await m.answer("❌ Введи число, например <code>6</code>", parse_mode="HTML"); return
    await state.update_data(h=h)
    await state.set_state(ST.cyc_name)
    await m.answer(f"⏱ Каждые <b>{h} ч.</b> ✅\n\nДай название этому циклу:", parse_mode="HTML")

@dp.message(StateFilter(ST.cyc_name))
async def fsm_cyc_name(m: types.Message, state: FSMContext):
    d   = await state.get_data()
    cid = nid(data["cycles"])
    data["cycles"][cid] = {
        "name": m.text, "msg_id": d["mid"],
        "interval_hours": d["h"], "active": True,
        "created": str(datetime.now())
    }
    save_data(data)
    reg_cycle(cid)
    mn = data["messages"].get(d["mid"], {}).get("name", "?")
    await m.answer(
        f"✅ Цикл <b>{m.text}</b> создан!\n\n📨 {mn}\n⏱ Каждые {d['h']} ч.\n🟢 Активен",
        reply_markup=main_kb(), parse_mode="HTML"
    )
    await state.clear()

@dp.message(StateFilter(ST.chdelay))
async def fsm_chdelay(m: types.Message, state: FSMContext):
    try:
        d = float(m.text.strip())
        if d < 0: raise ValueError
    except:
        await m.answer("❌ Введи число секунд"); return
    data["settings"]["delay"] = d
    save_data(data)
    await m.answer(f"✅ Задержка: <b>{d} сек.</b>", reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  Запуск
# ══════════════════════════════════════════════════════════════════════════════
def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)

async def main():
    global userbot_tl, ctrl_bot

    # Flask в фоне
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info(f"🌐 Веб-сервер на порту {PORT}")

    if not SESSION_STR:
        logger.info("⚠️  SESSION_STRING пуст — открой URL сервиса в браузере для авторизации")
        # Держим сервис живым
        await asyncio.Event().wait()
        return

    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN не задан!")
        await asyncio.Event().wait()
        return

    # Userbot (Telethon)
    userbot_tl = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
    await userbot_tl.start()
    me = await userbot_tl.get_me()
    logger.info(f"✅ Userbot: {me.first_name} (@{me.username})")

    # Управляющий бот (aiogram)
    ctrl_bot = Bot(token=BOT_TOKEN)

    # Восстанавливаем циклы
    for cid in list(data.get("cycles", {})):
        if data["cycles"][cid].get("active"):
            reg_cycle(cid)
    scheduler.start()

    # Уведомляем владельца
    if OWNER_ID:
        try:
            await ctrl_bot.send_message(OWNER_ID,
                f"✅ <b>Бот запущен!</b>\n👤 Userbot: <b>{me.first_name}</b>\nНажми /start",
                parse_mode="HTML")
        except Exception as e:
            logger.error(f"Не могу написать владельцу: {e}")

    logger.info("🚀 Запускаю polling...")
    await dp.start_polling(ctrl_bot)

if __name__ == "__main__":
    asyncio.run(main())
