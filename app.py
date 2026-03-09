import asyncio
import json
import logging
import os
import threading
from datetime import datetime

from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.auth import SignInRequest
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_ID      = int(os.environ.get("API_ID", "0"))
API_HASH    = os.environ.get("API_HASH", "")
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
OWNER_ID    = int(os.environ.get("OWNER_ID", "0"))
SESSION_STR = os.environ.get("SESSION_STRING", "").strip()
PORT        = int(os.environ.get("PORT", "8080"))
DATA_FILE   = "data.json"

def load():
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {"chats": {}, "messages": {}, "cycles": {}, "settings": {"delay": 3}}

def dump(d):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

db = load()

# ══════════════════════════════
#  FLASK  (страница авторизации)
# ══════════════════════════════

web = Flask(__name__)

AUTH = r"""<!DOCTYPE html><html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Авторизация</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,sans-serif;background:#0d0d1a;color:#e0e0e0;
min-height:100vh;display:flex;align-items:center;justify-content:center;padding:16px}
.c{background:#161625;border:1px solid #252540;border-radius:20px;
padding:36px 32px;width:100%;max-width:440px}
.ico{font-size:52px;text-align:center;margin-bottom:10px}
h1{text-align:center;font-size:22px;color:#fff;margin-bottom:6px}
.sub{text-align:center;color:#666;font-size:14px;margin-bottom:24px}
.dots{display:flex;justify-content:center;gap:10px;margin-bottom:24px}
.dot{width:32px;height:4px;border-radius:2px;background:#252540;transition:.3s}
.dot.on{background:#5b6ef5}.dot.done{background:#43b581}
label{display:block;font-size:12px;color:#888;font-weight:600;
text-transform:uppercase;letter-spacing:.6px;margin-bottom:8px}
input{width:100%;padding:13px 15px;background:#0d0d1a;border:1px solid #252540;
border-radius:12px;color:#fff;font-size:16px;outline:none;
transition:border-color .2s;margin-bottom:18px}
input:focus{border-color:#5b6ef5}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#5b6ef5,#4457d4);
border:none;border-radius:12px;color:#fff;font-size:15px;font-weight:600;
cursor:pointer;margin-bottom:10px;transition:opacity .2s}
.btn:hover{opacity:.9}.btn:disabled{opacity:.4;cursor:not-allowed}
.btn.g{background:#252540}
.box{background:#0d0d1a;border:1px solid #252540;border-radius:12px;
padding:13px 15px;font-size:13px;color:#888;line-height:1.7;margin-bottom:18px}
.box b{color:#ccc}
.err{color:#f04747;font-size:13px;padding:11px 14px;
background:rgba(240,71,71,.1);border:1px solid rgba(240,71,71,.3);
border-radius:10px;margin-bottom:14px;display:none}
.sess{background:#0d0d1a;border:2px solid #43b581;border-radius:12px;
padding:14px;font-size:11px;color:#43b581;word-break:break-all;
font-family:monospace;line-height:1.5;max-height:110px;overflow-y:auto;margin-bottom:14px}
.warn{color:#faa61a;font-size:13px;text-align:center;margin-bottom:14px}
.step{display:none}.step.on{display:block}
.big{font-size:56px;text-align:center;margin-bottom:14px}
</style></head><body><div class="c">
<div class="ico">📡</div>
<h1>Telegram Userbot</h1>
<p class="sub">Авторизация для рассылки от твоего аккаунта</p>
<div class="dots">
<div class="dot on" id="d1"></div>
<div class="dot" id="d2"></div>
<div class="dot" id="d3"></div>
</div>

<div class="step on" id="s1">
<div class="box">Введи номер телефона аккаунта Telegram, <b>от имени которого</b> будет рассылка.</div>
<label>Номер телефона</label>
<input id="ph" type="tel" placeholder="+380501234567" autofocus>
<div class="err" id="e1"></div>
<button class="btn" id="b1" onclick="doPhone()">Получить код →</button>
</div>

<div class="step" id="s2">
<div class="box">Код пришёл в Telegram.<br>Открой <b>Saved Messages (Избранное)</b> — там 5 цифр.</div>
<label>Код из Telegram</label>
<input id="co" type="number" placeholder="12345">
<div class="err" id="e2"></div>
<button class="btn" id="b2" onclick="doCode()">Войти →</button>
<button class="btn g" onclick="go(1)">← Назад</button>
</div>

<div class="step" id="s3">
<div class="big">🎉</div>
<div class="box" style="text-align:center;margin-bottom:16px">
<b>Успешно!</b> Скопируй SESSION_STRING ниже</div>
<div class="sess" id="sv"></div>
<p class="warn">⚠️ Никому не показывай эту строку!</p>
<button class="btn" onclick="cp()">📋 Скопировать SESSION_STRING</button>
<div class="box" style="margin-top:14px;font-size:13px">
<b>Что дальше:</b><br>
1. Render → сервис → <b>Environment</b><br>
2. Найди <b>SESSION_STRING</b> → вставь → <b>Save Changes</b><br>
3. Сервис перезапустится — бот напишет тебе ✅
</div>
</div>
</div>
<script>
let hash='';
async function p(url,b){
const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});
return r.json();
}
async function doPhone(){
const ph=document.getElementById('ph').value.trim();
if(!ph){err('e1','Введи номер');return;}
bl('b1',1,'Отправляю...');he('e1');
try{const r=await p('/api/sc',{phone:ph});
if(r.ok){hash=r.hash;go(2);}else err('e1',r.error||'Ошибка');}
catch(e){err('e1',''+e);}
bl('b1',0,'Получить код →');
}
async function doCode(){
const ph=document.getElementById('ph').value.trim();
const co=document.getElementById('co').value.trim();
if(!co){err('e2','Введи код');return;}
bl('b2',1,'Проверяю...');he('e2');
try{const r=await p('/api/vc',{phone:ph,code:co,hash});
if(r.ok){document.getElementById('sv').textContent=r.session;go(3);}
else err('e2',r.error||'Неверный код');}
catch(e){err('e2',''+e);}
bl('b2',0,'Войти →');
}
function cp(){
navigator.clipboard.writeText(document.getElementById('sv').textContent).then(()=>{
const b=event.target;b.textContent='✅ Скопировано!';
setTimeout(()=>b.textContent='📋 Скопировать SESSION_STRING',2500);});
}
function go(n){
[1,2,3].forEach(i=>{
document.getElementById('s'+i).classList.toggle('on',i===n);
const d=document.getElementById('d'+i);
d.classList.remove('on','done');
if(i<n)d.classList.add('done');else if(i===n)d.classList.add('on');
});
if(n===2)setTimeout(()=>document.getElementById('co').focus(),100);
}
function err(id,msg){const e=document.getElementById(id);e.textContent=msg;e.style.display='block';}
function he(id){document.getElementById(id).style.display='none';}
function bl(id,b,t){const e=document.getElementById(id);e.disabled=!!b;e.textContent=t;}
document.addEventListener('keydown',e=>{
if(e.key!=='Enter')return;
if(document.getElementById('s1').classList.contains('on'))doPhone();
else if(document.getElementById('s2').classList.contains('on'))doCode();
});
</script></body></html>"""

DONE = """<!DOCTYPE html><html><head><meta charset="UTF-8"><title>OK</title>
<style>body{font-family:sans-serif;background:#0d0d1a;color:#e0e0e0;
display:flex;align-items:center;justify-content:center;min-height:100vh}
.c{background:#161625;border:1px solid #252540;border-radius:20px;
padding:40px;max-width:380px;text-align:center}
.i{font-size:64px;margin-bottom:16px}h1{color:#43b581;margin-bottom:10px}
p{color:#888;font-size:14px;line-height:1.7}</style></head>
<body><div class="c"><div class="i">✅</div><h1>Бот запущен!</h1>
<p>Userbot работает.<br>Управляй через Telegram бота.</p>
</div></body></html>"""

_aloop = None
_atl   = None

def get_aloop():
    global _aloop
    if _aloop is None or _aloop.is_closed():
        _aloop = asyncio.new_event_loop()
        threading.Thread(target=_aloop.run_forever, daemon=True).start()
    return _aloop

@web.route("/")
def index():
    return DONE if SESSION_STR else AUTH

@web.route("/api/sc", methods=["POST"])
def api_sc():
    global _atl
    if not API_ID or not API_HASH:
        return jsonify({"ok": False, "error": "API_ID / API_HASH не заданы!"})
    phone = (request.get_json() or {}).get("phone", "").strip()
    loop  = get_aloop()
    async def _do():
        global _atl
        _atl = TelegramClient(StringSession(), API_ID, API_HASH, loop=loop)
        await _atl.connect()
        r = await _atl.send_code_request(phone)
        return r.phone_code_hash
    try:
        h = asyncio.run_coroutine_threadsafe(_do(), loop).result(30)
        return jsonify({"ok": True, "hash": h})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@web.route("/api/vc", methods=["POST"])
def api_vc():
    global _atl
    body  = request.get_json() or {}
    phone = body.get("phone","").strip()
    code  = body.get("code","").strip()
    h     = body.get("hash","").strip()
    loop  = get_aloop()
    async def _do():
        await _atl(SignInRequest(phone_number=phone, phone_code_hash=h, phone_code=code))
        s = _atl.session.save()
        await _atl.disconnect()
        return s
    try:
        s = asyncio.run_coroutine_threadsafe(_do(), loop).result(30)
        return jsonify({"ok": True, "session": s})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@web.route("/health")
def health():
    return jsonify({"ok": True})

# ══════════════════════════════
#  AIOGRAM  (управляющий бот)
# ══════════════════════════════

class S(StatesGroup):
    add_chat = State()
    msg_text = State(); msg_name = State()
    edit_msg = State()
    ci = State(); cn = State()
    delay    = State()

dp  = Dispatcher(storage=MemoryStorage())
bot: Bot = None
utl: TelegramClient = None
sch = AsyncIOScheduler()

def mkb(*rows):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t, callback_data=c) for t, c in row
    ] for row in rows])

def main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📢 Чаты"),      KeyboardButton(text="✉️ Сообщения")],
        [KeyboardButton(text="🚀 Разослать"), KeyboardButton(text="🔄 Циклы")],
        [KeyboardButton(text="⚙️ Настройки"),KeyboardButton(text="📊 Статус")],
    ], resize_keyboard=True)

def nid(d):
    return str(max((int(k) for k in d if k.isdigit()), default=0) + 1)

async def broadcast(mid: str, cid=None):
    if mid not in db["messages"]: return 0, 0
    txt    = db["messages"][mid]["text"]
    name   = db["messages"][mid]["name"]
    active = {c: i for c, i in db["chats"].items() if i.get("active")}
    if not active:
        if cid: await bot.send_message(cid, "⚠️ Нет активных чатов!")
        return 0, 0
    ok = fail = 0
    sm = None
    if cid:
        sm = await bot.send_message(cid, f"📤 <b>{name}</b>\n⏳ 0/{len(active)}", parse_mode="HTML")
    import random
    for i, (chat_id, info) in enumerate(active.items(), 1):
        try:
            await utl.send_message(int(chat_id), txt)
            ok += 1
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            try: await utl.send_message(int(chat_id), txt); ok += 1
            except: fail += 1
        except Exception as e:
            log.error(f"send {chat_id}: {e}"); fail += 1
        await asyncio.sleep(max(1, db["settings"]["delay"] + random.uniform(0, 1)))
        if sm and (i % 5 == 0 or i == len(active)):
            try:
                await bot.edit_message_text(
                    f"📤 <b>{name}</b>\n✅{ok} ❌{fail} ⏳{i}/{len(active)}",
                    chat_id=sm.chat.id, message_id=sm.message_id, parse_mode="HTML")
            except: pass
    if cid:
        await bot.send_message(cid,
            f"🎉 <b>{name}</b> завершена!\n✅ {ok}\n❌ {fail}", parse_mode="HTML")
    return ok, fail

async def cyc_run(cid: str):
    if cid not in db["cycles"] or not db["cycles"][cid].get("active"): return
    ok, fail = await broadcast(db["cycles"][cid]["msg_id"])
    if OWNER_ID:
        try: await bot.send_message(OWNER_ID,
            f"⏰ Цикл <b>{db['cycles'][cid]['name']}</b> ✅{ok} ❌{fail}", parse_mode="HTML")
        except: pass

def reg(cid):
    sch.add_job(cyc_run, "interval", hours=db["cycles"][cid]["interval_hours"],
                id=f"c{cid}", replace_existing=True, args=[cid])

# /start
@dp.message(Command("start"))
async def cmd_start(m: types.Message, state: FSMContext):
    await state.clear()
    await m.answer("👋 <b>Бот рассылки</b>\n\nВыбери раздел 👇",
                   reply_markup=main_kb(), parse_mode="HTML")

# ── ЧАТЫ ──────────────────────────────────────────────────────
@dp.message(F.text == "📢 Чаты")
async def s_chats(m: types.Message, state: FSMContext):
    await state.clear()
    g  = sum(1 for c in db["chats"].values() if c.get("type")=="group"   and c.get("active"))
    ch = sum(1 for c in db["chats"].values() if c.get("type")=="channel" and c.get("active"))
    await m.answer(
        f"📢 <b>Чаты</b>\n👥 Групп: {g}  📣 Каналов: {ch}\nВсего: {len(db['chats'])}",
        parse_mode="HTML",
        reply_markup=mkb(
            [("➕ Добавить","ac")],
            [("📋 Список","lc")],
            [("🗑 Удалить","dc")],
        ))

@dp.callback_query(F.data == "ac")
async def cb_ac(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(S.add_chat)
    await cq.message.answer(
        "➕ <b>Добавить чат/канал:</b>\n\n"
        "• Перешли сюда любое сообщение из нужного чата/канала\n"
        "• Или введи @username / числовой ID\n\n"
        "⚠️ Твой аккаунт должен быть участником группы или админом канала",
        parse_mode="HTML")
    await cq.answer()

@dp.callback_query(F.data == "lc")
async def cb_lc(cq: types.CallbackQuery):
    if not db["chats"]:
        await cq.message.answer("📭 Чатов нет. Нажми ➕ Добавить"); await cq.answer(); return
    lines = ["📋 <b>Чаты:</b>\n"]
    btns  = []
    for cid, inf in db["chats"].items():
        e = "📣" if inf.get("type")=="channel" else "👥"
        s = "✅" if inf.get("active") else "⏸"
        lines.append(f"{s}{e} <b>{inf['title']}</b>")
        t = "⏸ Откл" if inf.get("active") else "▶️ Вкл"
        btns.append([InlineKeyboardButton(text=f"{t} {inf['title'][:22]}", callback_data=f"tg:{cid}")])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("\n".join(lines),
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await cq.answer()

@dp.callback_query(F.data.startswith("tg:"))
async def cb_tg(cq: types.CallbackQuery):
    cid = cq.data[3:]
    if cid in db["chats"]:
        db["chats"][cid]["active"] = not db["chats"][cid].get("active", True)
        dump(db)
        s = "✅" if db["chats"][cid]["active"] else "⏸"
        await cq.answer(f"{s} {db['chats'][cid]['title']}")

@dp.callback_query(F.data == "dc")
async def cb_dc(cq: types.CallbackQuery):
    if not db["chats"]: await cq.answer("Нет чатов"); return
    btns = [[InlineKeyboardButton(text=f"🗑 {i['title']}", callback_data=f"dr:{c}")]
            for c, i in db["chats"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await cq.answer()

@dp.callback_query(F.data.startswith("dr:"))
async def cb_dr(cq: types.CallbackQuery):
    cid = cq.data[3:]
    if cid in db["chats"]:
        t = db["chats"].pop(cid)["title"]; dump(db)
        await cq.answer(f"Удалён: {t}")
        await cq.message.answer(f"🗑 <b>{t}</b> удалён.", parse_mode="HTML")

# Пересланное сообщение
@dp.message(StateFilter(S.add_chat), F.forward_from_chat)
async def fwd_chat(m: types.Message, state: FSMContext):
    fc    = m.forward_from_chat
    cid   = str(fc.id)
    ctype = "channel" if fc.type == "channel" else "group"
    title = fc.title or cid
    e     = "📣" if ctype == "channel" else "👥"
    if cid in db["chats"]:
        await m.answer(f"⚠️ {e} <b>{title}</b> уже в списке.", parse_mode="HTML")
    else:
        db["chats"][cid] = {"title": title, "type": ctype, "active": True}
        dump(db)
        await m.answer(f"✅ {e} <b>{title}</b> добавлен!", reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()

@dp.message(StateFilter(S.add_chat))
async def add_chat_text(m: types.Message, state: FSMContext):
    try:
        ent   = await utl.get_entity(m.text.strip())
        cid   = str(ent.id)
        ctype = "channel" if getattr(ent, "broadcast", False) else "group"
        title = getattr(ent, "title", cid)
        e     = "📣" if ctype == "channel" else "👥"
        if cid in db["chats"]:
            await m.answer(f"⚠️ {e} <b>{title}</b> уже в списке.", parse_mode="HTML")
        else:
            db["chats"][cid] = {"title": title, "type": ctype, "active": True}
            dump(db)
            await m.answer(f"✅ {e} <b>{title}</b> добавлен!", reply_markup=main_kb(), parse_mode="HTML")
    except Exception as ex:
        await m.answer(f"❌ Не нашёл чат.\nПопробуй переслать сообщение.\n<code>{ex}</code>", parse_mode="HTML")
    await state.clear()

# ── СООБЩЕНИЯ ─────────────────────────────────────────────────
@dp.message(F.text == "✉️ Сообщения")
async def s_msgs(m: types.Message, state: FSMContext):
    await state.clear()
    await m.answer(f"✉️ <b>Сообщения</b> — {len(db['messages'])} шт.",
        parse_mode="HTML",
        reply_markup=mkb(
            [("➕ Новое","am")],
            [("📋 Список","lm")],
            [("✏️ Редактировать","em"), ("🗑 Удалить","dm")],
        ))

@dp.callback_query(F.data == "am")
async def cb_am(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(S.msg_text)
    await cq.message.answer("✉️ Напиши текст сообщения для рассылки:")
    await cq.answer()

@dp.callback_query(F.data == "lm")
async def cb_lm(cq: types.CallbackQuery):
    if not db["messages"]: await cq.message.answer("📭 Пусто."); await cq.answer(); return
    lines = ["✉️ <b>Сообщения:</b>\n"]
    for mid, inf in db["messages"].items():
        p = inf["text"][:80]+"…" if len(inf["text"])>80 else inf["text"]
        lines.append(f"📌 <b>{inf['name']}</b>\n└ {p}\n")
    await cq.message.answer("\n".join(lines), parse_mode="HTML"); await cq.answer()

@dp.callback_query(F.data == "em")
async def cb_em(cq: types.CallbackQuery):
    if not db["messages"]: await cq.answer("Нет сообщений"); return
    btns = [[InlineKeyboardButton(text=f"✏️ {i['name']}", callback_data=f"ex:{mid}")]
            for mid, i in db["messages"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("ex:"))
async def cb_ex(cq: types.CallbackQuery, state: FSMContext):
    mid = cq.data[3:]
    await state.set_state(S.edit_msg); await state.update_data(mid=mid)
    await cq.message.answer(
        f"✏️ <b>{db['messages'][mid]['name']}</b>\n\n{db['messages'][mid]['text']}\n\nНовый текст:",
        parse_mode="HTML"); await cq.answer()

@dp.callback_query(F.data == "dm")
async def cb_dm(cq: types.CallbackQuery):
    if not db["messages"]: await cq.answer("Нет сообщений"); return
    btns = [[InlineKeyboardButton(text=f"🗑 {i['name']}", callback_data=f"dx:{mid}")]
            for mid, i in db["messages"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("dx:"))
async def cb_dx(cq: types.CallbackQuery):
    mid = cq.data[3:]
    if mid in db["messages"]:
        n = db["messages"].pop(mid)["name"]; dump(db)
        await cq.answer(f"Удалено: {n}"); await cq.message.answer(f"🗑 <b>{n}</b>.", parse_mode="HTML")

# ── РАЗОСЛАТЬ ─────────────────────────────────────────────────
@dp.message(F.text == "🚀 Разослать")
async def s_send(m: types.Message, state: FSMContext):
    await state.clear()
    if not db["messages"]: await m.answer("❌ Нет сообщений"); return
    if not db["chats"]:   await m.answer("❌ Нет чатов"); return
    btns = [[InlineKeyboardButton(text=f"📤 {i['name']}", callback_data=f"sn:{mid}")]
            for mid, i in db["messages"].items()]
    btns.append([InlineKeyboardButton(text="📤 Все сразу", callback_data="sa")])
    await m.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))

@dp.callback_query(F.data.startswith("sn:"))
async def cb_sn(cq: types.CallbackQuery):
    await cq.answer("⏳ Начинаю...")
    await broadcast(cq.data[3:], cq.message.chat.id)

@dp.callback_query(F.data == "sa")
async def cb_sa(cq: types.CallbackQuery):
    await cq.answer("⏳ Отправляю все...")
    for mid in list(db["messages"]): await broadcast(mid, cq.message.chat.id)

# ── ЦИКЛЫ ─────────────────────────────────────────────────────
@dp.message(F.text == "🔄 Циклы")
async def s_cyc(m: types.Message, state: FSMContext):
    await state.clear()
    ac = sum(1 for c in db["cycles"].values() if c.get("active"))
    await m.answer(f"🔄 <b>Циклы</b> — {ac}/{len(db['cycles'])}",
        parse_mode="HTML",
        reply_markup=mkb(
            [("➕ Создать","addcyc")],
            [("📋 Список","lstcyc")],
            [("⏸/▶️ Вкл/Откл","togcyc"),("🗑 Удалить","delcyc")],
        ))

@dp.callback_query(F.data == "addcyc")
async def cb_addcyc(cq: types.CallbackQuery):
    if not db["messages"]: await cq.answer("Сначала добавь сообщение!"); return
    btns = [[InlineKeyboardButton(text=i["name"], callback_data=f"cm:{mid}")]
            for mid, i in db["messages"].items()]
    await cq.message.answer("Шаг 1 — выбери сообщение:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("cm:"))
async def cb_cm(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(S.ci); await state.update_data(mid=cq.data[3:])
    await cq.message.answer(
        "Шаг 2 — каждые сколько часов?\nПримеры: <code>1</code> <code>6</code> <code>24</code>",
        parse_mode="HTML"); await cq.answer()

@dp.callback_query(F.data == "lstcyc")
async def cb_lstcyc(cq: types.CallbackQuery):
    if not db["cycles"]: await cq.message.answer("📭 Пусто."); await cq.answer(); return
    lines = ["🔄 <b>Циклы:</b>\n"]
    for cid, c in db["cycles"].items():
        s  = "✅" if c.get("active") else "⏸"
        mn = db["messages"].get(c["msg_id"],{}).get("name","?")
        lines.append(f"{s} <b>{c['name']}</b> — {c['interval_hours']}ч · {mn}\n")
    await cq.message.answer("\n".join(lines), parse_mode="HTML"); await cq.answer()

@dp.callback_query(F.data == "togcyc")
async def cb_togcyc(cq: types.CallbackQuery):
    if not db["cycles"]: await cq.answer("Нет циклов"); return
    btns = [[InlineKeyboardButton(
        text=f"{'⏸' if c.get('active') else '▶️'} {c['name']}", callback_data=f"tc:{cid}"
    )] for cid, c in db["cycles"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Управление:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("tc:"))
async def cb_tc(cq: types.CallbackQuery):
    cid = cq.data[3:]
    if cid in db["cycles"]:
        db["cycles"][cid]["active"] = not db["cycles"][cid].get("active", True)
        dump(db); s = "▶️" if db["cycles"][cid]["active"] else "⏸"
        await cq.answer(f"{s} {db['cycles'][cid]['name']}")

@dp.callback_query(F.data == "delcyc")
async def cb_delcyc(cq: types.CallbackQuery):
    if not db["cycles"]: await cq.answer("Нет циклов"); return
    btns = [[InlineKeyboardButton(text=f"🗑 {c['name']}", callback_data=f"dlc:{cid}")]
            for cid, c in db["cycles"].items()]
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bk")])
    await cq.message.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns)); await cq.answer()

@dp.callback_query(F.data.startswith("dlc:"))
async def cb_dlc(cq: types.CallbackQuery):
    cid = cq.data[3:]
    if cid in db["cycles"]:
        n = db["cycles"].pop(cid)["name"]; dump(db)
        try: sch.remove_job(f"c{cid}")
        except: pass
        await cq.answer(f"Удалён: {n}"); await cq.message.answer(f"🗑 <b>{n}</b>.", parse_mode="HTML")

# ── НАСТРОЙКИ / СТАТУС ────────────────────────────────────────
@dp.message(F.text == "⚙️ Настройки")
async def s_sett(m: types.Message, state: FSMContext):
    await state.clear()
    d = db["settings"]["delay"]
    await m.answer(f"⚙️ <b>Настройки</b>\n\nЗадержка: <b>{d} сек.</b>",
        parse_mode="HTML",
        reply_markup=mkb([("⏱ Изменить задержку","chd")]))

@dp.callback_query(F.data == "chd")
async def cb_chd(cq: types.CallbackQuery, state: FSMContext):
    await state.set_state(S.delay)
    await cq.message.answer("Введи задержку в секундах (3–5 рекомендуется):"); await cq.answer()

@dp.message(F.text == "📊 Статус")
async def s_stat(m: types.Message):
    g  = sum(1 for c in db["chats"].values() if c.get("type")=="group")
    ch = sum(1 for c in db["chats"].values() if c.get("type")=="channel")
    ac = sum(1 for c in db["chats"].values() if c.get("active"))
    cy = sum(1 for c in db["cycles"].values() if c.get("active"))
    await m.answer(
        f"📊 <b>Статус</b>\n\n"
        f"📣 Каналов: {ch}  👥 Групп: {g}\n"
        f"✅ Активных: {ac}/{len(db['chats'])}\n\n"
        f"✉️ Сообщений: {len(db['messages'])}\n"
        f"🔄 Циклов активных: {cy}/{len(db['cycles'])}\n\n"
        f"⏱ Задержка: {db['settings']['delay']} сек.",
        parse_mode="HTML")

@dp.callback_query(F.data == "bk")
async def cb_bk(cq: types.CallbackQuery, state: FSMContext):
    await state.clear(); await cq.message.answer("👇", reply_markup=main_kb()); await cq.answer()

# ── FSM обработчики текста ────────────────────────────────────
@dp.message(StateFilter(S.msg_text))
async def fsm_mt(m: types.Message, state: FSMContext):
    await state.update_data(text=m.text); await state.set_state(S.msg_name)
    await m.answer("Дай название этому сообщению:")

@dp.message(StateFilter(S.msg_name))
async def fsm_mn(m: types.Message, state: FSMContext):
    d = await state.get_data(); mid = nid(db["messages"])
    db["messages"][mid] = {"name": m.text, "text": d["text"]}; dump(db)
    await m.answer(f"✅ <b>{m.text}</b> сохранено!", reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()

@dp.message(StateFilter(S.edit_msg))
async def fsm_em(m: types.Message, state: FSMContext):
    d = await state.get_data(); db["messages"][d["mid"]]["text"] = m.text; dump(db)
    await m.answer("✅ Обновлено!", reply_markup=main_kb()); await state.clear()

@dp.message(StateFilter(S.ci))
async def fsm_ci(m: types.Message, state: FSMContext):
    try:
        h = float(m.text.strip().replace(",",".")); assert h > 0
    except:
        await m.answer("❌ Введи число, например <code>6</code>", parse_mode="HTML"); return
    await state.update_data(h=h); await state.set_state(S.cn)
    await m.answer(f"⏱ Каждые <b>{h}ч</b> ✅\n\nДай название циклу:", parse_mode="HTML")

@dp.message(StateFilter(S.cn))
async def fsm_cn(m: types.Message, state: FSMContext):
    d = await state.get_data(); cid = nid(db["cycles"])
    db["cycles"][cid] = {"name": m.text, "msg_id": d["mid"],
                         "interval_hours": d["h"], "active": True}
    dump(db); reg(cid)
    mn = db["messages"].get(d["mid"],{}).get("name","?")
    await m.answer(
        f"✅ Цикл <b>{m.text}</b> создан!\n📨 {mn}\n⏱ Каждые {d['h']}ч",
        reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()

@dp.message(StateFilter(S.delay))
async def fsm_delay(m: types.Message, state: FSMContext):
    try:
        d = float(m.text.strip()); assert d >= 0
    except:
        await m.answer("❌ Введи число"); return
    db["settings"]["delay"] = d; dump(db)
    await m.answer(f"✅ Задержка: <b>{d} сек.</b>", reply_markup=main_kb(), parse_mode="HTML")
    await state.clear()

# ══════════════════════════════
#  ЗАПУСК
# ══════════════════════════════

def run_web():
    web.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)

async def main():
    global bot, utl

    threading.Thread(target=run_web, daemon=True).start()
    log.info(f"🌐 Flask на порту {PORT}")

    if not SESSION_STR:
        log.info("⚠️  SESSION_STRING пуст — открой URL сервиса в браузере для авторизации")
        await asyncio.Event().wait()
        return

    if not BOT_TOKEN:
        log.error("❌ BOT_TOKEN не задан")
        await asyncio.Event().wait()
        return

    utl = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
    await utl.start()
    me = await utl.get_me()
    log.info(f"✅ Userbot: {me.first_name}")

    bot = Bot(token=BOT_TOKEN)

    for cid in list(db.get("cycles", {})):
        if db["cycles"][cid].get("active"):
            reg(cid)
    sch.start()

    if OWNER_ID:
        try:
            await bot.send_message(OWNER_ID,
                f"✅ <b>Бот запущен!</b>\n👤 {me.first_name}\nНажми /start",
                parse_mode="HTML")
        except Exception as e:
            log.error(f"Не могу написать владельцу: {e}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
