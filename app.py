"""
Telegram Userbot Broadcaster
- Веб-страница авторизации: открой URL сервиса в браузере
- После авторизации добавь SESSION_STRING в Render и перезапусти
- Управление рассылкой через Telegram бота
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
from pyrogram.errors import FloodWait
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Переменные окружения (с безопасными дефолтами) ──────────────────────────
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

# ══════════════════════════════════════════════════════════════════════════════
#  FLASK — страница авторизации
# ══════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)

AUTH_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Userbot — Авторизация</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:#0d0d1a;color:#e0e0e0;min-height:100vh;display:flex;
  align-items:center;justify-content:center;padding:16px}
.wrap{width:100%;max-width:440px}
.card{background:#161625;border:1px solid #252540;border-radius:20px;
  padding:36px 32px;box-shadow:0 24px 64px rgba(0,0,0,.5)}
.icon{font-size:52px;text-align:center;margin-bottom:10px}
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
  cursor:pointer;transition:opacity .2s,transform .1s;margin-bottom:10px}
.btn:hover{opacity:.9;transform:translateY(-1px)}
.btn:disabled{opacity:.4;cursor:not-allowed;transform:none}
.btn.gray{background:#252540}
.hint{background:#0d0d1a;border:1px solid #252540;border-radius:12px;
  padding:13px 15px;font-size:13px;color:#888;line-height:1.7;margin-bottom:18px}
.hint b{color:#ccc}
.err{color:#f04747;font-size:13px;padding:11px 14px;background:rgba(240,71,71,.1);
  border:1px solid rgba(240,71,71,.3);border-radius:10px;margin-bottom:14px;display:none}
.sess{background:#0d0d1a;border:2px solid #43b581;border-radius:12px;
  padding:14px;font-size:10.5px;color:#43b581;word-break:break-all;
  font-family:monospace;line-height:1.5;max-height:110px;overflow-y:auto;margin-bottom:14px}
.warn{color:#faa61a;font-size:13px;text-align:center;margin-bottom:14px}
.step{display:none}.step.on{display:block}
.big{font-size:56px;text-align:center;margin-bottom:14px}
</style>
</head>
<body>
<div class="wrap"><div class="card">
<div class="icon">📡</div>
<h1>Telegram Userbot</h1>
<p class="sub">Авторизация для рассылки от твоего аккаунта</p>
<div class="dots">
  <div class="dot on" id="d1"></div>
  <div class="dot"    id="d2"></div>
  <div class="dot"    id="d3"></div>
</div>

<!-- ШАГ 1 -->
<div class="step on" id="s1">
  <div class="hint">Введи номер телефона аккаунта Telegram, <b>от имени которого</b> будет идти рассылка.</div>
  <label>Номер телефона</label>
  <input id="phone" type="tel" placeholder="+380501234567" autofocus>
  <div class="err" id="e1"></div>
  <button class="btn" id="b1" onclick="doPhone()">Получить код →</button>
</div>

<!-- ШАГ 2 -->
<div class="step" id="s2">
  <div class="hint">Telegram прислал код в приложение.<br>
    Открой <b>Telegram → Saved Messages (Избранное)</b> — там будет код.</div>
  <label>Код из Telegram</label>
  <input id="code" type="number" placeholder="12345" maxlength="10">
  <div class="err" id="e2"></div>
  <button class="btn" id="b2" onclick="doCode()">Войти →</button>
  <button class="btn gray" onclick="goStep(1)">← Назад</button>
</div>

<!-- ШАГ 3 -->
<div class="step" id="s3">
  <div class="big">🎉</div>
  <div class="hint" style="text-align:center;margin-bottom:16px">
    <b>Авторизация успешна!</b><br>Скопируй строку ниже
  </div>
  <div class="sess" id="sval"></div>
  <p class="warn">⚠️ Никому не показывай эту строку!</p>
  <button class="btn" onclick="doCopy()">📋 Скопировать SESSION_STRING</button>
  <div class="hint" style="margin-top:14px;font-size:13px">
    <b>Что дальше:</b><br>
    1. Render → твой сервис → <b>Environment</b><br>
    2. Найди <b>SESSION_STRING</b> → вставь строку → <b>Save</b><br>
    3. Сервис перезапустится — бот напишет тебе ✅
  </div>
</div>
</div></div>

<script>
let hash='';
async function doPhone(){
  const phone=document.getElementById('phone').value.trim();
  if(!phone){showErr('e1','Введи номер');return;}
  setBusy('b1',true,'Отправляю...');hideErr('e1');
  try{
    const r=await fetch('/api/send_code',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({phone})});
    const d=await r.json();
    if(d.ok){hash=d.hash;goStep(2);}
    else showErr('e1',d.error||'Ошибка. Проверь номер.');
  }catch(e){showErr('e1','Ошибка: '+e.message);}
  setBusy('b1',false,'Получить код →');
}
async function doCode(){
  const phone=document.getElementById('phone').value.trim();
  const code=document.getElementById('code').value.trim();
  if(!code){showErr('e2','Введи код');return;}
  setBusy('b2',true,'Проверяю...');hideErr('e2');
  try{
    const r=await fetch('/api/verify_code',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({phone,code,hash})});
    const d=await r.json();
    if(d.ok){document.getElementById('sval').textContent=d.session;goStep(3);}
    else showErr('e2',d.error||'Неверный код.');
  }catch(e){showErr('e2','Ошибка: '+e.message);}
  setBusy('b2',false,'Войти →');
}
function doCopy(){
  const t=document.getElementById('sval').textContent;
  navigator.clipboard.writeText(t).then(()=>{
    const b=event.target;b.textContent='✅ Скопировано!';
    setTimeout(()=>b.textContent='📋 Скопировать SESSION_STRING',2500);
  });
}
function goStep(n){
  [1,2,3].forEach(i=>{
    document.getElementById('s'+i).classList.toggle('on',i===n);
    const dot=document.getElementById('d'+i);
    dot.classList.remove('on','done');
    if(i<n)dot.classList.add('done');
    else if(i===n)dot.classList.add('on');
  });
  if(n===2)setTimeout(()=>document.getElementById('code').focus(),100);
}
function showErr(id,msg){const el=document.getElementById(id);el.textContent=msg;el.style.display='block';}
function hideErr(id){document.getElementById(id).style.display='none';}
function setBusy(id,busy,txt){const b=document.getElementById(id);b.disabled=busy;b.textContent=txt;}
document.addEventListener('keydown',e=>{
  if(e.key!=='Enter')return;
  if(document.getElementById('s1').classList.contains('on'))doPhone();
  else if(document.getElementById('s2').classList.contains('on'))doCode();
});
</script>
</body></html>"""

DONE_PAGE = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8"><title>Бот работает</title>
<style>body{font-family:sans-serif;background:#0d0d1a;color:#e0e0e0;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}
.card{background:#161625;border:1px solid #252540;border-radius:20px;
  padding:40px;max-width:400px;text-align:center}
.icon{font-size:64px;margin-bottom:16px}h1{color:#43b581;font-size:22px;margin-bottom:10px}
p{color:#888;font-size:14px;line-height:1.7}</style></head>
<body><div class="card"><div class="icon">✅</div>
<h1>Бот запущен!</h1>
<p>Userbot авторизован и работает.<br>Управляй рассылкой через Telegram бота.</p>
</div></body></html>"""

_auth_loop: asyncio.AbstractEventLoop | None = None
_auth_client: Client | None = None

def get_auth_loop():
    global _auth_loop
    if _auth_loop is None or _auth_loop.is_closed():
        _auth_loop = asyncio.new_event_loop()
        threading.Thread(target=_auth_loop.run_forever, daemon=True).start()
    return _auth_loop

@app.route("/")
def index():
    return DONE_PAGE if SESSION_STR else AUTH_PAGE

@app.route("/api/send_code", methods=["POST"])
def api_send_code():
    global _auth_client
    if not API_ID or not API_HASH:
        return jsonify({"ok": False, "error": "API_ID и API_HASH не заданы!"})
    payload = request.get_json() or {}
    phone   = payload.get("phone", "").strip()
    loop    = get_auth_loop()

    async def _do():
        global _auth_client
        _auth_client = Client("auth_tmp", api_id=API_ID, api_hash=API_HASH,
                              in_memory=True, no_updates=True)
        await _auth_client.connect()
        sent = await _auth_client.send_code(phone)
        return sent.phone_code_hash

    try:
        h = asyncio.run_coroutine_threadsafe(_do(), loop).result(timeout=30)
        return jsonify({"ok": True, "hash": h})
    except Exception as e:
        logger.error(f"send_code: {e}")
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/verify_code", methods=["POST"])
def api_verify_code():
    global _auth_client
    payload = request.get_json() or {}
    phone   = payload.get("phone", "").strip()
    code    = payload.get("code",  "").strip()
    h       = payload.get("hash",  "").strip()
    loop    = get_auth_loop()

    async def _do():
        await _auth_client.sign_in(phone, h, code)
        s = await _auth_client.export_session_string()
        await _auth_client.disconnect()
        return s

    try:
        s = asyncio.run_coroutine_threadsafe(_do(), loop).result(timeout=30)
        return jsonify({"ok": True, "session": s})
    except Exception as e:
        logger.error(f"verify_code: {e}")
        return jsonify({"ok": False, "error": str(e)})

@app.route("/health")
def health():
    return jsonify({"ok": True, "has_session": bool(SESSION_STR)})


# ══════════════════════════════════════════════════════════════════════════════
#  USERBOT + УПРАВЛЯЮЩИЙ БОТ
# ══════════════════════════════════════════════════════════════════════════════

scheduler = AsyncIOScheduler()
userbot: Client | None = None
ctrl_bot: Client | None = None

if SESSION_STR and API_ID and BOT_TOKEN:
    userbot  = Client("userbot_s", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STR)
    ctrl_bot = Client("bot_s",     api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

_states: dict[int, dict] = {}
def ss(uid, state, **kw): _states[uid] = {"s": state, **kw}
def gs(uid): return _states.get(uid, {})
def cs(uid): _states.pop(uid, None)

def main_kb():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📢 Чаты"),      KeyboardButton("✉️ Сообщения")],
        [KeyboardButton("🚀 Разослать"), KeyboardButton("🔄 Циклы")],
        [KeyboardButton("⚙️ Настройки"),KeyboardButton("📊 Статистика")],
    ], resize_keyboard=True)

def nid(d): return str(max([int(k) for k in d if k.isdigit()], default=0) + 1)

async def smart_sleep():
    import random
    await asyncio.sleep(max(1, data["settings"]["delay_between_sends"] + random.uniform(0, 1)))

async def do_broadcast(msg_id: str, chat_id=None):
    if msg_id not in data["messages"]: return 0, 0
    text   = data["messages"][msg_id]["text"]
    name   = data["messages"][msg_id]["name"]
    active = {c: i for c, i in data["chats"].items() if i.get("active")}
    if not active:
        if chat_id: await ctrl_bot.send_message(chat_id, "⚠️ Нет активных чатов!")
        return 0, 0
    ok = fail = 0
    sm = None
    if chat_id:
        sm = await ctrl_bot.send_message(chat_id, f"📤 **{name}**\n⏳ 0/{len(active)}")
    for i, (cid, info) in enumerate(active.items(), 1):
        try:
            await userbot.send_message(int(cid), text)
            ok += 1
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
            try: await userbot.send_message(int(cid), text); ok += 1
            except: fail += 1
        except Exception as e:
            logger.error(f"broadcast {cid}: {e}"); fail += 1
        await smart_sleep()
        if sm and (i % 5 == 0 or i == len(active)):
            try: await sm.edit_text(f"📤 **{name}**\n✅{ok} ❌{fail} ⏳{i}/{len(active)}")
            except: pass
    if chat_id:
        await ctrl_bot.send_message(chat_id,
            f"🎉 **{name}** завершена!\n✅ Доставлено: {ok}\n❌ Ошибок: {fail}")
    return ok, fail

async def cycle_run(cid: str):
    if cid not in data["cycles"] or not data["cycles"][cid].get("active"): return
    cyc = data["cycles"][cid]
    ok, fail = await do_broadcast(cyc["msg_id"])
    try: await ctrl_bot.send_message(OWNER_ID, f"⏰ Цикл **{cyc['name']}** ✅{ok} ❌{fail}")
    except: pass

def reg_cycle(cid):
    scheduler.add_job(cycle_run, "interval", hours=data["cycles"][cid]["interval_hours"],
                      id=f"c_{cid}", replace_existing=True, args=[cid])


if ctrl_bot:

    @ctrl_bot.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, m: Message):
        cs(m.from_user.id)
        await m.reply(
            "👋 **Бот рассылки — управление**\n\n"
            "📢 *Чаты* — добавь группы и каналы\n"
            "✉️ *Сообщения* — шаблоны текстов\n"
            "🚀 *Разослать* — отправить прямо сейчас\n"
            "🔄 *Циклы* — автоматическая рассылка каждые N часов",
            reply_markup=main_kb()
        )

    @ctrl_bot.on_message(filters.regex("^📢 Чаты$") & filters.private)
    async def sec_chats(_, m: Message):
        cs(m.from_user.id)
        g  = sum(1 for c in data["chats"].values() if c.get("type")=="group"   and c.get("active"))
        ch = sum(1 for c in data["chats"].values() if c.get("type")=="channel" and c.get("active"))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить чат/канал", callback_data="add_chat")],
            [InlineKeyboardButton("📋 Список чатов",       callback_data="lst_chats")],
            [InlineKeyboardButton("🗑 Удалить чат",        callback_data="del_chat_m")],
        ])
        await m.reply(
            f"📢 **Чаты для рассылки**\n\n"
            f"👥 Групп: {g}  📣 Каналов: {ch}\nВсего: {len(data['chats'])}",
            reply_markup=kb
        )

    @ctrl_bot.on_callback_query(filters.regex("^add_chat$"))
    async def cb_add_chat(_, cq):
        ss(cq.from_user.id, "add_chat")
        await cq.message.reply(
            "➕ **Как добавить чат или канал:**\n\n"
            "**Вариант 1 (проще всего):**\n"
            "Перешли сюда любое сообщение из нужной группы или канала\n\n"
            "**Вариант 2:**\n"
            "Введи `@username` публичного чата или его числовой ID\n\n"
            "⚠️ Твой аккаунт должен быть **участником** группы или **админом** канала"
        )
        await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex("^lst_chats$"))
    async def cb_lst_chats(_, cq):
        if not data["chats"]:
            await cq.message.reply("📭 Чатов нет.\nНажми **➕ Добавить чат/канал**")
            await cq.answer(); return
        lines, btns = ["📋 **Список чатов:**\n"], []
        for cid, inf in data["chats"].items():
            e = "📣" if inf.get("type")=="channel" else "👥"
            s = "✅" if inf.get("active") else "⏸"
            lines.append(f"{s} {e} **{inf['title']}**")
            tog = "⏸ Откл" if inf.get("active") else "▶️ Вкл"
            btns.append([InlineKeyboardButton(f"{tog} — {inf['title'][:22]}", callback_data=f"tog:{cid}")])
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("\n".join(lines), reply_markup=InlineKeyboardMarkup(btns))
        await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^tog:"))
    async def cb_tog(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["chats"]:
            data["chats"][cid]["active"] = not data["chats"][cid].get("active", True)
            save_data(data)
            s = "✅ включён" if data["chats"][cid]["active"] else "⏸ отключён"
            await cq.answer(f"{data['chats'][cid]['title']}: {s}")

    @ctrl_bot.on_callback_query(filters.regex("^del_chat_m$"))
    async def cb_del_chat_m(_, cq):
        if not data["chats"]: await cq.answer("Нет чатов"); return
        btns = [[InlineKeyboardButton(f"🗑 {i['title']}", callback_data=f"dc:{c}")]
                for c, i in data["chats"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns))
        await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^dc:"))
    async def cb_dc(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["chats"]:
            t = data["chats"].pop(cid)["title"]; save_data(data)
            await cq.answer(f"Удалён: {t}")
            await cq.message.reply(f"🗑 **{t}** удалён.")

    @ctrl_bot.on_message(filters.regex("^✉️ Сообщения$") & filters.private)
    async def sec_msgs(_, m: Message):
        cs(m.from_user.id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Новое сообщение", callback_data="add_msg")],
            [InlineKeyboardButton("📋 Список",          callback_data="lst_msgs")],
            [InlineKeyboardButton("✏️ Редактировать",   callback_data="edit_msg_m")],
            [InlineKeyboardButton("🗑 Удалить",         callback_data="del_msg_m")],
        ])
        await m.reply(f"✉️ **Сообщения** — сохранено: {len(data['messages'])}", reply_markup=kb)

    @ctrl_bot.on_callback_query(filters.regex("^add_msg$"))
    async def cb_add_msg(_, cq):
        ss(cq.from_user.id, "msg_text")
        await cq.message.reply("✉️ Напиши текст сообщения для рассылки:\n_(Markdown: **жирный**, _курсив_)_")
        await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex("^lst_msgs$"))
    async def cb_lst_msgs(_, cq):
        if not data["messages"]: await cq.message.reply("📭 Сообщений нет."); await cq.answer(); return
        lines = ["✉️ **Сообщения:**\n"]
        for mid, inf in data["messages"].items():
            p = inf["text"][:80]+"…" if len(inf["text"])>80 else inf["text"]
            lines.append(f"📌 **{inf['name']}**\n└ {p}\n")
        await cq.message.reply("\n".join(lines)); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex("^edit_msg_m$"))
    async def cb_edit_msg_m(_, cq):
        if not data["messages"]: await cq.answer("Нет сообщений"); return
        btns = [[InlineKeyboardButton(f"✏️ {i['name']}", callback_data=f"em:{mid}")]
                for mid, i in data["messages"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^em:"))
    async def cb_em(_, cq):
        mid = cq.data.split(":")[1]
        ss(cq.from_user.id, "edit_msg", mid=mid)
        await cq.message.reply(
            f"✏️ **{data['messages'][mid]['name']}**\n\n{data['messages'][mid]['text']}\n\nНовый текст:"
        ); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex("^del_msg_m$"))
    async def cb_del_msg_m(_, cq):
        if not data["messages"]: await cq.answer("Нет сообщений"); return
        btns = [[InlineKeyboardButton(f"🗑 {i['name']}", callback_data=f"dm:{mid}")]
                for mid, i in data["messages"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^dm:"))
    async def cb_dm(_, cq):
        mid = cq.data.split(":")[1]
        if mid in data["messages"]:
            n = data["messages"].pop(mid)["name"]; save_data(data)
            await cq.answer(f"Удалено: {n}"); await cq.message.reply(f"🗑 **{n}** удалено.")

    @ctrl_bot.on_message(filters.regex("^🚀 Разослать$") & filters.private)
    async def sec_send(_, m: Message):
        cs(m.from_user.id)
        if not data["messages"]: await m.reply("❌ Нет сообщений. Добавь в ✉️ Сообщения"); return
        if not data["chats"]:   await m.reply("❌ Нет чатов. Добавь в 📢 Чаты"); return
        btns = [[InlineKeyboardButton(f"📤 {i['name']}", callback_data=f"sn:{mid}")]
                for mid, i in data["messages"].items()]
        btns.append([InlineKeyboardButton("📤 Все сообщения подряд", callback_data="sa")])
        await m.reply("Выбери сообщение:", reply_markup=InlineKeyboardMarkup(btns))

    @ctrl_bot.on_callback_query(filters.regex(r"^sn:"))
    async def cb_sn(_, cq):
        await cq.answer("⏳ Начинаю рассылку...")
        await do_broadcast(cq.data.split(":")[1], cq.message.chat.id)

    @ctrl_bot.on_callback_query(filters.regex("^sa$"))
    async def cb_sa(_, cq):
        await cq.answer("⏳ Отправляю все...")
        for mid in list(data["messages"]): await do_broadcast(mid, cq.message.chat.id)

    @ctrl_bot.on_message(filters.regex("^🔄 Циклы$") & filters.private)
    async def sec_cycles(_, m: Message):
        cs(m.from_user.id)
        ac = sum(1 for c in data["cycles"].values() if c.get("active"))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Создать цикл",   callback_data="add_cyc")],
            [InlineKeyboardButton("📋 Список",         callback_data="lst_cyc")],
            [InlineKeyboardButton("⏸/▶️ Вкл / Откл", callback_data="tog_cyc")],
            [InlineKeyboardButton("🗑 Удалить",        callback_data="del_cyc_m")],
        ])
        await m.reply(f"🔄 **Циклы** — активных: {ac}/{len(data['cycles'])}", reply_markup=kb)

    @ctrl_bot.on_callback_query(filters.regex("^add_cyc$"))
    async def cb_add_cyc(_, cq):
        if not data["messages"]: await cq.answer("Сначала добавь сообщение!"); return
        btns = [[InlineKeyboardButton(i["name"], callback_data=f"cm:{mid}")]
                for mid, i in data["messages"].items()]
        await cq.message.reply("Шаг 1 — выбери сообщение:", reply_markup=InlineKeyboardMarkup(btns))
        await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^cm:"))
    async def cb_cm(_, cq):
        mid = cq.data.split(":")[1]
        ss(cq.from_user.id, "cyc_int", mid=mid)
        await cq.message.reply(
            "Шаг 2 — каждые сколько часов?\n\nПримеры: `1`, `6`, `12`, `24`"
        ); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex("^lst_cyc$"))
    async def cb_lst_cyc(_, cq):
        if not data["cycles"]: await cq.message.reply("📭 Циклов нет."); await cq.answer(); return
        lines = ["🔄 **Циклы:**\n"]
        for cid, c in data["cycles"].items():
            s  = "✅" if c.get("active") else "⏸"
            mn = data["messages"].get(c["msg_id"],{}).get("name","?")
            lines.append(f"{s} **{c['name']}** — каждые {c['interval_hours']}ч\n└ {mn}\n")
        await cq.message.reply("\n".join(lines)); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex("^tog_cyc$"))
    async def cb_tog_cyc(_, cq):
        if not data["cycles"]: await cq.answer("Нет циклов"); return
        btns = [[InlineKeyboardButton(
            f"{'⏸' if c.get('active') else '▶️'} {c['name']}", callback_data=f"tc:{cid}"
        )] for cid, c in data["cycles"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Управление:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^tc:"))
    async def cb_tc(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["cycles"]:
            data["cycles"][cid]["active"] = not data["cycles"][cid].get("active", True)
            save_data(data)
            s = "▶️ запущен" if data["cycles"][cid]["active"] else "⏸ пауза"
            await cq.answer(f"{data['cycles'][cid]['name']}: {s}")

    @ctrl_bot.on_callback_query(filters.regex("^del_cyc_m$"))
    async def cb_del_cyc_m(_, cq):
        if not data["cycles"]: await cq.answer("Нет циклов"); return
        btns = [[InlineKeyboardButton(f"🗑 {c['name']}", callback_data=f"dlc:{cid}")]
                for cid, c in data["cycles"].items()]
        btns.append([InlineKeyboardButton("◀️ Назад", callback_data="bk")])
        await cq.message.reply("Выбери:", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()

    @ctrl_bot.on_callback_query(filters.regex(r"^dlc:"))
    async def cb_dlc(_, cq):
        cid = cq.data.split(":")[1]
        if cid in data["cycles"]:
            n = data["cycles"].pop(cid)["name"]; save_data(data)
            try: scheduler.remove_job(f"c_{cid}")
            except: pass
            await cq.answer(f"Удалён: {n}"); await cq.message.reply(f"🗑 **{n}** удалён.")

    @ctrl_bot.on_message(filters.regex("^⚙️ Настройки$") & filters.private)
    async def sec_settings(_, m: Message):
        cs(m.from_user.id)
        d = data["settings"]["delay_between_sends"]
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(
            f"⏱ Задержка: {d}с — изменить", callback_data="chd")]])
        await m.reply(f"⚙️ **Настройки**\n\nЗадержка: **{d} сек.**\n_(рекомендуется 3–5)_", reply_markup=kb)

    @ctrl_bot.on_callback_query(filters.regex("^chd$"))
    async def cb_chd(_, cq):
        ss(cq.from_user.id, "chdelay")
        await cq.message.reply("Введи задержку в секундах:"); await cq.answer()

    @ctrl_bot.on_message(filters.regex("^📊 Статистика$") & filters.private)
    async def sec_stats(_, m: Message):
        g  = sum(1 for c in data["chats"].values() if c.get("type")=="group")
        ch = sum(1 for c in data["chats"].values() if c.get("type")=="channel")
        ac = sum(1 for c in data["chats"].values() if c.get("active"))
        cyc= sum(1 for c in data["cycles"].values() if c.get("active"))
        await m.reply(
            f"📊 **Статистика**\n\n"
            f"📣 Каналов: {ch} · 👥 Групп: {g}\n"
            f"✅ Активных: {ac} / {len(data['chats'])}\n\n"
            f"✉️ Сообщений: {len(data['messages'])}\n"
            f"🔄 Активных циклов: {cyc} / {len(data['cycles'])}\n\n"
            f"⏱ Задержка: {data['settings']['delay_between_sends']} сек."
        )

    @ctrl_bot.on_callback_query(filters.regex("^bk$"))
    async def cb_bk(_, cq):
        await cq.message.reply("Главное меню:", reply_markup=main_kb()); await cq.answer()

    @ctrl_bot.on_message(filters.private & filters.forwarded)
    async def handle_fwd(_, m: Message):
        st = gs(m.from_user.id)
        if st.get("s") != "add_chat": return
        fc = m.forward_from_chat
        if not fc: await m.reply("❌ Не могу определить чат."); cs(m.from_user.id); return
        cid   = str(fc.id)
        ctype = "channel" if fc.type.value == "channel" else "group"
        title = fc.title or cid
        emoji = "📣" if ctype == "channel" else "👥"
        if cid in data["chats"]:
            await m.reply(f"⚠️ {emoji} **{title}** уже в списке.")
        else:
            data["chats"][cid] = {"title": title, "type": ctype, "active": True, "added": str(datetime.now())}
            save_data(data)
            await m.reply(
                f"✅ {emoji} **{title}** добавлен!\n\n"
                "Можешь перешли ещё или нажми 🚀 Разослать",
                reply_markup=main_kb()
            )
        cs(m.from_user.id)

    @ctrl_bot.on_message(filters.private & filters.text)
    async def handle_text(_, m: Message):
        uid = m.from_user.id
        st  = gs(uid)
        if not st: return
        s = st.get("s")

        if s == "add_chat":
            try:
                ci    = await userbot.get_chat(m.text.strip())
                cid   = str(ci.id)
                ctype = "channel" if ci.type.value == "channel" else "group"
                title = ci.title or cid
                emoji = "📣" if ctype == "channel" else "👥"
                if cid in data["chats"]:
                    await m.reply(f"⚠️ {emoji} **{title}** уже в списке.")
                else:
                    data["chats"][cid] = {"title": title, "type": ctype, "active": True, "added": str(datetime.now())}
                    save_data(data)
                    await m.reply(f"✅ {emoji} **{title}** добавлен!", reply_markup=main_kb())
            except Exception as e:
                await m.reply(f"❌ Не нашёл чат. Попробуй переслать сообщение.\n\n`{e}`")
            cs(uid)

        elif s == "msg_text":
            ss(uid, "msg_name", text=m.text)
            await m.reply("Придумай короткое название для этого сообщения:")

        elif s == "msg_name":
            mid = nid(data["messages"])
            data["messages"][mid] = {"name": m.text, "text": st["text"]}
            save_data(data)
            await m.reply(f"✅ Сообщение **{m.text}** сохранено!", reply_markup=main_kb())
            cs(uid)

        elif s == "edit_msg":
            data["messages"][st["mid"]]["text"] = m.text
            save_data(data)
            await m.reply("✅ Текст обновлён!", reply_markup=main_kb())
            cs(uid)

        elif s == "cyc_int":
            try:
                h = float(m.text.strip().replace(",", "."))
                if h <= 0: raise ValueError
            except:
                await m.reply("❌ Введи число, например `6`"); return
            ss(uid, "cyc_name", mid=st["mid"], h=h)
            await m.reply(f"⏱ Каждые **{h} ч.** ✅\n\nДай название этому циклу:")

        elif s == "cyc_name":
            cid = nid(data["cycles"])
            data["cycles"][cid] = {
                "name": m.text, "msg_id": st["mid"],
                "interval_hours": st["h"], "active": True,
                "created": str(datetime.now())
            }
            save_data(data)
            reg_cycle(cid)
            mn = data["messages"].get(st["mid"], {}).get("name", "?")
            await m.reply(
                f"✅ Цикл **{m.text}** создан!\n\n"
                f"📨 Сообщение: {mn}\n⏱ Каждые {st['h']} ч.\n🟢 Активен",
                reply_markup=main_kb()
            )
            cs(uid)

        elif s == "chdelay":
            try:
                d = float(m.text.strip())
                if d < 0: raise ValueError
            except:
                await m.reply("❌ Введи число секунд"); return
            data["settings"]["delay_between_sends"] = d
            save_data(data)
            await m.reply(f"✅ Задержка: **{d} сек.**", reply_markup=main_kb())
            cs(uid)


# ══════════════════════════════════════════════════════════════════════════════
#  Запуск
# ══════════════════════════════════════════════════════════════════════════════

def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)

async def run_bots():
    if not SESSION_STR:
        logger.info("⚠️  SESSION_STRING пуст — открой URL сервиса в браузере для авторизации")
        return
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN не задан!")
        return

    for cid in list(data.get("cycles", {})):
        if data["cycles"][cid].get("active"):
            reg_cycle(cid)
    scheduler.start()

    await asyncio.gather(userbot.start(), ctrl_bot.start())
    me     = await userbot.get_me()
    bot_me = await ctrl_bot.get_me()
    logger.info(f"✅ Userbot: {me.first_name}  |  Bot: @{bot_me.username}")
    try:
        await ctrl_bot.send_message(OWNER_ID,
            f"✅ **Бот запущен!**\n"
            f"👤 Userbot: **{me.first_name}**\n"
            f"🤖 Управление: @{bot_me.username}\n\n"
            f"Нажми /start чтобы начать")
    except Exception as e:
        logger.error(f"Не могу написать владельцу ({OWNER_ID}): {e}")

    await asyncio.Event().wait()

def main():
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info(f"🌐 Веб-сервер на порту {PORT}")
    asyncio.run(run_bots())

if __name__ == "__main__":
    main()
