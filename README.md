# Telegram Join-Request Auto-Approver Bot

Built with **Python 3.10+**, **aiogram 3.x**, **asyncpg (PostgreSQL)** / aiosqlite (optional SQLite).

Approves join requests of **private channels / groups** (the ones with
"Approve new members" enabled) one by one, at an interval you control.

## How it works
1. A user taps **Request to Join** on your invite link.
2. The bot queues the request and can send a **pre-request message** immediately.
3. A background worker approves **one request every N seconds** (per destination) while accepting is started.
4. After approval, an optional **post-request message** can be sent (with delay).
5. Outbound messages include **Unsubscribe**. Unsubscribing clears tracked history and stops further messages.
6. State survives restarts (queue, intervals, messages, running flags in the DB).

## Setup
1. Create a bot with [@BotFather](https://t.me/BotFather) → copy token.
2. Get your numeric ID from [@userinfobot](https://t.me/userinfobot).
3. Copy env values into `.env`: `BOT_TOKEN`, `ADMIN_ID`, `DATABASE_URL`.
4. Add the bot as **admin** with **Invite Users via Link / Add Members**.
5. Use an invite link with **Request admin approval**.
6. Run:
   ```
   pip install -r requirements.txt
   python main.py
   ```
7. Open the bot in private chat → `/start` → admin panel.

## Admin panel
| Section | What it does |
|---|---|
| 📡 Destinations | Status, accept timer, links, per-destination pre/post messages |
| 💬 Massages | Pre-request · Post-request · Message library · Broadcast |
| ✅ Acceptors | Start/stop accepting, timers, retry failed |
| 👤 User info | Subscribed / unsubscribed lists |
| 📊 Universal status | Time-window stats + subscribed / unsubscribed counts |
| 🧾 Master log | Activity feed (summary + last 5 activities; no destination blocks) |

### Massages
- **Pre-request** — on join request (optional short delay)
- **Post-request** — after approval (optional delay up to 24h)
- **Library** — templates; set as pre-primary / post / broadcast
- Broadcast audience: subscribed, unsubscribed, or both

Placeholders: `{mention}` `{first_name}` `{full_name}` `{username}` `{chat_title}`

## Files
- `main.py` – bot start, dispatcher, queue worker
- `features.py` – DB, handlers, admin panel
- `requirements.txt`, `.gitignore`

## User pipeline & status (add-on)
Every user goes through: 📥 Request → 📨 Pre-msg → 👁 Seen (= delivered) → ✅ Accepted → 📤 Post-msg.

- **🧾 Master log → LATEST ACTIVITY** shows the last 10 users whose pipeline changed, newest first,
  with 5 dots each: 🟢 done · 🟡 pending · 🔴 failed · ⚪ message type switched OFF
  (change `STEP_OFF` in `features.py` to `"🟡"` if you do not want ⚪).
- Each destination has one totals line: `📥 requests · ⏳ pending · ✅ accepted · ➕ joined · 🚪 left · 🚫 blocked/deleted bot`.
- **📊 Universal status** shows the same layers for every time window (🟢/🔴 counts per layer).
- **🚫 Blocked / deleted the bot** is counted per user (detected when Telegram says the bot was blocked,
  or when the user blocks it) and cleared when they come back. In **Massages → Broadcast** a switch
  includes or excludes these members; the audience counts follow the switch.
- Telegram never tells a bot when a user only deletes the chat, so that case is detected on the next failed send.
