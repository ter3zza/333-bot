import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiosqlite
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()

# ───────────────────────── НАСТРОЙКИ ─────────────────────────
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))          # необязательно: ускоряет появление команд
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))  # по нему считается неделя (пн 00:00)

TRACK_ROLE_ID = 1566015000173346856   # роль, чей войс отслеживается
COOWNER_ID = 1512366895607713834      # совладелец (владелец сервера определяется автоматически)
DB_PATH = "bot.db"
FAR_FUTURE = 2 ** 40
# ─────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("voicebot")

intents = discord.Intents.default()
intents.members = True          # Server Members Intent
intents.voice_states = True
intents.message_content = True  # Message Content Intent (нужен, чтобы видеть вложения)

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

db: aiosqlite.Connection
voice_lock = asyncio.Lock()


# ───────────────────────── БАЗА ДАННЫХ ─────────────────────────
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS voice_sessions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            started INTEGER NOT NULL,
            ended INTEGER NOT NULL,
            open INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_vs_user ON voice_sessions(user_id);
        CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY, weekly_norm INTEGER);
        CREATE TABLE IF NOT EXISTS shots(
            log_message_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created INTEGER NOT NULL,
            kills INTEGER,
            credited_by INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_shots_user ON shots(user_id);
        """
    )
    await db.commit()


async def get_setting(key: str):
    cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cur.fetchone()
    return row[0] if row else None


async def set_setting(key: str, value: str):
    await db.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    await db.commit()


async def get_norm(user_id: int):
    cur = await db.execute("SELECT weekly_norm FROM users WHERE user_id=?", (user_id,))
    row = await cur.fetchone()
    return row[0] if row else None


async def set_norm(user_id: int, norm: int):
    await db.execute(
        "INSERT INTO users(user_id,weekly_norm) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET weekly_norm=excluded.weekly_norm",
        (user_id, norm),
    )
    await db.commit()


# ───────────────────────── ХЕЛПЕРЫ ─────────────────────────
def is_manager(user, guild) -> bool:
    return guild is not None and (user.id == COOWNER_ID or user.id == guild.owner_id)


def manager_only():
    async def pred(interaction: discord.Interaction):
        if is_manager(interaction.user, interaction.guild):
            return True
        raise app_commands.CheckFailure("no access")

    return app_commands.check(pred)


def week_start_ts() -> int:
    now = datetime.now(TZ)
    monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(monday.timestamp())


def period_range(period: str):
    if period == "week":
        return week_start_ts(), FAR_FUTURE
    return 0, FAR_FUTURE


def fmt_dur(seconds: int) -> str:
    seconds = int(seconds)
    return f"{seconds // 3600}ч {(seconds % 3600) // 60:02d}м"


def rank_label(i: int) -> str:
    return ["🥇", "🥈", "🥉"][i - 1] if i <= 3 else f"`{i}.`"


def clip(lines, limit=1000) -> str:
    out, size = [], 0
    for line in lines:
        if size + len(line) + 1 > limit:
            out.append("…")
            break
        out.append(line)
        size += len(line) + 1
    return "\n".join(out) if out else "—"


# ───────────────────────── ВОЙС-ТРЕКИНГ ─────────────────────────
def wants_tracking(member: discord.Member, voice: discord.VoiceState | None = None) -> bool:
    voice = voice or member.voice
    if member.bot or not voice or not voice.channel:
        return False
    if member.guild.afk_channel and voice.channel.id == member.guild.afk_channel.id:
        return False
    return any(r.id == TRACK_ROLE_ID for r in member.roles)


async def has_open_session(user_id: int) -> bool:
    cur = await db.execute("SELECT 1 FROM voice_sessions WHERE user_id=? AND open=1 LIMIT 1", (user_id,))
    return await cur.fetchone() is not None


async def close_session(user_id: int, set_now: bool = True):
    if set_now:
        await db.execute(
            "UPDATE voice_sessions SET ended=?, open=0 WHERE user_id=? AND open=1", (int(time.time()), user_id)
        )
    else:
        await db.execute("UPDATE voice_sessions SET open=0 WHERE user_id=? AND open=1", (user_id,))
    await db.commit()


async def sync_member(member: discord.Member, voice: discord.VoiceState | None = None):
    async with voice_lock:
        want = wants_tracking(member, voice)
        has = await has_open_session(member.id)
        if want and not has:
            now = int(time.time())
            await db.execute(
                "INSERT INTO voice_sessions(user_id,started,ended,open) VALUES(?,?,?,1)", (member.id, now, now)
            )
            await db.commit()
        elif not want and has:
            await close_session(member.id)


@tasks.loop(seconds=30)
async def heartbeat():
    await db.execute("UPDATE voice_sessions SET ended=? WHERE open=1", (int(time.time()),))
    await db.commit()


async def voice_totals(pstart: int, pend: int) -> dict[int, int]:
    now = int(time.time())
    cur = await db.execute(
        """
        SELECT user_id,
               SUM(MAX(0, MIN(CASE WHEN open=1 THEN ? ELSE ended END, ?) - MAX(started, ?)))
        FROM voice_sessions GROUP BY user_id
        """,
        (now, pend, pstart),
    )
    return {uid: int(sec or 0) for uid, sec in await cur.fetchall()}


async def reconcile_voice():
    """После перезапуска: закрыть «зависшие» сессии и открыть новые для тех, кто сейчас в войсе."""
    for guild in bot.guilds:
        cur = await db.execute("SELECT DISTINCT user_id FROM voice_sessions WHERE open=1")
        for (uid,) in await cur.fetchall():
            member = guild.get_member(uid)
            if not member or not wants_tracking(member):
                async with voice_lock:
                    await close_session(uid, set_now=False)  # конец = последний heartbeat
        for vc in guild.voice_channels + guild.stage_channels:
            for member in vc.members:
                await sync_member(member)


@bot.event
async def on_voice_state_update(member, before, after):
    await sync_member(member, after)


@bot.event
async def on_member_update(before, after):
    if before.roles != after.roles:
        await sync_member(after)


# ───────────────────────── СКРИНЫ / КИЛЫ ─────────────────────────
async def kills_totals(pstart: int, pend: int) -> dict[int, int]:
    cur = await db.execute(
        "SELECT user_id, SUM(kills) FROM shots WHERE kills IS NOT NULL AND created>=? AND created<? GROUP BY user_id",
        (pstart, pend),
    )
    return {uid: int(k or 0) for uid, k in await cur.fetchall()}


async def get_shot(message_id: int):
    cur = await db.execute(
        "SELECT user_id, created, kills, credited_by FROM shots WHERE log_message_id=?", (message_id,)
    )
    return await cur.fetchone()


async def build_shot_embed(guild, user_id, kills, credited_by, description=None) -> discord.Embed:
    norm = await get_norm(user_id)
    week_kills = (await kills_totals(week_start_ts(), FAR_FUTURE)).get(user_id, 0)
    member = guild.get_member(user_id)

    embed = discord.Embed(
        title="📸 Новый скриншот",
        description=description or None,
        color=discord.Color.green() if kills is not None else discord.Color.orange(),
    )
    if member:
        embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
    embed.add_field(name="Игрок", value=f"<@{user_id}>", inline=True)
    embed.add_field(name="Килы за скрин", value=str(kills) if kills is not None else "⏳ не зачислено", inline=True)
    embed.add_field(name="Норма в неделю", value=str(norm) if norm is not None else "⚠️ не установлена", inline=True)
    progress = f"{week_kills}" + (f" / {norm}" if norm else "")
    embed.add_field(name="Килы за текущую неделю", value=progress, inline=False)
    if credited_by:
        who = guild.get_member(credited_by)
        embed.set_footer(text=f"Зачислил: {who.display_name if who else credited_by}")
    return embed


def make_shot_view(norm_is_set: bool) -> "ShotView":
    view = ShotView()
    view.norm_btn.label = "Изменить норму" if norm_is_set else "Установить норму"
    return view


class KillsModal(discord.ui.Modal, title="Зачислить килы"):
    def __init__(self, user_id: int, current):
        super().__init__()
        self.user_id = user_id
        self.kills_input = discord.ui.TextInput(
            label="Количество килов за этот скрин",
            placeholder="Например: 12",
            default=str(current) if current is not None else None,
            max_length=5,
        )
        self.add_item(self.kills_input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.kills_input.value.strip()
        if not raw.isdigit():
            return await interaction.response.send_message("❌ Введите целое число ≥ 0.", ephemeral=True)
        kills = int(raw)
        await db.execute(
            "UPDATE shots SET kills=?, credited_by=? WHERE log_message_id=?",
            (kills, interaction.user.id, interaction.message.id),
        )
        await db.commit()
        old = interaction.message.embeds[0].description if interaction.message.embeds else None
        embed = await build_shot_embed(interaction.guild, self.user_id, kills, interaction.user.id, old)
        await interaction.response.edit_message(embed=embed)


class NormModal(discord.ui.Modal, title="Недельная норма"):
    def __init__(self, user_id: int, current):
        super().__init__()
        self.user_id = user_id
        self.norm_input = discord.ui.TextInput(
            label="Норма киллов в неделю",
            placeholder="Например: 50",
            default=str(current) if current is not None else None,
            max_length=6,
        )
        self.add_item(self.norm_input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.norm_input.value.strip()
        if not raw.isdigit():
            return await interaction.response.send_message("❌ Введите целое число ≥ 0.", ephemeral=True)
        await set_norm(self.user_id, int(raw))
        shot = await get_shot(interaction.message.id)
        old = interaction.message.embeds[0].description if interaction.message.embeds else None
        embed = await build_shot_embed(
            interaction.guild, self.user_id, shot[2] if shot else None, shot[3] if shot else None, old
        )
        await interaction.response.edit_message(embed=embed, view=make_shot_view(True))


class ShotView(discord.ui.View):
    """Постоянная view: работает и после перезапуска бота."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Зачислить килы", emoji="🎯", style=discord.ButtonStyle.success, custom_id="shot:kills")
    async def kills_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_manager(interaction.user, interaction.guild):
            return await interaction.response.send_message("⛔ Нет доступа.", ephemeral=True)
        shot = await get_shot(interaction.message.id)
        if not shot:
            return await interaction.response.send_message("❌ Этот скрин не найден в базе.", ephemeral=True)
        await interaction.response.send_modal(KillsModal(shot[0], shot[2]))

    @discord.ui.button(label="Норма", emoji="📏", style=discord.ButtonStyle.primary, custom_id="shot:norm")
    async def norm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_manager(interaction.user, interaction.guild):
            return await interaction.response.send_message("⛔ Нет доступа.", ephemeral=True)
        shot = await get_shot(interaction.message.id)
        if not shot:
            return await interaction.response.send_message("❌ Этот скрин не найден в базе.", ephemeral=True)
        await interaction.response.send_modal(NormModal(shot[0], await get_norm(shot[0])))


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    shots_id = await get_setting("shots_channel")
    if not shots_id or message.channel.id != int(shots_id):
        return

    images = [a for a in message.attachments if (a.content_type or "").startswith("image/")]
    if not images:
        return

    logs_id = await get_setting("logs_channel")
    log_channel = message.guild.get_channel(int(logs_id)) if logs_id else None
    if not log_channel:
        log.warning("Канал логов скринов не настроен — используйте /настройка")
        return

    try:
        files = [await a.to_file() for a in images[:10]]
        norm = await get_norm(message.author.id)
        embed = await build_shot_embed(
            message.guild, message.author.id, None, None, message.content[:1000] if message.content else None
        )
        content = None
        if norm is None:  # первый скрин игрока — просим один раз установить норму
            content = (
                f"<@{message.guild.owner_id}> <@{COOWNER_ID}> "
                f"первый скрин от {message.author.mention} — установите недельную норму кнопкой ниже."
            )
        sent = await log_channel.send(
            content=content,
            embed=embed,
            files=files,
            view=make_shot_view(norm is not None),
            allowed_mentions=discord.AllowedMentions(users=True),
        )
    except Exception:
        log.exception("Не удалось переслать скрин — оригинал НЕ удалён")
        return

    await db.execute(
        "INSERT INTO shots(log_message_id,user_id,created) VALUES(?,?,?)",
        (sent.id, message.author.id, int(time.time())),
    )
    await db.commit()

    try:
        await message.delete()
    except discord.HTTPException:
        log.warning("Нет прав удалить сообщение в канале скринов (нужно Manage Messages)")
    try:
        await message.channel.send(f"{message.author.mention}, скрин принят ✅", delete_after=5)
    except discord.HTTPException:
        pass


# ───────────────────────── КОМАНДЫ ─────────────────────────
PERIOD_CHOICES = [
    app_commands.Choice(name="Эта неделя", value="week"),
    app_commands.Choice(name="За всё время", value="all"),
]


@tree.command(name="лидерборд_войс", description="Рейтинг по времени в голосовых каналах")
@app_commands.guild_only()
@app_commands.rename(period="период")
@app_commands.choices(period=PERIOD_CHOICES)
async def voice_leaderboard(interaction: discord.Interaction, period: str = "week"):
    await interaction.response.defer()
    role = interaction.guild.get_role(TRACK_ROLE_ID)
    if not role:
        return await interaction.followup.send("❌ Отслеживаемая роль не найдена на сервере.")

    pstart, pend = period_range(period)
    totals = await voice_totals(pstart, pend)
    rows = sorted(((m.id, totals.get(m.id, 0)) for m in role.members if not m.bot), key=lambda r: -r[1])

    title = "🎙️ Войс — " + ("эта неделя" if period == "week" else "всё время")
    embed = discord.Embed(title=title, color=discord.Color.blurple())
    if not rows:
        embed.description = "Нет участников с отслеживаемой ролью."
        return await interaction.followup.send(embed=embed)

    top = rows[:10]
    embed.add_field(
        name="🏆 Больше всего",
        value=clip([f"{rank_label(i)} <@{uid}> — **{fmt_dur(sec)}**" for i, (uid, sec) in enumerate(top, 1)]),
        inline=False,
    )
    rest = rows[10:]
    if rest:
        bottom = rest[-10:][::-1]  # самые «молчаливые» сверху
        embed.add_field(
            name="🐢 Меньше всего",
            value=clip([f"<@{uid}> — {fmt_dur(sec)}" for uid, sec in bottom]),
            inline=False,
        )
    embed.set_footer(text=f"Участников с ролью: {len(rows)} • суммарно {fmt_dur(sum(s for _, s in rows))}")
    await interaction.followup.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())


@tree.command(name="лидерборд_килы", description="Рейтинг по килам и выполнение нормы")
@app_commands.guild_only()
@app_commands.rename(period="период")
@app_commands.choices(period=PERIOD_CHOICES)
async def kills_leaderboard(interaction: discord.Interaction, period: str = "week"):
    await interaction.response.defer()
    pstart, pend = period_range(period)
    totals = await kills_totals(pstart, pend)

    cur = await db.execute("SELECT user_id, weekly_norm FROM users WHERE weekly_norm IS NOT NULL")
    norms = {uid: n for uid, n in await cur.fetchall()}

    ids = set(totals) | (set(norms) if period == "week" else set())
    rows = sorted(((uid, totals.get(uid, 0)) for uid in ids), key=lambda r: -r[1])

    embed = discord.Embed(
        title="🎯 Килы — " + ("эта неделя" if period == "week" else "всё время"), color=discord.Color.red()
    )
    if not rows:
        embed.description = "Пока нет данных."
        return await interaction.followup.send(embed=embed)

    lines = []
    for i, (uid, k) in enumerate(rows[:20], 1):
        line = f"{rank_label(i)} <@{uid}> — **{k}**"
        if period == "week" and norms.get(uid):
            line += f" / {norms[uid]} " + ("✅" if k >= norms[uid] else "❌")
        lines.append(line)
    embed.add_field(name="🏆 Топ", value=clip(lines), inline=False)

    if period == "week":
        behind = [(uid, k, norms[uid]) for uid, k in rows if norms.get(uid) and k < norms[uid]]
        behind.sort(key=lambda r: r[1] - r[2])
        embed.add_field(
            name="⚠️ Не выполнили норму",
            value=clip([f"<@{uid}> — {k}/{n} (не хватает {n - k})" for uid, k, n in behind]),
            inline=False,
        )
        embed.set_footer(text="Неделя считается с понедельника 00:00")
    await interaction.followup.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())


@tree.command(name="профиль", description="Статистика игрока: войс, килы, норма")
@app_commands.guild_only()
@app_commands.rename(member="игрок")
async def profile(interaction: discord.Interaction, member: discord.Member | None = None):
    member = member or interaction.user
    ws = week_start_ts()
    v_week = (await voice_totals(ws, FAR_FUTURE)).get(member.id, 0)
    v_all = (await voice_totals(0, FAR_FUTURE)).get(member.id, 0)
    k_week = (await kills_totals(ws, FAR_FUTURE)).get(member.id, 0)
    k_all = (await kills_totals(0, FAR_FUTURE)).get(member.id, 0)
    norm = await get_norm(member.id)

    embed = discord.Embed(title=f"Профиль {member.display_name}", color=discord.Color.blurple())
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="🎙️ Войс (неделя)", value=fmt_dur(v_week))
    embed.add_field(name="🎙️ Войс (всего)", value=fmt_dur(v_all))
    embed.add_field(name="\u200b", value="\u200b")
    embed.add_field(name="🎯 Килы (неделя)", value=f"{k_week}" + (f" / {norm}" if norm else ""))
    embed.add_field(name="🎯 Килы (всего)", value=str(k_all))
    embed.add_field(name="📏 Норма", value=str(norm) if norm is not None else "не установлена")
    await interaction.response.send_message(embed=embed)


@tree.command(name="настройка", description="[Владелец] Указать каналы для скринов и логов скринов")
@app_commands.guild_only()
@app_commands.rename(shots="канал_скринов", logs="канал_логов")
@app_commands.describe(shots="Куда игроки кидают скрины", logs="Куда бот пересылает скрины")
@manager_only()
async def setup_cmd(interaction: discord.Interaction, shots: discord.TextChannel, logs: discord.TextChannel):
    await set_setting("shots_channel", str(shots.id))
    await set_setting("logs_channel", str(logs.id))
    await interaction.response.send_message(
        f"✅ Скрины: {shots.mention}\n✅ Логи скринов: {logs.mention}", ephemeral=True
    )


@tree.command(name="норма", description="[Владелец] Установить недельную норму игроку")
@app_commands.guild_only()
@app_commands.rename(member="игрок", value="килы")
@manager_only()
async def norm_cmd(interaction: discord.Interaction, member: discord.Member, value: app_commands.Range[int, 0, 100000]):
    await set_norm(member.id, value)
    await interaction.response.send_message(f"✅ Норма {member.mention}: **{value}** киллов/неделю", ephemeral=True)


@tree.command(name="килы_изменить", description="[Владелец] Добавить килы игроку вручную (без скрина)")
@app_commands.guild_only()
@app_commands.rename(member="игрок", value="килы")
@manager_only()
async def add_kills_cmd(interaction: discord.Interaction, member: discord.Member, value: app_commands.Range[int, 0, 100000]):
    await db.execute(
        "INSERT INTO shots(log_message_id,user_id,created,kills,credited_by) VALUES(?,?,?,?,?)",
        (-int(time.time() * 1000), member.id, int(time.time()), value, interaction.user.id),
    )
    await db.commit()
    await interaction.response.send_message(f"✅ {member.mention}: +{value} киллов", ephemeral=True)


@tree.error
async def on_app_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        msg = "⛔ Эта команда доступна только владельцу и совладельцу."
    else:
        log.exception("Ошибка команды", exc_info=error)
        msg = "❌ Произошла ошибка."
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


# ───────────────────────── ЗАПУСК ─────────────────────────
_ready_once = False


@bot.event
async def setup_hook():
    await init_db()
    bot.add_view(ShotView())
    if GUILD_ID:
        guild = discord.Object(id=GUILD_ID)
        tree.copy_global_to(guild=guild)
        await tree.sync(guild=guild)
    else:
        await tree.sync()
    heartbeat.start()


@bot.event
async def on_ready():
    global _ready_once
    log.info("Вошёл как %s", bot.user)
    if not _ready_once:
        _ready_once = True
        await reconcile_voice()


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Укажите DISCORD_TOKEN в файле .env")
    bot.run(TOKEN)
