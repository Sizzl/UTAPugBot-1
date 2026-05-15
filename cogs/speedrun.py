import discord
from discord.ext import commands
import json
import os
import re
from discord.ext.commands import CheckFailure

TIME_REGEX = r"^\d{1,2}:\d{2}\.\d{3}$"
RECORDS_FILE = "speedrun/records.json"
PENDING_FILE = "speedrun/pending.json"
SETTINGS_FILE = "speedrun/settings.json"

VALID_MAPS = [
    "AsthenosphereSE", "AutoRIP", "Ballistic", "Bridge", "Desertstorm", "Desolate][", "DustbowlALRev04",
    "Frigate", "GolgothaAL", "Golgotha][AL", "Guardia", "HiSpeed", "Lavafort][PV", "Mazon",
    "OceanFloorAL", "RiverbedSE", "Riverbed]l[AL", "Rook", "Siege][", "Submarinebase][",
    "TheDungeon]l[AL", "TheScarabSE", "Vampire"
]
ADMIN_IDS = {244823882605920266, 189485300001538048, 254223470408237057}
TARGET_CHANNEL_ID = 788823535597649920

def parse_time_to_seconds(time_str):
    match = re.match(r'^(\d+):([0-5]?\d)\.(\d{1,3})$', time_str)
    if not match:
        return None

    minutes, seconds, milliseconds = match.groups()
    try:
        total_seconds = int(minutes) * 60 + int(seconds) + int(milliseconds.ljust(3, '0')) / 1000
        return total_seconds
    except ValueError:
        return None

def load_data(filename):
    if not os.path.exists(filename):
        return []
    try:
        with open(filename, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            print(f"⚠️ Warning: Expected list in {filename}, got {type(data)}.")
            return []
    except json.JSONDecodeError as e:
        print(f"⚠️ Error decoding JSON from {filename}: {e}")
        return []

def save_data(filename, data):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

class Speedrun(commands.Cog):
    def __init__(self, bot, channel=TARGET_CHANNEL_ID, admins=ADMIN_IDS, maplist=VALID_MAPS, settingsFile=SETTINGS_FILE):
        self.bot = bot
        self.channel = channel
        self.admins = admins
        self.maplist = maplist
        self.settingsFile = settingsFile
        self.load_settings(filename=self.settingsFile)

    def load_settings(self, filename):
        if not os.path.exists(filename):
            return
        settings = None
        with open(filename, "r") as f:
            try:
                settings = json.load(f)
            except:
                settings = None
        if settings is not None:
            if 'adminIDs' in settings:
                self.admins = settings['adminIDs']
            if 'targetChannelId' in settings:
                self.channel = settings['targetChannelId']
            if 'validMaps' in settings:
                self.maplist = settings['validMaps']
        return

    def is_admin():
        async def predicate(ctx):
            if ctx.author.id in self.admins:
                return True
            raise CheckFailure("You do not have permission to use this command.")
        return commands.check(predicate)

    def cog_check(self, ctx):
        if ctx.channel.id != self.channel:
            raise CheckFailure("Wrong channel")
        return True

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        if isinstance(error, CheckFailure) and str(error) == "Wrong channel":
            await ctx.send(f"🚫 This command can only be used in <#{self.channel}>.")

    @commands.command(name="leaderboard", aliases=["lb"])
    async def leaderboard(self, ctx, *, map_name: str = None):
        data = load_data(RECORDS_FILE)
        if not data:
            await ctx.send("❌ No leaderboard data found.")
            return

        if map_name:
            entry = next((m for m in data if m["map"].lower() == map_name.lower()), None)
            if not entry:
                await ctx.send(f"❌ No data found for map `{map_name}`.")
                return

            embed = discord.Embed(
                title=f"🏁 Top Times for {entry['map']}",
                color=discord.Color.gold()
            )

            medals = ["🥇", "🥈", "🥉"]
            for record in entry["leaderboard"]:
                pos = record["position"]
                player = record["player"]
                time = record["time"]
                emoji = medals[pos - 1] if pos <= len(medals) else f"#{pos}"
                embed.add_field(name=f"{emoji} {player}", value=f"**{time}**", inline=False)

            embed.set_footer(text="Submitted times are verified screenshots.")
            await ctx.send(embed=embed)

        else:
            embeds = []
            chunk = []
            for entry in data:
                map_name = entry["map"]
                top = next((r for r in entry["leaderboard"] if r["position"] == 1), None)
                if top:
                    chunk.append((map_name, f"{top['player']} – **{top['time']}**"))

                if len(chunk) == 25:
                    embed = discord.Embed(
                        title="🏆 Current Record Holders by Map",
                        color=discord.Color.blurple()
                    )
                    for name, value in chunk:
                        embed.add_field(name=name, value=value, inline=False)
                    embeds.append(embed)
                    chunk = []

            if chunk:
                embed = discord.Embed(
                    title="🏆 Current Record Holders by Map",
                    color=discord.Color.blurple()
                )
                for name, value in chunk:
                    embed.add_field(name=name, value=value, inline=False)
                embeds.append(embed)

            for embed in embeds:
                await ctx.send(embed=embed)

    @commands.command(name="edit")
    @is_admin()
    async def edit(self, ctx, map_name: str, player: str, new_time: str, new_link: str = None):
        total_seconds = parse_time_to_seconds(new_time)
        if total_seconds is None:
            await ctx.send("❌ Invalid time format. Use `mm:ss.sss` (e.g., `1:23.456`).")
            return

        records = load_data(RECORDS_FILE)
        map_entry = next((m for m in records if m["map"].lower() == map_name.lower()), None)
        if not map_entry:
            await ctx.send(f"❌ No record found for map `{map_name}`.")
            return

        leaderboard = map_entry["leaderboard"]
        record = next((r for r in leaderboard if r["player"].lower() == player.lower()), None)
        if not record:
            await ctx.send(f"❌ No record found for `{player}` on `{map_name}`.")
            return

        old_time = record["time"]
        record["time"] = new_time
        if new_link:
            record["screenshot_link"] = new_link

        leaderboard.sort(key=lambda r: parse_time_to_seconds(r["time"]))
        for i, r in enumerate(leaderboard, start=1):
            r["position"] = i

        save_data(RECORDS_FILE, records)

        await ctx.send(
            embed=discord.Embed(
                title="✏️ Record Updated",
                description=(
                        f"Updated `{player}` on `{map_name}`:\n"
                        f"**Old Time:** {old_time}\n"
                        f"**New Time:** {new_time}"
                        + (f"\n**New Link:** {new_link}" if new_link else "")
                ),
                color=discord.Color.teal()
            )
        )

    @commands.command(name="remove")
    @is_admin()
    async def remove(self, ctx, map_name: str, player: str):
        records = load_data(RECORDS_FILE)
        map_entry = next((m for m in records if m["map"].lower() == map_name.lower()), None)
        if not map_entry:
            await ctx.send(f"❌ No record found for map `{map_name}`.")
            return

        leaderboard = map_entry["leaderboard"]
        new_leaderboard = [r for r in leaderboard if r["player"].lower() != player.lower()]
        if len(new_leaderboard) == len(leaderboard):
            await ctx.send(f"⚠️ No approved record found for `{player}` on `{map_name}`.")
            return

        for i, r in enumerate(new_leaderboard, start=1):
            r["position"] = i
        map_entry["leaderboard"] = new_leaderboard

        save_data(RECORDS_FILE, records)

        await ctx.send(
            embed=discord.Embed(
                title="🗑️ Record Removed",
                description=f"Removed `{player}`'s approved record on `{map_name}`.",
                color=discord.Color.red()
            )
        )

    @commands.command(name="submit")
    async def submit(self, ctx, map_name=None, time=None, screenshot_link=None):
        if not all([map_name, time, screenshot_link]):
            await ctx.send("⚠️ Usage: `.submit <map_name> <mm:ss.sss> <screenshot_link>`")
            return

        if map_name not in self.maplist:
            await ctx.send(f"❌ Invalid map. Try one of: `{', '.join(self.maplist)}`")
            return

        try:
            total_seconds = parse_time_to_seconds(time)
        except Exception:
            total_seconds = None

        if total_seconds is None:
            await ctx.send("❌ Invalid time format. Use `mm:ss.sss`")
            return

        if not screenshot_link.startswith("http"):
            await ctx.send("❌ Screenshot link must be a valid URL.")
            return

        player = ctx.author.display_name
        pending = load_data(PENDING_FILE)
        records = load_data(RECORDS_FILE)

        record_entry = next((m for m in records if m["map"] == map_name), None)
        if record_entry:
            existing = next((r for r in record_entry["leaderboard"] if r["player"].lower() == player.lower()), None)
            if existing and parse_time_to_seconds(existing["time"]) <= total_seconds:
                await ctx.send(f"⚠️ You already have a better or equal **approved** time for `{map_name}`.")
                return

        pending_entry = next((m for m in pending if m["map"] == map_name), None)
        if not pending_entry:
            pending_entry = {"map": map_name, "leaderboard": []}
            pending.append(pending_entry)

        board = pending_entry["leaderboard"]
        current = next((r for r in board if r["player"].lower() == player.lower()), None)

        if current:
            existing_seconds = parse_time_to_seconds(current["time"])
            if existing_seconds is None or total_seconds < existing_seconds:
                current["time"] = time
                current["screenshot_link"] = screenshot_link
                await ctx.send(f"✅ Updated with a faster time for `{map_name}`.")
            else:
                await ctx.send("⚠️ You already submitted an equal or faster pending time.")
                return
        else:
            board.append({"player": player, "time": time, "screenshot_link": screenshot_link})
            await ctx.send(f"✅ Submission received for `{map_name}`. Awaiting approval.")

        save_data(PENDING_FILE, pending)

    @commands.command(name="pending")
    async def pending(self, ctx):
        data = load_data(PENDING_FILE)
        if not data:
            await ctx.send("📭 No pending submissions.")
            return

        embed = discord.Embed(
            title="🕒 Pending Submissions",
            color=discord.Color.orange()
        )

        for entry in data:
            lines = [
                f"**{r['player']}** – {r['time']} | {r.get('screenshot_link', 'No link')}"
                for r in entry["leaderboard"]
            ]
            embed.add_field(name=f"🗺️ {entry['map']}", value="\n".join(lines), inline=False)

        await ctx.send(embed=embed)

    @commands.command(name="approve")
    @is_admin()
    async def approve(self, ctx, *args):
        pending = load_data(PENDING_FILE)
        records = load_data(RECORDS_FILE)

        if not args:
            approved = []
            for pmap in pending[:]:
                for rec in pmap["leaderboard"]:
                    approved.append(self._approve_record(pmap["map"], rec, records))
                pending.remove(pmap)

            save_data(PENDING_FILE, pending)
            save_data(RECORDS_FILE, records)
            if approved:
                await ctx.send("✅ Approved:\n" + "\n".join(approved))
            else:
                await ctx.send("📭 No records to approve.")
            return

        if len(args) != 2:
            await ctx.send("⚠️ Use: `.approve <map> <player>` or `.approve` (all)")
            return

        map_name, player = args
        pmap = next((m for m in pending if m["map"].lower() == map_name.lower()), None)
        if not pmap:
            await ctx.send(f"❌ No pending entries for `{map_name}`.")
            return

        record = next((r for r in pmap["leaderboard"] if r["player"].lower() == player.lower()), None)
        if not record:
            await ctx.send(f"❌ No submission by `{player}` for `{map_name}`.")
            return

        self._approve_record(map_name, record, records)
        pmap["leaderboard"].remove(record)
        if not pmap["leaderboard"]:
            pending.remove(pmap)

        save_data(PENDING_FILE, pending)
        save_data(RECORDS_FILE, records)

        await ctx.send(f"✅ Approved `{player}` – {record['time']} on `{map_name}`.")

    def _approve_record(self, map_name, record, records):
        player = record["player"]
        time = record["time"]
        screenshot_link = record.get("screenshot_link", "")
        map_entry = next((m for m in records if m["map"] == map_name), None)
        if not map_entry:
            map_entry = {"map": map_name, "leaderboard": []}
            records.append(map_entry)

        board = map_entry["leaderboard"]
        board = [r for r in board if r["player"].lower() != player.lower()]
        board.append({"player": player, "time": time, "screenshot_link": screenshot_link})
        board.sort(key=lambda r: parse_time_to_seconds(r["time"]))
        for i, r in enumerate(board, start=1):
            r["position"] = i
        map_entry["leaderboard"] = board
        return f"{player} – {time} on {map_name}"

    @commands.command(name="reject")
    @is_admin()
    async def reject(self, ctx, *args):
        pending = load_data(PENDING_FILE)

        if not args:
            count = sum(len(e["leaderboard"]) for e in pending)
            save_data(PENDING_FILE, [])
            await ctx.send(embed=discord.Embed(
                title="❌ All Pending Submissions Rejected",
                description=f"{count} submission(s) removed.",
                color=discord.Color.red()
            ))
            return

        if len(args) != 2:
            await ctx.send("⚠️ Usage: `.reject <map> <player>` or `.reject` to reject all.")
            return

        map_name, player = args
        entry = next((m for m in pending if m["map"].lower() == map_name.lower()), None)
        if not entry:
            await ctx.send(f"❌ No pending map called `{map_name}`.")
            return

        before = len(entry["leaderboard"])
        entry["leaderboard"] = [
            r for r in entry["leaderboard"] if r["player"].lower() != player.lower()
        ]
        after = len(entry["leaderboard"])

        if after == before:
            await ctx.send(f"⚠️ No pending record from `{player}` on `{map_name}`.")
            return

        if not entry["leaderboard"]:
            pending.remove(entry)

        save_data(PENDING_FILE, pending)
        await ctx.send(f"🗑️ Rejected `{player}`'s submission on `{map_name}`.")

    @commands.command(name="scores")
    async def scores(self, ctx, *, player: str = None):
        player = player or ctx.author.display_name
        data = load_data(RECORDS_FILE)
        results = []

        for entry in data:
            for r in entry.get("leaderboard", []):
                if r["player"].lower() == player.lower():
                    results.append(f"**{entry['map']}** — {r['time']} (#{r['position']})")

        if not results:
            await ctx.send(f"❌ No records found for `{player}`.")
            return

        chunks = [results[i:i + 25] for i in range(0, len(results), 25)]
        for i, chunk in enumerate(chunks, start=1):
            embed = discord.Embed(
                title=f"🎯 Records for {player}",
                description="\n".join(chunk),
                color=discord.Color.green()
            )
            if len(chunks) > 1:
                embed.set_footer(text=f"Page {i} of {len(chunks)}")
            await ctx.send(embed=embed)

    @commands.command(name="rankings")
    async def rankings(self, ctx):
        data = load_data(RECORDS_FILE)
        wins = {}

        for entry in data:
            top = next((r for r in entry["leaderboard"] if r["position"] == 1), None)
            if top:
                wins[top["player"]] = wins.get(top["player"], 0) + 1

        if not wins:
            await ctx.send("🏁 No first-place finishes found.")
            return

        sorted_players = sorted(wins.items(), key=lambda x: x[1], reverse=True)
        lines = [f"**{i+1}. {p}** – {c}" for i, (p, c) in enumerate(sorted_players)]

        embed = discord.Embed(
            title="🏆 Top Speedrunners",
            description="\n".join(lines),
            color=discord.Color.purple()
        )
        await ctx.send(embed=embed)

    @commands.command(name="srhelp")
    async def help(self, ctx):
        embed = discord.Embed(
            title="📖 Speedrun Bot Commands",
            description="Here's a list of available commands:",
            color=discord.Color.blue()
        )

        embed.add_field(
            name="🎮 Player Commands",
            value=(
                "`.submit <map> <time> <screenshot_link>` – Submit a new run\n"
                "`.leaderboard [map]` / `.lb [map]` – Show leaderboard for a map or all current record holders\n"
                "`.scores [player]` – Show all records for a player (or your own if no player is given)\n"
                "`.rankings` – Rankings by number of 🥇 first place finishes"
            ),
            inline=False
        )

        embed.add_field(
            name="🛠️ Admin Commands",
            value=(
                "`.approve` – Approve all pending submissions\n"
                "`.approve <map> <player>` – Approve one submission\n"
                "`.reject` – Reject all pending submissions\n"
                "`.reject <map> <player>` – Reject one pending submission\n"
                "`.remove <map> <player>` – Remove an approved record\n"
                "`.edit <map> <player> <new_time>` – Edit an approved time\n"
                "`.pending` – List all pending submissions"
            ),
            inline=False
        )

        embed.add_field(
            name="ℹ️ Notes",
            value=(
                "- Time format: `mm:ss.sss` (e.g. `0:42.123`)\n"
                "- Screenshot must be a direct URL\n"
                "- Only faster submissions replace existing records"
            ),
            inline=False
        )

        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(Speedrun(bot))

