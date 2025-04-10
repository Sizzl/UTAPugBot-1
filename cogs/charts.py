from discord.ext import commands
import discord
from cogs import admin
import plotly.graph_objects as go
from PIL import Image 
import os
import logging
import sys
from datetime import datetime

DEFAULT_RATING_FILE = 'players/ratings.json'

#########################################################################################
# Logging
#########################################################################################
def setupLogging(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logfilename = 'log//{0}-{1}.log'.format(name,datetime.now().strftime("%Y-%m-%d"))
    os.makedirs(os.path.dirname(logfilename), exist_ok=True)
    handler = logging.FileHandler(filename=logfilename, encoding='utf-8', mode='w')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)

    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    screen_handler.setLevel(logging.INFO)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger

log = setupLogging('charts')

#########################################################################################
# Charting
#########################################################################################

class PlayerChart(commands.Cog):
    """Renders charts for Player related data"""
    def __init__(self, bot, ratingsFile=DEFAULT_RATING_FILE):
        self.bot = bot
        self.ratingsFile = ratingsFile

    def generateRankHistory(self, ctx, mode, pid, data, playername, force = True):
        if not os.path.exists('images'):
            os.mkdir('images')
        if len(data['lastgamedate']):
            g_date = datetime.fromisoformat(data['lastgamedate'])
        else:
            g_date = datetime.fromisoformat(data['ratingdate'])
        image = 'images/{0}_{1}_{2}.png'.format(data['lastgameref'],g_date.strftime('%Y%m%d%H%M%S'),pid)
        if not os.path.exists(image) or force:
            matches = {'Time':[],'RP':[],'Type':[]}
            admsets = {'Time':[],'RP':[],'Type':[]}
            if 'ratinghistory' in data and len(data['ratinghistory']):
                for h in data['ratinghistory']:
                    h_date = datetime.fromisoformat(h['matchdate'])
                    matches['Time'].append(h_date)
                    matches['RP'].append(h['ratingafter'])
                    if len(h['matchref']) == 0 or h['matchref'] == 'admin-set':
                        admsets['Time'].append(h_date)
                        admsets['RP'].append(h['ratingafter'])
                        matches['Type'].append('Admin set @<br>{0}'.format(h_date.strftime('%H:%M:%S on<br>%d/%m/%Y')))
                    else:
                        admsets['Time'].append(None)
                        admsets['RP'].append(None)
                        matches['Type'].append('Ranked match')
            matches['Time'].append(g_date)
            matches['RP'].append(data['ratingvalue'])
            if len(data['lastgameref']) == 0 or data['lastgameref'] == 'admin-set':
                admsets['Time'].append(g_date)
                admsets['RP'].append(data['ratingvalue'])
                matches['Type'].append('Admin set @<br>{0}'.format(g_date.strftime('%H:%M:%S on<br>%d/%m/%Y')))
            else:
                admsets['Time'].append(None)
                admsets['RP'].append(None)
                matches['Type'].append('Ranked match')
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=matches['Time'],
                    y=matches['RP'],
                    mode='lines+markers+text',
                    textposition='bottom right',
                    line_shape='linear',
                    name='Matches',
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=admsets['Time'],
                    y=admsets['RP'],
                    mode='markers+text',
                    textposition='top right',
                    name='Admin Override',
                    marker=dict(color=str(discord.Color.orange()).replace('0x','#')),
                )
            )
            fig.update_layout(
                margin=dict(t=90),
                legend=dict(y=-0.2, x=0.75, font_size=10),
                title = dict(
                    text = 'UTA PUG Ranked Stats for {0} ({1})'.format(playername,mode),
                    font = dict(size=20, weight=300),
                    subtitle = dict(text='https://utassault.net/discord', font=dict(size=14)),
                ),
                xaxis=dict(
                    title=dict(
                        text='Time',
                        font = dict(size=12, weight=300),
                    ),
                    gridcolor=str(discord.Color.dark_grey()).replace('0x','#')
                ),
                yaxis=dict(
                    title=dict(
                        text='Rank/Power (RP)',
                        font = dict(size=12, weight=300),
                    ),
                    gridcolor=str(discord.Color.dark_grey()).replace('0x','#')
                ),
                paper_bgcolor=str(discord.Color.dark_embed()).replace('0x','#'),
                plot_bgcolor=str(discord.Color.dark_theme()).replace('0x','#'),
                legend_font_color=str(discord.Color.lighter_grey()).replace('0x','#'),
                hoverlabel_font_color=str(discord.Color.blurple()).replace('0x','#'),
                title_font_color=str(discord.Color.orange()).replace('0x','#'),
                title_subtitle_font_color=str(discord.Color.blurple()).replace('0x','#'),
                font_color=str(discord.Color.light_embed()).replace('0x','#'),
                colorway=[
                    str(discord.Color.brand_green()).replace('0x','#'),
                    str(discord.Color.blue()).replace('0x','#'),                   
                    str(discord.Color.magenta()).replace('0x','#'),
                    str(discord.Color.orange()).replace('0x','#'),
                    str(discord.Color.gold()).replace('0x','#')
                ]
            )
            if os.path.exists('images/uta-logo-sm.jpg'):
                log.debug('generateRankHistory() adding logo')
                fig.add_layout_image(
                    dict(
                        source=Image.open('images/uta-logo-sm.jpg'),
                        xref="paper",
                        yref="paper",
                        x=1.1,
                        y=1.2,
                        sizex=0.2,
                        sizey=0.2,
                        xanchor="right",
                        yanchor="top",
                        opacity=0.6)
                )
            fig.write_image(image)
        return image


    @commands.hybrid_command(aliases=['rkstat','elo','rp','rating','ratings'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkstats(self, ctx, player: discord.Member = None, mode: str = 'rASPlus'):
        """Shows player rank history within a game mode: PlayerNick GameMode(e.g. rASPlus)"""
        if player == None:
            player = ctx.message.author
        pid = player.id
        ctx.bot.get_cog('PUG').pugInfo.savePugRatings(self.ratingsFile)
        rkData = ctx.bot.get_cog('PUG').pugInfo.loadPugRatings(self.ratingsFile, True)
        if rkData not in [None,'']:
            if 'rankedgames' in rkData:
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        mode = x['mode']
                        if pid in x['registrations']:
                            for r in x['ratings']:
                                if r['did'] == pid:
                                    pdata = r
        if pdata:                        
            # await ctx.send('Showing RP history for {0}'.format(player.display_name))
            if len(pdata['lastgamedate']) > 0:
                g_last = datetime.fromisoformat(pdata['lastgamedate'])
            else:
                g_last = datetime.fromisoformat(pdata['ratingdate'])
            r_current = pdata['ratingvalue']
            image = self.generateRankHistory(ctx, mode, pid, pdata, player.display_name)
            embedInfo = discord.Embed(color=discord.Color.dark_embed(),title='Ratings history for {0}'.format(player.display_name),description='Last match/update: {0}; Current RP: **{1}**'.format(g_last.strftime('%d/%m/%Y @ %H:%M:%S'),r_current))
            file = discord.File(image, filename='{0}.png'.format(pid))
            embedInfo.set_image(url='attachment://{0}.png'.format(pid))
            await ctx.send(file=file, embed=embedInfo)
        return True


async def setup(bot):
    await bot.add_cog(PlayerChart(bot))

