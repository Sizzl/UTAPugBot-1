from discord.ext import commands
import discord
import plotly.graph_objects as go
from cogs import admin
from PIL import Image 
from datetime import datetime
import os
import logging
import sys
import re
import hashlib

DEFAULT_RATING_FILE = 'players/ratings.json'
#########################################################################################
# Logging
#########################################################################################
log = admin.setupLogging('charts',logging.DEBUG,logging.DEBUG)
log.info('Charts extension loaded with logging...')

#########################################################################################
# Charting
#########################################################################################
class PlayerChart(commands.Cog):
    """Renders charts for Player related data"""
    def __init__(self, bot, ratingsFile=DEFAULT_RATING_FILE):
        self.bot = bot
        self.ratingsFile = ratingsFile

    def getRankStats(self, mode, rkData, pids):
        """Fetches data for one or more players and passes to generateRankHistory()"""
        rsData = {'image':None,'g_last':None,'r_current':0}
        pdata = []
        fpids = []
        fplayers = []
        if rkData not in [None,'']:
            if 'rankedgames' in rkData:
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        mode = x['mode']
                        for r in x['ratings']:
                            if r['did'] in pids:
                                pdata.append(r)
                                fpids.append(r['did'])
                                fplayers.append(r['dlastnick'])
        if len(pdata):
            if len(pdata) == 1:
                if len(pdata[0]['lastgamedate']) > 0:
                    rsData['g_last'] = datetime.fromisoformat(pdata[0]['lastgamedate'])
                else:
                    rsData['g_last'] = datetime.fromisoformat(pdata[0]['ratingdate'])
                rsData['r_current'] = pdata[0]['ratingvalue']
            rsData['image'] = self.generateRankHistory(mode, fpids, pdata, fplayers)
        return rsData

    def generateRankHistory(self, mode, pids, datasets, playernames, teamname = 'multiple players', force = True):
        """Uses plotly to generate graphs of incoming datasets"""
        if not os.path.exists('images'):
            os.mkdir('images')
        if type(pids) is int:
            pid = pids
            data = datasets
            playername = playernames
            pids = []
            datasets = []
            playernames = []
            pids.append(pid)
            datasets.append(data)
            playernames.append(playername)
        elif type(pids) is not list:
            return
        else:
            pid = pids[0]
            playername = playernames[0]
            data = datasets[0]
        if len(pids) != len(datasets) and len(datasets) != len(playernames):
            return
        hfpids = ''.join(map(str, pids))
        if len(data['lastgamedate']):
            g_date = datetime.fromisoformat(data['lastgamedate'])
        else:
            g_date = datetime.fromisoformat(data['ratingdate'])
        hashedfile = hashlib.md5('{0}_{1}_{2}'.format(data['lastgameref'],g_date.strftime('%Y%m%d%H%M%S'),hfpids).encode())
        image = 'images/{0}.png'.format(hashedfile.hexdigest())
        if not os.path.exists(image) or force:
            fig = go.Figure()
            admsets = {'Time':[],'RP':[],'Player':[]}
            for i in range(len(datasets)):
                data = datasets[i]
                playername = playernames[i]
                pid = pids[i]
                matches = {'Time':[],'RP':[],'Player':[]}
                if 'ratinghistory' in data and len(data['ratinghistory']):
                    history = sorted(data['ratinghistory'], key=lambda m: datetime.fromisoformat(m['matchdate']))
                    for h in history:
                        h_date = datetime.fromisoformat(h['matchdate'])
                        matches['Time'].append(h_date)
                        matches['RP'].append(h['ratingafter'])
                        matches['Player'].append(playername)
                        if len(h['matchref']) == 0 or h['matchref'] == 'admin-set':
                            admsets['Time'].append(h_date)
                            admsets['RP'].append(h['ratingafter'])
                            admsets['Player'].append(playername)
                        else:
                            admsets['Time'].append(None)
                            admsets['RP'].append(None)
                            admsets['Player'].append(None)
                if len(data['lastgamedate']):
                    g_date = datetime.fromisoformat(data['lastgamedate'])
                else:
                    g_date = datetime.fromisoformat(data['ratingdate'])
                matches['Time'].append(g_date)
                matches['RP'].append(data['ratingvalue'])
                matches['Player'].append(playername)
                if len(data['lastgameref']) == 0 or data['lastgameref'] == 'admin-set':
                    admsets['Time'].append(g_date)
                    admsets['RP'].append(data['ratingvalue'])
                    admsets['Player'].append(playername)
                else:
                    admsets['Time'].append(None)
                    admsets['RP'].append(None)
                    admsets['Player'].append(None)
                if len(playernames) > 1:
                    t_name = playername
                else:
                    t_name = 'Matches'
                fig.add_trace(
                    go.Scatter(
                        x=matches['Time'],
                        y=matches['RP'],
                        mode='lines+markers+text',
                        textposition='bottom right',
                        line_shape='linear',
                        name=t_name,
                    )
                )
            if len(admsets):
                fig.add_trace(
                    go.Scatter(
                        x=admsets['Time'],
                        y=admsets['RP'],
                        mode='markers+text',
                        textposition='top right',
                        name='Admin override',
                        marker=dict(color=str(discord.Color.orange()).replace('0x','#')),
                    )
                )
            if len(playernames) > 1:
                if len(playernames) > 2:
                    c_title = teamname
                else:
                    c_title = ', '.join(playernames)
                c_legend=dict(y=-0.3, x=0, font_size=10, orientation='h',yanchor='bottom',xanchor='left')
                if len(playernames) > 5:
                    c_legend['y'] = -0.35
                c_height = 510
            else:
                c_title = playernames[0]
                c_legend=dict(y=-0.2, x=0.75, font_size=10, orientation='v')
                c_height = 500
            fig.update_layout(
                legend=c_legend,
                height=c_height,
                margin=dict(t=90),
                title = dict(
                    text = 'UTA PUG Ranked Stats for {0} ({1})'.format(c_title,mode),
                    font = dict(size=20, weight=300),
                    subtitle = dict(text='https://utassault.net/discord', font=dict(size=14)),
                ),
                xaxis=dict(
                    title=dict(
                        text='',
                        font = dict(size=10, weight=300),
                    ),
                    gridcolor=str(discord.Color.dark_grey()).replace('0x','#')
                ),
                yaxis=dict(
                    title=dict(
                        text='Rank/Power (RP)',
                        font = dict(size=10, weight=300),
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
                    str(discord.Color.brand_red()).replace('0x','#'),
                    str(discord.Color.blue()).replace('0x','#'),                   
                    str(discord.Color.magenta()).replace('0x','#'),
                    str(discord.Color.blurple()).replace('0x','#'),
                    str(discord.Color.gold()).replace('0x','#'),
                    str(discord.Color.teal()).replace('0x','#'),
                    str(discord.Color.dark_blue()).replace('0x','#'),
                    str(discord.Color.dark_magenta()).replace('0x','#'),
                    str(discord.Color.dark_gold()).replace('0x','#'),
                    str(discord.Color.dark_teal()).replace('0x','#'),
                ]
            )
            if os.path.exists('images/uta-logo-sm.jpg'):
                log.debug('generateRankHistory() adding logo')
                fig.add_layout_image(
                    dict(
                        source=Image.open('images/uta-logo-sm.jpg'),
                        xref='paper',
                        yref='paper',
                        x=1.1,
                        y=1.2,
                        sizex=0.2,
                        sizey=0.2,
                        xanchor='right',
                        yanchor='top',
                        opacity=0.6)
                )
            fig.write_image(image)
        return image


    @commands.hybrid_command(aliases=['rkstat','elo','rp','rating','ratings'])
    @commands.guild_only()
    async def rkstats(self, ctx, player: discord.Member = None, mode: str = 'rASPlus'):
        """Shows player rank history within a game mode: PlayerNick GameMode(e.g. rASPlus)"""
        if player == None:
            player = ctx.message.author
        pid = player.id
        pids = []
        pids.append(pid)
        rkData = ctx.bot.get_cog('PUG').pugInfo.loadPugRatings(self.ratingsFile, True)
        rsResult = self.getRankStats(mode, rkData, pids)
        if rsResult not in [None,'']:
            embedInfo = discord.Embed(color=discord.Color.dark_embed(),title='Ratings history for {0}'.format(player.display_name))
            if 'g_last' in rsResult and rsResult['g_last'] not in [None, '']:
                embedInfo.description='Last match/update: {0}; Current RP: **{1}**'.format(rsResult['g_last'].strftime('%d/%m/%Y @ %H:%M:%S'), rsResult['r_current'])
            file = discord.File(rsResult['image'], filename='{0}.png'.format(pid))
            embedInfo.set_image(url='attachment://{0}.png'.format(pid))
            await ctx.send(file=file, embed=embedInfo)
        return True

    @commands.hybrid_command(aliases=['ego','rkts','rkteam'])
    @commands.guild_only()
    async def rkmpstats(self, ctx, mode: str = 'rASPlus', player1: discord.Member = None, player2: discord.Member = None, player3: discord.Member = None, player4: discord.Member = None, player5: discord.Member = None, player6: discord.Member = None):
        """Compares player rank history within a game mode: GameMode(e.g. rASPlus) Player1 Player2 <Player3..>"""
        pid = re.search(r'<@(\d*)>', mode)
        if (pid):
            player6 = player5
            player5 = player4
            player4 = player3
            player3 = player2
            player2 = player1
            player1 = mode
            mode = 'rASPlus'
        if player1 in [None,''] and player2 in [None,'']:
            await ctx.send('Please provide a valid mode and at least two players for comparison.')
            return True
        pids = []
        playernames = []
        for p in [player1,player2,player3,player4,player5,player6]:
            if p not in [None,'']:
                pids.append(p.id)
                playernames.append(p.display_name)
        log.debug('rkmpstats() - Generating graph for players {0}'.format(', '.join(playernames)))
        if len(pids) and len(playernames) and len(pids) == len(playernames):
            rkData = ctx.bot.get_cog('PUG').pugInfo.loadPugRatings(self.ratingsFile, True)
            rsResult = self.getRankStats(mode, rkData, pids)
            if rsResult not in [None,''] and 'image' in rsResult and rsResult['image'] not in [None,'']:
                embedInfo = discord.Embed(color=discord.Color.dark_embed())
                if len(playernames) > 2:
                    embedInfo.title='Ratings comparison for multiple players'
                    embedInfo.description='{0}'.format(' vs. '.join(playernames))
                else:
                    embedInfo.title='Ratings comparison for {0}'.format(', '.join(playernames))
                file = discord.File(rsResult['image'], filename='{0}.png'.format(pid))
                embedInfo.set_image(url='attachment://{0}.png'.format(pid))
                await ctx.send(file=file, embed=embedInfo)
        return True
    
async def setup(bot):
    await bot.add_cog(PlayerChart(bot))