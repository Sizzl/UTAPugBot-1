from discord.ext import commands
import discord
from cogs import admin
from datetime import datetime, timedelta
import os
import logging
import sys
import re
import configparser

DEFAULT_RECORDS_FILE = 'players/BTPlusPlus.ini'
DEFAULT_RECORDS_TYPE = 'BTPlusPlusv0994.ServerRecords'
DEFAULT_THUMBNAIL_SERVER = 'https://utassault.net/pugstats/images/maps/'

#########################################################################################
# Logging
#########################################################################################
log = admin.setupLogging('recs-bt',logging.DEBUG,logging.DEBUG)
log.info('BunnyTrack records extension loaded with logging...')

#########################################################################################
# Records viewer
#########################################################################################
class PlayerBTRecords(commands.Cog):
    """Shows records for BunnyTrack """
    def __init__(self, bot, recordsFile=DEFAULT_RECORDS_FILE, recordsType=DEFAULT_RECORDS_TYPE, thumbnailServer=DEFAULT_THUMBNAIL_SERVER):
        self.bot = bot
        self.recordsFile = recordsFile
        self.recordsType = recordsType
        self.thumbnailServer = thumbnailServer
        self.lastCache = None
        self.records = []
        self.maxCapTime = 600000

    def loadBTini(self):
        if os.path.exists(self.recordsFile):
            btini = configparser.ConfigParser()
            btini.read(self.recordsFile,encoding='utf-8')
            if self.recordsType in btini:
                for x in range(0,3000):
                    rec = btini[self.recordsType].get('Records[{0}]'.format(x), '')
                    if rec not in ['',None,'(M="",C=0,t=0,P="")']:
                        rec = rec[1:-1]
                        crec = {k: v for k, v in [i.split('=',1) for i in rec.split(',')]}
                        if 'M' in crec:
                            crec['M'] = crec['M'].replace('"','')
                        if 'P' in crec:
                            crec['P'] = crec['P'].replace('"','')
                        self.records.append(crec)
                self.lastCache = datetime.now()
                return True
            else:
                log.error('loadBTini() - failed to load configuration section {0} from configuration file {1}'.format(self.recordsType,self.recordsFile))
                return False
        else:
            log.error('loadBTini() - failed to load configuration file {0}'.format(self.recordsFile))
            return False

    def formatCentiseconds(self, cs: int = 0):
        if cs <= 0 or cs >= self.maxCapTime:
            return '-:--'
        cs = self.maxCapTime - cs 
        if cs/100 < 60:
            secs = str(cs//100)
            ms = '{:02}'.format(cs%100)
            return '{0}.{1}s'.format(secs,ms)
        else:
            mins = (cs//100)//60
            secs = (cs//100)%60
            secs = '{:02}'.format(secs)
            ms = cs%100
            return '{0}m {1}.{2}s'.format(mins, secs, ms)

    @commands.hybrid_command(aliases=['btrec'])
    @commands.guild_only()
    async def btrecs(self, ctx, map: str = '', player: str = ''):
        """Shows player records for given maps"""
        if len(self.records) == 0 or self.lastCache == None:
            self.loadBTini()
        elif len(self.records) > 0 and self.lastCache < (datetime.now() - timedelta(minutes=5)):
            self.loadBTini()
        if len(self.records):
            embedInfo = discord.Embed(color=discord.Color.blurple(),title='')
            records = []
            for r in self.records:
                match = False
                if len(map) == 0 and len(player) == 0:
                    match = True
                if len(map) > 0:
                    if re.search(r'{0}'.format(map), r['M']) or re.search(r'{0}'.format(map), r['P']):
                        match = True
                if len(player) > 0:
                    if re.search(r'{0}'.format(player), r['M']) or re.search(r'{0}'.format(player), r['P']):
                        match = True
                if match:
                    records.append({
                        'map': r['M'],
                        'captime': r['C'],
                        'datestamp': int(r['t']),
                        'datetime': datetime.fromtimestamp(int(r['t'])).strftime('%H:%M:%S on %d/%m/%Y'),
                        'player': r['P']
                    })
            if len(records):
                records = sorted(records, key=lambda r: r['datestamp'], reverse=True)
                top = ''
                if len(records) == 1:
                    player = '{0} on '.format(records[0]['player'])
                    map = records[0]['map']
                    embedInfo.set_thumbnail(url='{0}{1}.jpg'.format(self.thumbnailServer,str(records[0]['map'].lower())))
                else:
                    if map == '' and player == '':
                        player = 'all players on '
                        map = 'all maps'
                    elif player == '.*':
                        player = map
                        map = ''                
                if len(records) > 5:
                    top = 'Top 5 recent '
                    records = records[0:5]
                elif len(records) > 1:
                    top = 'All '
                embedInfo.title = ('{0}BunnyTrack records for {1} {2}'.format(top, player, map)).replace('  ',' ')
                embedInfo.description = 'Records last fetched at {0}'.format(self.lastCache.strftime('%H:%M:%S on %d/%m/%Y'))
                for r in records:
                    embedInfo.add_field(name='Map',value=r['map'],inline=False)
                    embedInfo.add_field(name='Cap Time',value='{0}'.format(self.formatCentiseconds(int(r['captime']))))
                    embedInfo.add_field(name='Record Date',value=r['datetime'])
                    embedInfo.add_field(name='Record Holder',value=r['player'])
                    #embedInfo.add_field(name='\u200B', value='\u200B')
                await ctx.send(embed=embedInfo)
            else:
                await ctx.send('No records could be found with the parameters provided.')
        else:
            await ctx.send('Records could not be obtained.')
        return True
    
async def setup(bot):
    await bot.add_cog(PlayerBTRecords(bot))