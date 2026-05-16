import collections
import time
import asyncpg
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from math import gcd
import functools
import itertools
import logging
import random
import re
import requests # should replace with aiohttp. See https://discordpy.readthedocs.io/en/latest/faq.html#what-does-blocking-mean
import json
import discord
import socket
import dns.resolver
from discord.ext import commands, tasks
from cogs import admin

# Commands and maps from the IRC bot:
#
#<UTAPugbot> !pughelp !reset !setmaps !setplayers !listmaps !pug !letter !status !server !captainmode !launchprotect !version
#<UTAPugbot> during pug setup: !player !randplayer !map !randmap !showteams !showmaps !captain
#
#Maplist:
#Server map list is: 1.AS-AsthenosphereSE 2.AS-AutoRip 3.AS-Ballistic 4.AS-Bridge 5.AS-Desertstorm 6.AS-Desolate][ 7.AS-Frigate
#8.AS-GolgothaAL 9.AS-Golgotha][AL 10.AS-Mazon 11.AS-RiverbedSE 12.AS-Riverbed]l[AL 13.AS-Rook 14.AS-Siege][
#15.AS-Submarinebase][ 16.AS-SaqqaraPE_preview3 17.AS-SnowDunes][AL_beta 18.AS-LostTempleBetaV2 19.AS-TheDungeon]l[AL
#20.AS-DustbowlALRev04 21.AS-NavaroneAL 22.AS-TheScarabSE 23.AS-Vampire 24.AS-ColderSteelSE_beta3 25.AS-HiSpeed
#26.AS-NaliColony_preview5 27.AS-LavaFort][PV 28.AS-BioassaultSE_preview2 29.AS-Razon_preview3 30.AS-Resurrection
#31.AS-WorseThings_preview 32.AS-GekokujouAL][

DEFAULT_PLAYERS = 12
DEFAULT_MAPS = 5
DEFAULT_PICKMODETEAMS = 1 # Fairer for even numbers (players should be even, 1st pick gets 1, 2nd pick gets 2)
DEFAULT_PICKMODEMAPS = 3 # Fairer for odd numbers (maps are usually odd, so 2nd pick should get more picks)

DEFAULT_GAME_SERVER_REF = 'pugs1'
DEFAULT_GAME_SERVER_IP = '0.0.0.0'
DEFAULT_GAME_SERVER_PORT = '7777'
DEFAULT_GAME_SERVER_NAME = 'Unknown Server'
DEFAULT_ACCOUNT_URL = 'https://utassault.net/discord/link'
DEFAULT_POST_SERVER = 'https://utassault.net'
DEFAULT_POST_TOKEN = 'NoToken'
DEFAULT_STATS_URL = f'{DEFAULT_POST_SERVER}/pugstats/index.php?p=utapugrecent'
DEFAULT_STATS_MATCH_URL = f'{DEFAULT_POST_SERVER}/pugstats/index.php?p=uta_match&matchcode='
DEFAULT_THUMBNAIL_SERVER = f'{DEFAULT_POST_SERVER}/pugstats/images/maps/'
DEFAULT_CONFIG_FILE = 'servers/config.json'
DEFAULT_RATING_FILE = 'players/ratings.json'

# Valid modes with default config
Mode = collections.namedtuple('Mode', 'name isRanked minPlayers maxPlayers friendlyFireScale gameType mutators modeGroup')
MODE_CONFIG = {
    'stdAS': Mode('Assault', False, 2, 20, 0, 'LeagueAS140.LeagueAssault', None, 0),
    'proAS': Mode('Pro Assault', False, 2, 20, 100, 'LeagueAS140.LeagueAssault', None, 0),
    'ASplus': Mode('Assault Plus', False, 2, 20, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlus', 1),
    'rASplus': Mode('Ranked Assault', True, 8, 14, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlus,rAS140.RankedAS', 1),
    'proASplus': Mode('Pro Assault Plus', False, 2, 20, 100, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlus', 1),
    'pcASplus': Mode('Ping-Compensated Assault Plus', False, 2, 20, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlusPC', 2),
    'rASpc': Mode('Ping-Compensated Ranked AS', True, 8, 20, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlusPC,rAS140.RankedAS', 2),
    'ZPiAS': Mode('InstaGib Assault', False, 2, 20, 0, 'LeagueAS140.LeagueAssault', 'ZeroPingPlus103.ColorAccuGib', 0)
}
MODE_DEFAULT = 'ASplus'
MODE_RANKED_DEFAULT = 'rASplus'

RED_PASSWORD_PREFIX = 'RP'
BLUE_PASSWORD_PREFIX = 'BP'
DEFAULT_SPECTATOR_PASSWORD = 'pug'
DEFAULT_NUM_SPECTATORS = 4
DEFAULT_RED_PASSWORD = RED_PASSWORD_PREFIX + '000'
DEFAULT_BLUE_PASSWORD = BLUE_PASSWORD_PREFIX + '000'

RATED_CAP_MODE = {
    0: 'No captains selected',
    1: 'Captains randomly selected',
    2: 'Captains preferenced by Discord role',
    3: 'Self-nominated captains within a time-window'
}

# Map list:
#  List of League Assault default maps.
#  This hardcoded list will be replaced through JSON config upon bot load

DEFAULT_MAP_LIST = [
    'AS-AsthenosphereSE',
    'AS-AutoRip',
    'AS-Ballistic',
    'AS-Bridge',
    'AS-Desertstorm',
    'AS-Desolate][',
    'AS-Frigate',
    'AS-GolgothaAL',
    'AS-Golgotha][AL',
    'AS-Guardia',
    'AS-GuardiaAL',
    'AS-HiSpeed',
    'AS-Mazon',
    'AS-OceanFloor',
    'AS-OceanFloorAL',
    'AS-Overlord',
    'AS-RiverbedSE',
    'AS-Riverbed]l[AL',
    'AS-Rook',
    'AS-Siege][',
    'AS-Submarinebase][',
    'AS-TheDungeon]l[AL',
]

# Server list:
#  List of Tuples, first element is the API reference, second element is the placeholder
#  Name / Description of the server, which is updated after a successful check.
#  This hardcoded list will be replaced through JSON config upon bot load, and subsequently
#  verified against the online API.

DEFAULT_SERVER_LIST = [
    ('pugs1','UTA Pug Server 1.uk','unreal://pug1.utassault.net',False,'')
]

PICKMODES = [
        [0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
        [0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0],
        [0, 1, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
        [0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0]]
MAX_PLAYERS_LIMIT = len(PICKMODES[0]) + 2

PLASEP = '\N{SMALL ORANGE DIAMOND}'
MODSEP = '\N{SMALL BLUE DIAMOND}'
OKMSG = '\N{OK HAND SIGN}'
CAPSIGN = '\N{CROWN}'
GRAPHUP = '\N{CHART WITH UPWARDS TREND}'
GRAPHDN = '\N{CHART WITH DOWNWARDS TREND}'
UP = '<:smallgreenuparrow:1359842129433596067>'
DN = '\U0001F53B'

DISCORD_MD_CHARS = '*~_`'
DISCORD_MD_ESCAPE_RE = re.compile(f'[{DISCORD_MD_CHARS}]')
DISCORD_MD_ESCAPE_DICT = {c: '\\' + c for c in DISCORD_MD_CHARS}

#########################################################################################
# Logging
#########################################################################################
log = admin.setupLogging('pugbot',logging.DEBUG,logging.DEBUG)
log.info('Pug extension loaded with logging...')

#########################################################################################
# Utilities
#########################################################################################

def discord_md_escape(value):
    return DISCORD_MD_ESCAPE_RE.sub(lambda match: DISCORD_MD_ESCAPE_DICT[match.group(0)], value)

def display_name(member):
    return discord_md_escape(member.display_name)

def getDuration(then, now, interval: str = 'default'):
    # Adapted from https://stackoverflow.com/a/47207182
    duration = now - then
    duration_in_s = duration.total_seconds()

    def years():                    return divmod(duration_in_s, 31536000) # Seconds in a year = 31536000.
    def days(seconds = None):       return divmod(seconds if seconds is not None else duration_in_s, 86400) # Seconds in a day = 86400
    def hours(seconds = None):      return divmod(seconds if seconds is not None else duration_in_s, 3600) # Seconds in an hour = 3600
    def minutes(seconds = None):    return divmod(seconds if seconds is not None else duration_in_s, 60) # Seconds in a minute = 60
    def seconds(seconds = None):    return divmod(seconds, 1) if seconds is not None else duration_in_s
    def totalDuration():
        y = years()
        d = days(y[1]) # Use remainder to calculate next variable
        h = hours(d[1])
        m = minutes(h[1])
        s = seconds(m[1])
        msg = []
        if y[0] > 0:
            msg.append(f'{int(y[0])} years')
        if d[0] > 0:
            msg.append(f'{int(d[0])} days')
        if h[0] > 0:
            msg.append(f'{int(h[0])} hours')
        if m[0] > 0:
            msg.append(f'{int(m[0])} minutes')
        msg.append(f'{int(s[0])} seconds')
        return ', '.join(msg)
    return {'years': int(years()[0]),'days': int(days()[0]),'hours': int(hours()[0]),'minutes': int(minutes()[0]),'seconds': int(seconds()),'default': totalDuration()}[interval]

#########################################################################################
# CLASS
#########################################################################################
class Players:
    """Maintains the state of a set of players"""
    def __init__(self, maxPlayers, ranked: bool, roleRequired: str = ''):
        self.maxPlayers = maxPlayers
        self.players = []
        self.playerFlags = {}
        self.queuedPlayers = []
        self.ranked = ranked
        self.roleRequired = roleRequired
        self.ratingsData = None

    def __contains__(self, player):
        return player in self.players

    def __iter__(self):
        return iter(self.players)

    def __len__(self):
        return len(self.players)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['players']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.players = []

    #########################################################################################
    # Properties
    #########################################################################################
    @property
    def numPlayers(self):
        return len(self)

    @property
    def playersBrief(self):
        return f'[{self.numPlayers}/{self.maxPlayers}]'

    @property
    def playersFull(self):
        return self.numPlayers == self.maxPlayers

    @property
    def playersNeeded(self):
        return self.maxPlayers - self.numPlayers

    @property
    def playerQueueFull(self):
        return len(self.queuedPlayers) == self.maxPlayers
    
    #########################################################################################
    # Functions
    #########################################################################################
    def addPlayer(self, player, flags: str = ''):
        if flags.lower()[:2] in ['qu','ne']:
            if player not in self.queuedPlayers and not self.playerQueueFull:
                log.debug(f'addPlayer() - Adding player {player.display_name} to pug queue.')
                self.queuedPlayers.append(player)
                return True
        else:
            if player not in self and not self.playersFull:
                self.players.append(player)
                if len(flags):
                    self.playerFlags[player.id] = flags
                return True
        return False

    def addRankedPlayer(self, player, flags: str = ''):
        # Determine eligibility and ratings data present, perform any other checks here
        log.debug(f'addRankedPlayer({player.display_name}) started')
        if self.checkRankedPlayersEligibility([player]):
            if self.addPlayer(player, flags):
                log.debug(f'addRankedPlayer({player.display_name}) succeeded.')
                return True
        return False

    def removePlayer(self, player):
        if player in self:
            self.players.remove(player)
            if player.id in self.playerFlags:
                del self.playerFlags[player.id]
            return True
        return False

    def resetPlayers(self, includeQueuedPlayers: bool = False):
        self.players = []
        self.playerFlags = {}
        if includeQueuedPlayers:
            self.queuedPlayers = []

    def setMaxPlayers(self, numPlayers):
        if (numPlayers < 1 or numPlayers % 2 > 0):
            return
        if numPlayers < MAX_PLAYERS_LIMIT:
            self.maxPlayers = numPlayers
        else:
            self.maxPlayers = MAX_PLAYERS_LIMIT
        # If we have more players, then prune off the end.
        while(len(self) > self.maxPlayers):
            self.players.pop()

    def checkRankedPlayersEligibility(self, players):
        # Checks provided players are eligible and registered for ranked play
        if self.roleRequired:
            checked = 0
            for p in players:
                for role in p.roles:
                    if role.name == self.roleRequired or role.mention == self.roleRequired:
                        checked += 1
            if checked != len(players):
                return False
        if self.ratingsData is not None:
            checked = 0
            if 'registrations' in self.ratingsData:
                for p in players:
                    if p.id in self.ratingsData['registrations'] or str(p.id) in self.ratingsData['registrations']:
                        checked += 1
            else:
                log.debug(f'checkRankedPlayersEligibility({players}) - registrations list not present in ratingsData.')
            if checked != len(players):
                return False
        return True

    def configurePlayersRankedMode(self, isRanked: bool, eligibilityRole: str = '', ratingsData=None):
        self.ranked = isRanked
        self.roleRequired = eligibilityRole
        self.ratingsData = ratingsData
        return True

#########################################################################################
# CLASS
#########################################################################################
class PugMaps:
    """Maintains the state of a set of maps for a pug"""
    def __init__(self, maxMaps, pickMode, rankedMode, mapList):
        self.maxMaps = maxMaps
        self.rankedMode = rankedMode
        self.autoPickShuffled = False
        self.desirabilityReduction = 2
        self.desirabilityMultiplier = 100
        self.pickMode = pickMode
        self.availableMapsList = mapList
        self.filteredMapsList = mapList
        self.mapListWeighting = None
        self.maps = []
        self.cooldownMaps = []
        self.cooldownCount = 0
        self.startMapFromPick = 0
        self.startMap = ''

    def __contains__(self, map):
        return map in self.maps

    def __iter__(self):
        return iter(self.maps)

    def __len__(self):
        return len(self.maps)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['maps']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.maps = []

    #########################################################################################
    # Properties
    #########################################################################################
    @property
    def mapsFull(self):
        return len(self) == self.maxMaps

    @property
    def currentTeamToPickMap(self):
        return PICKMODES[self.pickMode][len(self.maps)]

    @property
    def maxMapsLimit(self):
        return len(self.availableMapsList)

    #########################################################################################
    # Formatted strings
    #########################################################################################
    def format_maplist(self, maps, filteredOnly: bool = False):
        indexedMaps = self.indexMaps(maps)
        if (self.rankedMode and filteredOnly == False and len(maps) != self.maxMaps):
            # Ranked modes can offer a sub-set of the maplist,
            # However, our smooth-brained players don't like change, 
            # So all the numbers must stay the same if we're showing all maps...
            # Even if we're only giving them the illusion of choice.
            listedMaps = []
            unlistedMaps = []
            for idx, x in indexedMaps:
                if (x in self.filteredMapsList):
                    listedMaps.append(f'**{idx})** __{x}__')
                else:
                    listedMaps.append(f'**{idx})** ~~{x}~~')
            for x in self.filteredMapsList:
                if (x not in maps):
                    unlistedMaps.append(x)
            if len(unlistedMaps) > 0:
                msg = '\n'.join([PLASEP.join(listedMaps),'Plus additional ranked-mode specific maps:',PLASEP.join(unlistedMaps)])
            else:
                msg = PLASEP.join(listedMaps)
            return msg
        else:
            if (self.rankedMode and len(maps) != self.maxMaps):
                uniqMaps = list(set(indexedMaps))
                return PLASEP.join(f'{x[1]}' for x in uniqMaps)
            else:
                return PLASEP.join(f'**{x[0]})** {x[1]}' for x in indexedMaps)

    @property
    def format_filtered_maplist(self):
        return self.format_maplist(self.filteredMapsList, True)
    
    @property
    def format_available_maplist(self):
        return self.format_maplist(self.availableMapsList)

    @property
    def format_current_maplist(self):
        return self.format_maplist(self.maps)

    #########################################################################################
    # Functions
    #########################################################################################
    def indexMaps(self, maps):
        indexedMaplist = ((i, m) for i, m in enumerate(maps, 1) if m)
        return indexedMaplist

    #########################################################################################
    # Maintaining Available Maplist
    #########################################################################################
    def validateAvailableListIndex(self, index: int):
        return index >= 0 and index < len(self.availableMapsList)

    def validateAvailableListInsertIndex(self, index: int):
        return index >= 0 and index <= len(self.availableMapsList)

    def validateAvailableListNewMap(self, map: str):
        # Can't really verify the map, but ignore blank/number/None inputs.
        return (map not in self.availableMapsList and map not in [None, ''] and not map.isdigit())

    def addMapToAvailableList(self, map: str):
        if self.validateAvailableListNewMap(map):
            self.availableMapsList.append(map)
            return True
        return False

    def substituteMapInAvailableList(self, index: int, map: str):
        # Index must be passed in as 0-based.
        if self.validateAvailableListIndex(index) and self.validateAvailableListNewMap(map):
            self.availableMapsList[index] = map
            return True
        return False

    def insertMapIntoAvailableList(self, index: int, map: str):
        # Index must be passed in as 0-based.
        if self.validateAvailableListInsertIndex(index) and self.validateAvailableListNewMap(map):
            self.availableMapsList.insert(index, map)
            return True
        return False

    def removeMapFromAvailableList(self, map: str):
        if map and map in self.availableMapsList:
            self.availableMapsList.remove(map)
            return True
        return False

    def getMapFromAvailableList(self, index: int):
        # Index must be passed in as 0-based.
        if self.validateAvailableListIndex(index):
            return self.availableMapsList[index]
        return None

    #########################################################################################
    # Picking a set of maps for a pug
    #########################################################################################
    def autoPickRankedMaps(self, simulate: bool = False):
        simulatedMaps = []
        simulatedMapsStr = []
        mapRatios = {}
        if self.rankedMode and self.filteredMapsList not in [None,'']:
            for i in range(self.maxMaps):
                opts = []
                pick = None
                dCheck = 0
                dCount = 0
                if self.mapListWeighting is not None:
                    for m in self.mapListWeighting:
                        # Run through ordered picks only, to test desirability
                        if (int(m['order']) == (i+1)) and (m['map'] not in self.maps and m['map'] not in simulatedMaps):
                            dCheck += 1
                            if 'desirability' in m:
                                if m['desirability'] < m['weight']*self.desirabilityMultiplier:
                                    dCount +=1
                    for m in self.mapListWeighting:
                        if (int(m['order']) == (i+1) or int(m['order']) == 0) and (m['map'] not in self.maps and m['map'] not in simulatedMaps):
                            if 'weight' in m and m['weight'] > 0:
                                if 'desirability' not in m or dCount == dCheck:
                                    m['desirability'] = m['weight']*self.desirabilityMultiplier
                                m['desirability'] = max(1, min(int(m['desirability']), 500))
                                for x in range(m['desirability']):
                                    opts.append(m['map'])
                                mapRatios[m['map']] = m['desirability']
                            else:
                                opts.append(m['map'])
                                mapRatios[m['map']] = 1
                if len(opts):
                    pick = random.choice(opts)
                else:
                    opts = self.filteredMapsList
                    while (pick == None or pick in self.maps or pick in simulatedMaps):
                        pick = random.choice(opts) # fallback to whole list if position has failed to pick
                    mapRatios[pick] = 1
                    log.debug(f'autoPickRankedMaps() - Reverted to full maplist for pick {str(len(simulatedMaps)+1)} of {str(self.maxMaps)} [order preference {str((i+1))}] - {pick}')
                if pick not in [None,'']:
                    if self.startMapFromPick > 0 and self.startMapFromPick == (i+1):
                        self.startMap = pick
                    mapTotals = 0
                    for value in mapRatios.values():
                        mapTotals += value
                    mapDiv = gcd(mapRatios[pick],mapTotals)
                    mapRatio = f'{str(mapRatios[pick]//mapDiv)}:{str(mapTotals//mapDiv)}'
                    if simulate:
                        log.debug(f'autoPickRankedMaps() - Simulating map pick {str(len(simulatedMaps)+1)} of {str(self.maxMaps)} [order preference {str((i+1))}] - {pick}; slot chances - {mapRatio}')
                        simulatedMaps.append(pick)
                        simulatedMapsStr.append(pick)
                        # simulatedMapsStr.append(f'{pick} *({mapRatio} chance)*')
                    else:
                        log.debug(f'autoPickRankedMaps() - Adding map {str(len(self.maps)+1)} of {str(self.maxMaps)} [order preference {str((i+1))}] - {pick}; slot chances - {mapRatio}')
                        self.maps.append(pick)
                    if self.mapListWeighting is not None:
                        for m in self.mapListWeighting:
                            if m['map'] == pick:
                                if 'desirability' not in m:
                                    m['desirability'] = m['weight']*self.desirabilityMultiplier
                                m['desirability'] = max(1, int(round(m['desirability']/self.desirabilityReduction,0)))
            if self.autoPickShuffled:
                if simulate:
                    random.shuffle(simulatedMapsStr)
                else:
                    random.shuffle(self.maps)
            if simulate:
                return simulatedMapsStr
            return True
        return False
    
    def adjustRankedMapDesirability(self, action: str = 'revert', map: str = '', adjustment: int = 1):
        newDesirability = 0
        if action.lower() == 'resetall':
            if self.mapListWeighting is not None:
                for m in self.mapListWeighting:
                    log.debug(f'adjustRankedMapDesirability() - resetting {m["map"]} to default desirability')
                    m['desirability'] = m['weight']*self.desirabilityMultiplier
            return True
        elif action.lower() == 'mapincrease' or action.lower() == 'mapdecrease':
            if len(map) and self.mapListWeighting is not None:
                for m in self.mapListWeighting:
                    if str(m['map']).lower() == map.lower():
                        if action.lower() == 'mapdecrease':
                            newDesirability = min(m['desirability']/max(1,adjustment),m['weight']*self.desirabilityMultiplier)
                        else:
                            newDesirability = min(m['desirability']*max(1,adjustment),m['weight']*self.desirabilityMultiplier)
                        log.debug(f'adjustRankedMapDesirability() - adjusting {m["map"]} desirability; from: {m["desirability"]}, to: {newDesirability}')
                        m['desirability'] = newDesirability
            if newDesirability > 0 and self.mapListWeighting is not None:
                return True
            return False
        else:
            log.debug(f'adjustRankedMapDesirability() called for maplist length - {str(len(self.maps))}')
            for pick in self.maps:
                if self.mapListWeighting is not None:
                    for m in self.mapListWeighting:
                        if m['map'] == pick:
                            if 'desirability' not in m:
                                log.debug(f'adjustRankedMapDesirability() - resetting {pick} to default desirability')
                                m['desirability'] = m['weight']*self.desirabilityMultiplier
                            else:
                                log.debug(f'adjustRankedMapDesirability() - reverting {pick} desirability to {str(int(round(m["desirability"]*self.desirabilityReduction,0)))}')
                                m['desirability'] = max(1, min(int(round(m['desirability']*self.desirabilityReduction,0)), m['weight']*self.desirabilityMultiplier))
        return True
    
    def setMaxMaps(self, numMaps: int):
        if numMaps > 0 and numMaps <= self.maxMapsLimit:
            self.maxMaps = numMaps
            return True
        return False

    def addMap(self, index: int):
        if self.mapsFull:
            return False
        map = self.getMapFromAvailableList(index)
        if map and map not in self:
            self.maps.append(map)
            return True
        return False

    def removeMap(self, map: str):
        if map in self:
            self.maps.remove(map)
            return True
        return False

    def resetMaps(self):
        self.maps = []
        self.startMap = ''

#########################################################################################
# CLASS
#########################################################################################
class Team(list):
    """Represents a team of players with a captain"""
    def __init__(self):
        super().__init__()

    #########################################################################################
    # Properties
    #########################################################################################
    @property
    def captain(self):
        if len(self):
            return self[0]
        return None
#########################################################################################
# CLASS
#########################################################################################
class PugTeams(Players):
    """Represents players who can be divided into 2 teams who captains pick."""
    def __init__(self, maxPlayers: int, pickMode: int, ranked: bool = False, roleRequired: str = ''):
        super().__init__(maxPlayers=maxPlayers, ranked=ranked, roleRequired=roleRequired) # Send to Players()
        self.teams = (Team(), Team())
        self.pickMode = pickMode

    def __contains__(self, player):
        return player in self.all

    def __getstate__(self):
        state = super().__getstate__()
        del state['teams']
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self.teams = (Team(), Team())

    #########################################################################################
    # Properties
    #########################################################################################
    @property
    def numCaptains(self):
        return sum([self.red.captain is not None, self.blue.captain is not None])

    @property
    def captainsFull(self):
        return self.red and self.blue
    
    @property
    def maxPicks(self):
        return self.maxPlayers - 2

    @property
    def currentPickIndex(self):
        return (len(self.red) + len(self.blue) - 2) if self.captainsFull else 0

    @property
    def currentTeamToPickPlayer(self):
        return PICKMODES[self.pickMode][self.currentPickIndex]

    @property
    def currentCaptainToPickPlayer(self):
        return self.teams[self.currentTeamToPickPlayer].captain if self.captainsFull else None

    @property
    def teamsFull(self):
        return len(self.red) + len(self.blue) == self.maxPlayers

    @property
    def currentTeam(self):
        return PICKMODES[self.pickMode][self.currentPickIndex]

    @property
    def red(self):
        return self.teams[0]

    @property
    def blue(self):
        return self.teams[1]

    @property
    def all(self):
        return list(filter(None, self.players + self.red + self.blue)) 

    #########################################################################################
    # Functions:
    #########################################################################################
    def removePugTeamPlayer(self, player):
        if player in self:
            self.softPugTeamReset()
            self.removePlayer(player)
            return True
        return False

    def softPugTeamReset(self):
        if self.red or self.blue:
            self.players += self.red + self.blue
            self.players = list(filter(None, self.players))
            self.teams = (Team(), Team())
            self.here = [True, True]
            return True
        return False

    def fullPugTeamReset(self, manualReset: bool = False):
        self.softPugTeamReset()
        self.resetPlayers(manualReset)
        self.here = [True, True]

    def setCaptain(self, player):
        if player and player in self.players and self.playersFull:
            index = self.players.index(player)
            # Pick a random team.
            remaining = []
            if not self.red:
                remaining.append('red')
            if not self.blue:
                remaining.append('blue')
            team = random.choice(remaining)

            # Add player to chosen team.
            if team == 'red':
                self.red.append(player)
            elif team == 'blue':
                self.blue.append(player)
            else:
                return False
            self.players[index] = None
            return True
        return False

    def pickPlayer(self, captain, index: int):
        if captain == self.currentCaptainToPickPlayer:

            if index < 0 or index >= len(self) or not self.players[index]:
                return False

            player = self.players[index]
            self.teams[self.currentTeam].append(player)
            self.players[index] = None

            # Check if the next team has any choice of pick, if not fill automatically.
            remainingPicks = PICKMODES[self.pickMode][self.currentPickIndex:self.maxPicks]
            if len(set(remainingPicks)) == 1:
                for i, p in enumerate(self.players):
                    if p:
                        self.teams[self.currentTeam].append(p)
                        self.players[i] = None
            return True

#########################################################################################
# CLASS
#########################################################################################
class GameServer:
    def __init__(self, configFile=DEFAULT_CONFIG_FILE, parent=None, channelId=None):
        # Initialise the class with hardcoded defaults, then parse in JSON config
        self.parent = parent
        self.channelId = channelId
        self.configFile = configFile
        self.configMaps = []

        # POST server, game server and map thumbnails / info:
        self.postServer = DEFAULT_POST_SERVER
        self.authtoken = DEFAULT_POST_TOKEN
        self.thumbnailServer = DEFAULT_THUMBNAIL_SERVER

        # All servers
        if parent is not None and parent.parent is not None and parent.parent.cachedServers is not None and len(parent.parent.cachedServers):
            log.debug('GameServer(): Using cached server list from grandparent PUG instance.')
            self.allServers = parent.parent.cachedServers
        else:
            self.allServers = DEFAULT_SERVER_LIST

        # Chosen game server details
        self.gameServerRef = DEFAULT_GAME_SERVER_REF
        self.gameServerIP = DEFAULT_GAME_SERVER_IP
        self.gameServerPort = DEFAULT_GAME_SERVER_PORT
        self.gameServerName = DEFAULT_GAME_SERVER_NAME
        self.gameServerState = ''
        self.gameServerOnDemand = False
        self.gameServerOnDemandReady = True
        self.gameServerRotation = []

        # Setup details and live score
        self.redPassword = DEFAULT_RED_PASSWORD
        self.bluePassword = DEFAULT_BLUE_PASSWORD
        self.redScore = 0
        self.blueScore = 0
        self.spectatorPassword = DEFAULT_SPECTATOR_PASSWORD
        self.numSpectators = DEFAULT_NUM_SPECTATORS
        self.matchCode = ''
        self.lastMatchCode = ''

        # We keep a track of the server's match status and also if we have used "endMatch" since the last server setup, which
        # can be used to override the updating matchInProgress when a match has been ended since the last server setup.
        # This avoids the need to wait for the last map to complete before the server shows as match finished.
        self.matchInProgress = False
        self.endMatchPerformed = False

        # Store the responses from the setup server.
        self.lastSetupResult = ''
        self.lastCheckJSON = {}
        self.lastSetupJSON = {}
        self.lastEndGameJSON = {}

        self.lastUpdateTime = datetime.now()

        self.loadServerConfig(configFile)
        if self.allServers == DEFAULT_SERVER_LIST:
            self.validateServers()
        parent.parent.cachedServers = self.allServers
        self.updateServerStatus()
        
        # Stream GameSpy Unreal Query data using UDP sockets to send packets to the query port of the target server, then
        # receive back data into an array. Protocol info: https://wiki.beyondunreal.com/Legacy:UT_Server_Query
        # UTA servers extend the protocol server-side to offer Assault-related info and Event streams (e.g. chat)
        self.udpSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpSock.settimeout(3)
        self.utQueryStatsActive = False
        self.utQueryReporterActive = False
        self.utQueryConsoleWatermark = self.format_new_watermark
        self.utQueryData = {}
        self.utQueryEmbedCache = {}

    # Load configuration defaults (some of this will later be superceded by live API data)
    def loadServerConfig(self, configFile):
        with open(configFile) as f:
            info = json.load(f)
            if info:
                if 'setupapi' in info:
                    setupapi = info['setupapi']
                    if 'postserver' in setupapi:
                        self.postServer = setupapi['postserver']
                    if 'authtoken' in setupapi:
                        self.authtoken = info['setupapi']['authtoken']
                else:
                    log.warning('setupapi not found in config file.')
                if 'thumbnailserver' in info:
                    self.thumbnailServer = info['thumbnailserver']
                if 'maplist' in info and len(info['maplist']):
                    log.info(f'Loaded {len(info["maplist"])} maps from config.json')
                    self.configMaps = info['maplist']
                else:
                    self.configMaps = DEFAULT_MAP_LIST
                    log.warning('Maplist not found in config file.')

                # Iterate through local cache of servers, and set the default if present
                if 'serverlist' in info and len(info['serverlist']):
                    for server in info['serverlist']:
                        if 'serverondemand' in server.keys() and server['serverondemand'] is True:
                            ondemand = True
                        else:
                            ondemand = False
                        self.updateServerReference(server['serverref'],server['servername'],'',ondemand)
                        if 'serverdefault' in server.keys():
                            self.gameServerRef = server['serverref']
                else:
                    log.warning('Serverlist not found in config file.')
                if 'serverrotation' in info and len(info['serverrotation']):
                    self.gameServerRotation = []
                    for x in info['serverrotation']:
                        svindex = int(x)-1
                        if svindex >= 0 and svindex < len(self.allServers):
                            self.gameServerRotation.append(int(x))
            else:
                log.error(f'GameServer: Config file could not be loaded: {configFile}')
            f.close()
        return True

    # Update config (currently only maplist is being saved)
    def saveMapConfig(self, configFile, maplist):
        with open(configFile) as f:
            info = json.load(f)
            if len(self.configMaps):
                info['maplist'] = self.configMaps
            if len(maplist):
                info['maplist'] = maplist
            f.close()
        with open(configFile, 'w') as f:
            json.dump(info, f, indent=4)
            f.close()
        return True

    def saveServerConfig(self, configFile):
        with open(configFile) as f:
            info = json.load(f)
            if len(self.allServers):
                info['serverlist'] = []
                for s in self.allServers:
                    # allServers[x][0] = server ref
                    # allServers[x][1] = server name
                    # allServers[x][2] = server url
                    # allServers[x][3] = on-demand server (bool)
                    # allServers[x][4] = last state (e.g. OPEN - PUBLIC, N/A)
                    serverinfo = {'serverref': s[0], 'servername': s[1], 'serverurl': s[2], 'serverondemand': s[3]}
                    if s[0] == self.gameServerRef:
                        serverinfo['serverdefault'] = True
                    info['serverlist'].append(serverinfo)
            if len(self.gameServerRotation):
                info['serverrotation'] = self.gameServerRotation
            f.close()
        with open(configFile, 'w') as f:
            json.dump(info, f, indent=4)
            f.close()
        return True
    
    def utQueryServer(self, queryType):
        if 'ip' not in self.utQueryData:
            self.utQueryData['ip'] = self.gameServerIP
        if 'game_port' not in self.utQueryData:
            self.utQueryData['game_port'] = self.gameServerPort
            self.utQueryData['query_port'] = (self.gameServerPort)+1
        try:
            self.udpSock.sendto(str.encode(f'\\{queryType}\\'),(self.utQueryData['ip'], self.utQueryData['query_port']))
            udpData = []
            while True:
                if queryType == 'consolelog': # Larger buffer required for consolelog
                    udpRcv, _ = self.udpSock.recvfrom(65536)
                else:
                    udpRcv, _ = self.udpSock.recvfrom(4096)
                try:
                    udpData.extend(udpRcv.decode('utf-8','ignore').split('\\')[1:-2]) 
                except UnicodeDecodeError as e:
                    log.error(f'UDP decode error: {e.reason}')
                    log.debug(f'Attempted sending UDP query {queryType} to {self.utQueryData["ip"]}:{self.utQueryData["query_port"]}.')
                    return
                if udpRcv.split(b'\\')[-2] == b'final':
                    break
            parts = zip(udpData[::2], udpData[1::2])
            for part in parts:
                self.utQueryData[part[0]] = part[1]
            self.utQueryData['code'] = 200
            self.utQueryData['lastquery'] = int(time.time())
            self.utQueryData['attempts'] = 0
        except socket.timeout:
            log.error(f'UDP socket timeout when connecting to {self.utQueryData["ip"]}:{self.utQueryData["query_port"]} to perform a query: {queryType}')
            self.utQueryData['status'] = 'Timeout connecting to server.'
            self.utQueryData['code'] = 408
            self.utQueryData['lastquery'] = 0
            if 'attempts' not in self.utQueryData:
                self.utQueryData['attempts'] = 0
            self.utQueryData['attempts'] += 1
            if self.utQueryData['attempts'] >= 30:
                self.utQueryData['status'] = 'Failed to connect to server after 30 attempts.'
                self.utQueryReporterActive = False
                self.utQueryStatsActive = False
        return True

    #########################################################################################
    # Formatted JSON
    #########################################################################################
    @property
    def format_post_header_auth(self):
        fmt = {
                'Content-Type': 'application/json; charset=UTF-8',
                'PugAuth': f'{self.authtoken}',
                'Accept':'*/*',
                'Accept-Encoding':'gzip, deflate, br'
        }
        return fmt

    @property
    def format_post_header_check(self):
        fmt = self.format_post_header_auth
        fmt.update({'Mode': 'check'})
        return fmt
    
    @property
    def format_post_header_list(self):
        fmt = self.format_post_header_auth
        fmt.update({'Mode': 'list'})
        return fmt
    
    @property
    def format_post_header_setup(self):
        fmt = self.format_post_header_auth
        fmt.update({'Mode': 'setup'})
        return fmt

    @property
    def format_post_header_endgame(self):
        fmt = self.format_post_header_auth
        fmt.update({'Mode': 'endgame'})
        return fmt
    
    def format_post_header_control(self, state: str = 'start'):
        fmt = self.format_post_header_auth
        fmt.update({'Mode': f'remote{state}'})
        return fmt

    def format_post_body_serverref(self, serverref: str = ''):
        if len(serverref) == 0:
            serverref = self.gameServerRef
        fmt = {
            'server': serverref
        }
        return fmt

    def format_post_body_setup(self, numPlayers: int, maps, mode: str, startmap: str = ''):
        fmt = {
            'server': self.gameServerRef,
            'authEnabled': True,
            'tiwEnabled': True,
            'matchLength': len(maps),
            'maxPlayers': numPlayers,
            'specLimit': self.numSpectators,
            'redPass': self.redPassword,
            'bluePass': self.bluePassword,
            'specPass': self.spectatorPassword,
            'maplist': maps,
            'gameType': MODE_CONFIG[mode].gameType,
            'mutators': MODE_CONFIG[mode].mutators,
            'friendlyFireScale': MODE_CONFIG[mode].friendlyFireScale,
            'initialWait': 180
        }
        if startmap not in ['',None] and len(startmap) > 0:
            fmt['startMap'] = startmap
        return fmt

    def current_serverrefs(self):
        allServerRefs = []
        for s in self.allServers:
            allServerRefs.append(s[0])
        return allServerRefs

    #########################################################################################
    # Formatted strings
    #########################################################################################
    @property
    def format_current_serveralias(self):
        serverName = self.allServers[self.current_serverrefs().index(self.gameServerRef)][1]
        if self.gameServerIP not in [None, '', '0.0.0.0']:
            serverName = self.gameServerName
        return f'{serverName}'
    
    @property
    def format_showall_servers(self):
        flags = {
            'UK':':flag_gb:',
            'FR':':flag_fr:',
            'NL':':flag_nl:',
            'DE':':flag_de:',
            'SE':':flag_se:',
            'ES':':flag_es:',
            'IT':':flag_it:',
            'DK':':flag_dk:',
            'JP':':flag_jp:',
            'AU':':flag_au:',
            'AT':':flag_at:',
            'BE':':flag_be:',
            'CA':':flag_ca:',
            'PL':':flag_pl:',
            'FI':':flag_fi:',
            'HU':':flag_hu:',
            'NO':':flag_no:',
            'IS':':flag_is:',
            'CN':':flag_cn:',
            'XX':':pirate_flag:',
            'GP':':rainbow_flag:',
            'US':':flag_us:'
        }
        msg = []
        i = 0
        for s in self.allServers:
            i += 1
            servername = f'{s[1]}'
            for flag in flags:
                servername  = re.compile(flag).sub(flags[flag], servername)
            msg.append(f'{i}. {servername} - {s[2]}')
        return '\n'.join(msg)

    @property
    def format_gameServerURL(self):
        return f'unreal://{self.gameServerIP}:{self.gameServerPort}'

    @property
    def format_gameServerURL_red(self):
        return f'{self.format_gameServerURL}?password={self.redPassword}'

    @property
    def format_gameServerURL_blue(self):
        return f'{self.format_gameServerURL}?password={self.bluePassword}'

    @property
    def format_gameServerURL_spectator(self):
        return f'{self.format_gameServerURL}?password={self.spectatorPassword}'

    @property
    def format_gameServerState(self):
        return f'{self.gameServerState}'

    @property
    def format_server_info(self):
        fmt = f'{self.gameServerName} | {self.format_gameServerURL}'
        return fmt

    @property
    def format_red_password(self):
        fmt = f'Red team password: **{self.redPassword}**'
        return fmt

    @property
    def format_blue_password(self):
        fmt = f'Blue team password: **{self.bluePassword}**'
        return fmt

    @property
    def format_spectator_password(self):
        fmt = f'Spectator password: **{self.spectatorPassword}**'
        return fmt

    @property
    def format_game_server(self):
        fmt = f'Server: **{self.format_gameServerURL}**'
        return fmt
    
    @property
    def format_game_server_status(self):
        info = self.getServerStatus(restrict=True, delay=5)
        if not info:
            info = self.lastCheckJSON
        msg = ['```']
        try:
            msg.append('Server: ' + info['serverName'])
            msg.append(self.format_gameServerURL)
            msg.append('Summary: ' + info['serverStatus']['Summary'])
            msg.append('Map: ' + info['serverStatus']['Map'])
            msg.append('Mode: ' + info['serverStatus']['Mode'])
            msg.append('Match Code: ' + info['serverStatus']['MatchCode'])
            msg.append('Players: ' + info['serverStatus']['Players'])
            msg.append('Remaining Time: ' + info['serverStatus']['RemainingTime'])
            msg.append('TournamentMode: ' + info['serverStatus']['TournamentMode'])
            msg.append('Status: ' + info['setupResult'])
        except:
            msg.append('WARNING: Unexpected or incomplete response from server.')
        msg.append('```')
        return '\n'.join(msg)

    @property
    def format_new_watermark(self):
        return int(datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S000'))

    #########################################################################################
    # Functions:
    ######################################################################################### 
    def makePostRequest(self, server: str, headers, json=None):
        if json:
            try:
                r = requests.post(server, headers=headers, json=json)
            except requests.exceptions.RequestException:
                return None
        else:
            try:
                r = requests.post(server, headers=headers)
            except requests.exceptions.RequestException:
                return None
        return r

    def removeServerReference(self, serverref: str):
        if serverref in self.current_serverrefs() and serverref not in [None, '']:
            self.allServers.pop(self.current_serverrefs().index(serverref))
            return True
        return False
    
    def updateServerReference(self, serverref: str, serverdesc: str, serverurl: str = '', serverondemand: bool = False, serverlaststatus: str = ''):
        if serverref in self.current_serverrefs() and serverref not in [None, '']:
            self.allServers.pop(self.current_serverrefs().index(serverref))
        self.allServers.append((serverref, serverdesc, serverurl,serverondemand,serverlaststatus))
        return True
    
    def useServer(self, index: int, autostart: bool = False, byref: str = ''):
        """Sets the active server"""
        serverchanged = False
        log.debug(f'useServer() called for mode {str(self.parent.mode)}, with index: {str(index)}, autostart: {str(autostart)}, byref: {byref}')
        if index >= 0 and index < len(self.allServers):
            # check if current server needs to be shut down first
            if self.gameServerOnDemand:
                self.controlOnDemandServer('stop', self.gameServerRef)
            # update to new server
            self.gameServerRef = self.allServers[index][0]
            self.gameServerOnDemand = self.allServers[index][3]
            if autostart and self.gameServerOnDemand:
                self.controlOnDemandServer('start')
            else:
                self.updateServerStatus()
            serverchanged = True
        elif len(byref) > 0:
            for s in self.allServers:
                if s[0] == byref:
                    self.gameServerRef = s[0]
                    self.gameServerOnDemand = s[3]
                    self.updateServerStatus()
                    serverchanged = True
        if serverchanged:
            self.saveServerConfig(self.configFile)
            self.utQueryData = {}
            return True
        return False

    def generatePasswords(self):
        """Generates random passwords for red and blue teams."""
        # Spectator password is not changed, think keeping it fixed is fine.
        self.redPassword = RED_PASSWORD_PREFIX + str(random.randint(0, 999))
        self.bluePassword = BLUE_PASSWORD_PREFIX + str(random.randint(0, 999))

    def getServerList(self, restrict: bool = False, delay: int = 0, listall: bool = True):
        if restrict and (datetime.now() - self.lastUpdateTime).total_seconds() < delay:
            # 5 second delay between requests when restricted.
            return None
        log.debug('Sending API request, fetching server list...')
        if listall:
            r = self.makePostRequest(self.postServer, self.format_post_header_list)
        else:
            body = self.format_post_body_serverref()
            r = self.makePostRequest(self.postServer, self.format_post_header_list, body)
       
        self.lastUpdateTime = datetime.now()
        if(r):            
            try:
                validatedJSON = r.json()
                log.debug('API response validated.')
                return validatedJSON
            except:
                log.error(f'Invalid JSON returned from server, URL: {r.url} HTTP response: {r.status_code}; content:{r.content}')
                return None
        else:
            return None

    def validateServers(self):
        if len(self.allServers):
            info = self.getServerList()
            if info and len(info):
                # firstly, determine if the primary server is online and responding, then drop the local list
                serverDefaultPresent = False
                for svc in info:
                    if svc['serverDefault'] is True and (svc['cloudManaged'] is True or svc['serverStatus']['Summary'] not in [None,'','N/A','N/AN/A']):
                        # If for whatever reason the default server isn't working, then stick to local list for now.
                        serverDefaultPresent = True
                        break

                if serverDefaultPresent:
                    # Default is present and working, re-iterate through list and populate local var
                    # Populate only those either online now, or that are "cloudManaged" on-demand servers
                    self.allServers = []
                    for sv in info:
                        if sv['cloudManaged'] is True or sv['serverStatus']['Summary'] not in [None, '', 'N/A', 'N/AN/A']:
                            self.updateServerReference(sv['serverRef'], sv['serverName'],f'unreal://{sv["serverAddr"]}:{sv["serverPort"]}', sv['cloudManaged'], sv['serverStatus']['Summary'])

                # Write the server config:
                self.saveServerConfig(self.configFile)
                return True
            else:
                return True # query failed, fall back to local json config
            return True
        return False

    def getServerStatus(self, restrict: bool = False, delay: int = 0):
        if restrict and (datetime.now() - self.lastUpdateTime).total_seconds() < delay:
            # 5 second delay between requests when restricted.
            return None
        body = self.format_post_body_serverref()
        log.debug(f'Posting "Check" to API {self.postServer} - {body}')
        r = self.makePostRequest(self.postServer, self.format_post_header_check, body)
        log.debug(f'Received data from API - Status: {r.status_code}; Content-Length: {r.headers["content-length"]}')
        self.lastUpdateTime = datetime.now()
        if(r):
            return r.json()
        else:
            return None

    def updateServerStatus(self, ignorematchStarted: bool = False):
        log.debug(f'updateServerStatus({ignorematchStarted}) - running getServerStatus for {self.parent.mode}')
        info = self.getServerStatus()
        log.debug(f'updateServerStatus({ignorematchStarted}) -  info fetched for {self.parent.mode}')
        log.debug(f'- serverStatus: {info["serverStatus"]}')
        log.debug(f'- setupResult: {info["setupResult"]}')
        if info:
            self.gameServerName = info['serverName']
            self.gameServerIP = info['serverAddr']
            self.gameServerPort = info['serverPort']
            self.gameServerOnDemand = info['cloudManaged']
            self.gameServerOnDemandReady = True
            self.gameServerState = info['serverStatus']['Summary']
            self.matchInProgress = False
            if self.gameServerState.startswith('OPEN - PUBLIC') is not True and self.gameServerState.startswith('LOCKED - PRIVATE') is not True:
                self.redScore = info['serverStatus']['ScoreRed']
                self.blueScore = info['serverStatus']['ScoreBlue']
            if not self.endMatchPerformed and ignorematchStarted is False:
                self.matchInProgress = info['matchStarted']
                if (self.matchCode in [None,''] or self.matchCode[:4] == 'temp') and info['serverStatus']['MatchCode'] not in [None,''] and len(info['serverStatus']['MatchCode']):
                    self.matchCode = info['serverStatus']['MatchCode']
            self.lastSetupResult = info['setupResult']
            self.lastCheckJSON = info
            return True
        self.lastSetupResult = 'Failed'
        return False
    
    def controlOnDemandServer(self, state: str = 'start', serverref: str = ''):
        if len(serverref) == 0:
            serverref = self.gameServerRef
        log.debug(f'Running controlOnDemandServer-{state} for {serverref}...')
        if state not in [None, 'stop','halt','shutdown']:
            if not self.updateServerStatus(True): # or self.matchInProgress:
                return None

        headers = self.format_post_header_control(state)
        body = self.format_post_body_serverref(serverref)
        log.debug(f'Posting "Remote{state}" to API {self.postServer} - {body}')
        r = self.makePostRequest(self.postServer, headers, body)
        log.debug(f'Received data from API - Status: {r.status_code}; Content-Length: {r.headers["content-length"]}')
        if(r):
            log.debug(f'controlOnDemandServer-{state} returned JSON info...')
            info = r.json()
            return info
        else:
            log.error(f'controlOnDemandServer-{state} failed.')
        return None

    def stopOnDemandServer(self, index: int):
        log.debug('Running stopOnDemandServer...')
        if index >= 0 and index < len(self.allServers):
            if self.allServers[index][3] is True:
                self.controlOnDemandServer('stop',self.allServers[index][0])
                log.debug('stopOnDemandServer - Control command issued.')
                return True
        log.debug('stopOnDemandServer - Invalid server selected.')
        return False

    def setupMatch(self, numPlayers, maps, mode, startmap=''):
        if self.matchInProgress:
            return False

        # Start the looped task which checks whether the server is ready
        if self.gameServerOnDemand:
            self.gameServerOnDemandReady = False
            # TODO - Unblock thread and move setup and checks to background
            #      - Lots of re-work required for this

            # log.debug('Starting Server state checker...')
            # self.updateOnDemandServerState.start(ctx, log, False)
            # log.debug('Server state checker started.')
        else:
            self.gameServerOnDemandReady = True

        i = 0
        while self.gameServerOnDemandReady is False:
            log.debug('Waiting for gameServerOnDemandReady...')
            if i < 5:
                self.controlOnDemandServer('start')
            else:
                if self.parent.gameServer.updateServerStatus():
                    self.gameServerOnDemandReady=True
                else:
                    time.sleep(5)
            i += 1
            if i > 12:
               # Stop trying after a minute
               self.gameServerOnDemandReady=True # fail the setup instead, and allow for manual retry.

        if not self.gameServerOnDemandReady:
            return False

        self.blueScore = 0
        self.redScore = 0
        self.matchCode = ''
        self.generatePasswords()
        headers = self.format_post_header_setup
        body = self.format_post_body_setup(numPlayers, maps, mode, startmap)

        r = self.makePostRequest(self.postServer, headers, body)
        if(r):
            info = r.json()
            self.lastSetupResult = info['setupResult']
            self.matchInProgress = info['matchStarted']
            self.lastSetupJSON = info
            self.endMatchPerformed = False
            self.matchCode = info['setupConfig']['matchCode']

            # Get passwords from the server
            self.redPassword = info['setupConfig']['redPass']
            self.bluePassword = info['setupConfig']['bluePass']
            self.spectatorPassword = info['setupConfig']['specPass']
            
            return self.lastSetupResult == 'Completed'

        self.matchInProgress = False
        self.lastSetupResult = 'Failed'
        return False

    def endMatch(self, viaReset: bool = False):
        # returns server back to public
        if not self.updateServerStatus():
            return False
        # Cache last scores from server status
        log.debug(f'endMatch (viaReset={viaReset}): Ended = {self.endMatchPerformed}. matchCode = {self.matchCode}, redScore = {self.redScore} - blueScore = {self.blueScore}')
        if self.endMatchPerformed is True:
            if self.parent.storeLastPug(f'**Score:** Red {self.redScore} - {self.blueScore} Blue', self.redScore, self.blueScore, self.lastMatchCode, viaReset):
                log.info('Pug reset; last scores appended successfully.')
            else:
                log.info('Pug reset; last scores did not append successfully.')
        # Tear down match
        body = self.format_post_body_serverref()
        r = self.makePostRequest(self.postServer, self.format_post_header_endgame, body)
        if(r):
            info = r.json()
            self.lastSetupResult = info['setupResult']
            self.lastEndGameJSON = info
            if self.lastSetupResult == 'Completed':
                self.matchInProgress = False
                self.endMatchPerformed = True
                self.lastMatchCode = f'{self.matchCode}'
                self.matchCode = ''
                if self.parent.ranked and self.parent.parent.ratingsLock:
                    self.parent.parent.ratingsLock = False
                try:
                    self.parent.parent.popMultiInstancePlayers(self.parent.channelId,self.parent.mode)
                except Exception as e:
                    log.error(f'endMatch({viaReset}) - Error popping multi-instance players: {e}')
                return True

            return False
        self.lastSetupResult = 'Failed'
        return False

    def processMatchFinished(self):
        if self.lastSetupResult == 'Failed' or not self.updateServerStatus():
            return False

        if not self.matchInProgress and self.lastSetupResult == 'Match Finished':
            return self.endMatch()
        return False

    def waitUntilServerStarted(self):

        return True

    def checkServerRotation(self):
        # An imprecise science here, as where there is a mismatch between number of rotation items and weeks in a year,
        # the pattern may break when crossing over between week 52 and week 1 at new year.
        if len(self.gameServerRotation) > 0:
            # Extended the input a little, rather than simply week number, it's a combination of yearweek (e.g., 202201 - 202252),
            # which works better with smaller rotation pools
            newServer = int(self.gameServerRotation[int(f'{datetime.now().year}{datetime.now().isocalendar()[1]:02}')%len(self.gameServerRotation)])-1
            if self.gameServerRef != self.allServers[newServer][0]:
                log.debug(f'checkServerRotation - Updating current server to: {self.allServers[newServer][1]}')
                self.useServer(newServer)
        return True

    #########################################################################################
    # Loops
    #########################################################################################
    @tasks.loop(seconds=15.0, count=8)
    async def updateOnDemandServerState(self, ctx):
        log.debug('Checking on-demand server state...')
        if self.parent.gameServer.updateServerStatus():
            serverOnline=True
        else:
            serverOnline=False

        if serverOnline:
            log.info('Server online.')
            self.gameServerOnDemandReady = True
            await ctx.send(f'{self.parent.gameServer.gameServerName} is ready for action.')
            self.updateOnDemandServerState.cancel()
        else:
            log.warning('Server not yet online.')
    
    @updateOnDemandServerState.after_loop
    async def on_updateOnDemandServerState_cancel(self):
        # Assume after loop completion that the servers is ready, and fall back to pug setup to confirm
        self.gameServerOnDemandReady = True

#########################################################################################
# CLASS
#########################################################################################
class AssaultPug(PugTeams):
    """Represents a Pug of 2 teams (to be selected), a set of maps to be played and a server to play on."""
    def __init__(self, numPlayers, numMaps, pickModeTeams, pickModeMaps, configFile=DEFAULT_CONFIG_FILE, ratingsFile=DEFAULT_RATING_FILE, modeLimit=0, mode=MODE_DEFAULT, parent=None, channelId=None):
        super().__init__(maxPlayers=numPlayers, pickMode=pickModeTeams, ranked=False, roleRequired='') # Send to PugTeams()
        self.parent = parent
        self.mode = mode
        self.name = MODE_CONFIG[self.mode].name
        self.modeLimit = modeLimit
        self.lastPlayedMode = 'stdAS'
        self.matchReportPending = False
        self.desc = self.name + ': ' + self.mode + ' PUG'
        self.servers = [GameServer(configFile=configFile, parent=self)]
        self.serverIndex = 0
        self.channelId = channelId
        self.ranked = False
        self.redPower = 0
        self.bluePower = 0
        self.ratings = parent.allRatings if parent else None
        self.ratingsFile = ratingsFile
        log.debug(f'AssaultPug() instance created with ratings file: {self.ratingsFile}, mode: {self.mode}')
        self.ratingsSyncAPI = parent.ratingsSyncAPI if parent else {'matchDataURL':'','ratingsDataURL':'','playerDataURL':'','apiKey':''}

        self.maps = PugMaps(numMaps, pickModeMaps, self.ranked, self.servers[self.serverIndex].configMaps)
        self.roleRequired = None
        self.lastPug = {}
        self.lastPugStr = 'No last pug info available.' # deprecated
        self.lastPugTimeStarted = None # deprecated
        self.pugLocked = False
        self.pugTempLocked = 0 # 0 = not locked, 1 = temp locked, 2 = long locked (e.g. server/players busy in another match)

        # Bit of a hack to get around the problem of a match being in progress when this is initialised.
        # Will improve this later.
        if self.gameServer.lastSetupResult == 'Match In Progress':
            self.pugLocked = True

    #########################################################################################
    # Properties:
    #########################################################################################
    @property
    def playersReady(self):
        if self.playersFull:
            return True
        return False

    @property
    def captainsReady(self):
        if self.captainsFull:
            return True
        return False

    @property
    def teamsReady(self):
        if self.captainsFull and self.teamsFull:
            return True
        return False

    @property
    def currentCaptainToPickMap(self):
        if self.captainsFull and not self.maps.mapsFull:
            return self.teams[self.maps.currentTeamToPickMap].captain
        else:
            return None

    @property
    def mapsReady(self):
        if self.maps.mapsFull:
            return True
        return False

    @property
    def matchReady(self):
        if self.playersReady and self.teamsReady and self.mapsReady:
            return True
        return False

    @property
    def gameServer(self):
        if len(self.servers):
            return self.servers[self.serverIndex]
        else:
            return None

    #########################################################################################
    # Formatted strings:
    #########################################################################################
    def format_players(self, players, number: bool = False, mention: bool = False):
        def name(p):
            isCap = ''
            if self.ranked and self.ratings not in [None, '']:
                if 'capMode' in self.ratings and int(self.ratings['capMode']) > 0:
                    isCap = ' ('+CAPSIGN+')' if (p == self.red.captain or p == self.blue.captain) else ''
            return p.mention+isCap if mention else display_name(p)+isCap
        if self.ranked and self.ratings not in [None, '']:
            numberedPlayers = ((i, name(p)) for i, p in enumerate(list(set(players)), 1) if p)
        else:
            numberedPlayers = ((i, name(p)) for i, p in enumerate(players, 1) if p)
        fmt = '**{0})** {1}' if number else '{1}'
        return PLASEP.join(f'**{i})** {name}' if number else f'{name}' for i, name in numberedPlayers)

    def format_all_players(self, number: bool = False, mention: bool = False):
        return self.format_players(self.all, number=number, mention=mention)

    def format_remaining_players(self, number: bool = False, mention: bool = False):
        return self.format_players(self.players, number=number, mention=mention)

    def format_queued_players(self, number: bool = False, mention: bool = False):
        return self.format_players(self.queuedPlayers, number=number, mention=mention)

    def format_red_players(self, number: bool = False, mention: bool = False):
        return self.format_players(self.red, number=number, mention=mention)

    def format_blue_players(self, number: bool = False, mention: bool = False):
        return self.format_players(self.blue, number=number, mention=mention)

    def format_teams(self, number: bool = False, mention: bool = False, indent: bool = False):
        red = self.format_red_players(number=number, mention=mention)
        blue = self.format_blue_players(number=number, mention=mention)
        if indent:
            return f'> **Red Team:** {red}\n> **Blue Team:** {blue}'
        return f'**Red Team:** {red}\n**Blue Team:** {blue}'

    @property
    def format_pug_short(self):
        return f'**__{self.desc} [{len(self)}/{self.maxPlayers}] \|\| {self.gameServer.gameServerName} \|\| {self.maps.maxMaps} maps__**'

    def format_pug(self, number=True, mention=False):
        return f'**__{self.desc} [{len(self)}/{self.maxPlayers}] \|\| {self.gameServer.gameServerName} \|\| {self.maps.maxMaps} maps:__**\n{self.format_all_players(number=number, mention=mention)}'

    @property
    def format_match_is_ready(self):
        fmt = ['Match is ready:']
        fmt.append(f'{self.format_teams(mention=True, indent=True)}')
        fmt.append(f'> Maps ({self.maps.maxMaps}):\n> {self.maps.format_current_maplist}')
        fmt.append(f'> {self.gameServer.format_game_server}')
        fmt.append(f'> {self.gameServer.format_spectator_password}')
        return '\n'.join(fmt)

    @property
    def format_match_in_progress(self):
        if self.pugLocked:
            if not self.matchReady:
                # Handles the case when the bot has been restarted so doesn't have previous info.
                # Could improve this in future by caching the state to disk when shutting down and loading back in on restart.
                return 'Match is in progress, but do not have previous pug info. Please use **!serverstatus** to monitor this match'

            fmt = [f'Match in progress ({getDuration(self.lastPugTimeStarted, datetime.now())} ago):']
            fmt.append(self.format_teams(mention=False))
            if self.ranked:
                fmt.append(f'Red RP: {str(self.redPower)}; Blue RP: {str(self.bluePower)}')
            fmt.append(f'Maps ({self.maps.maxMaps}): {self.maps.format_current_maplist}')
            fmt.append('Mode: ' + self.mode+' @ '+self.gameServer.format_game_server)
            fmt.append(self.gameServer.format_spectator_password)
            if len(self.queuedPlayers):
                fmt.append(f'Queued players for next pug: {self.format_queued_players(mention=False)}')
            return '\n'.join(fmt)
        return None

    @property
    def format_last_pug(self):
        if self.lastPugTimeStarted and '{}' in self.lastPugStr:
            return self.lastPugStr.replace('{}', getDuration(self.lastPugTimeStarted, datetime.now()))
        else:
            return 'No last pug info available.'

    @property
    def format_last_pug_for_embed(self):
        if self.lastPug not in [None,'',{}]:
            ago = ''
            if 'timestarted' in self.lastPug:
                ago = getDuration(datetime.fromisoformat(self.lastPug['timestarted']), datetime.now())
            if 'players' in self.lastPug:
                playerList = f':red_circle: {self.lastPug["teamred"]}\n:blue_circle: {self.lastPug["teamblue"]}'
                maplist = self.lastPug['maplist']
                server = f'{self.lastPug["servername"]} (`{self.lastPug["serveraddr"]}`)'
                completed = ' (incomplete)' if not(self.lastPug['completed']) else ''
                stats = ''
                if 'matchcode' in self.lastPug and not(str(self.lastPug['matchcode']).startswith('temp')):
                    matchcode = self.lastPug['matchcode']
                    stats = f' ([stats]({DEFAULT_STATS_MATCH_URL}{matchcode}))'
                pugstr = f'Played {ago}{completed}{stats}:\nBest of `{self.lastPug["length"]}` maps. `{self.lastPug["players"]}/{self.lastPug["maxplayers"]}` players signed:\n{playerList}\n{maplist}\n{server}'
                pass
            elif 'pugstr' in self.lastPug:
                pugstr = self.format_last_pug
        else:
            pugstr = self.format_last_pug
        return pugstr

    @property
    def format_list_servers(self):
        indexedServers = ((i,s) for i,s in enumerate(self.servers, 1) if s)
        fmt = []
        for x in indexedServers:
            fmt.append(f'**{x[0]})** {x[1].format_server_info}')

        return '\n'.join(fmt)

    #########################################################################################
    # Functions:
    #########################################################################################
    def removePlayerFromPug(self, player):
        if player in self.queuedPlayers:
            if player.id in self.playerFlags:
                del self.playerFlags[player.id]
            self.queuedPlayers.remove(player)
            return True
        if self.removePugTeamPlayer(player):
            # Reset the maps too. If maps have already been picked, removing a player will mean teams and maps must be re-picked.
            self.maps.resetMaps()
            return True
        else:
            return False

    def pickMap(self, captain, index: int):
        if captain != self.currentCaptainToPickMap:
            return False
        return self.maps.addMap(index)

    def setupPug(self):
        if not self.pugLocked and self.matchReady:
            # Check if server is already locked by another instance (any mode in any channel)
            serverRef = self.gameServer.gameServerRef
            if self.parent and serverRef in self.parent.serverLocks:
                existing_lock = self.parent.serverLocks[serverRef]
                log.debug(f'setupPug() - Server {serverRef} is already locked by channel {existing_lock[0]} mode {existing_lock[1]}')
                return False, 'locked'
            
            # Try to set up 5 times with a 5s delay between attempts.
            result = False
            self.pugTempLocked = 1
            startMap = ''
            if self.maps.startMap not in ['',None] and self.maps.maps[-1] != self.maps.startMap:
                startMap = self.maps.startMap # Only send this if it's not the last map in the list, otherwise it'll take longer to set up
            for x in range(0, 5):
                result = self.gameServer.setupMatch(self.maxPlayers, self.maps.maps, self.mode, startMap)
                log.debug(f'Setup attempt {x+1}/5: Result returned: {result}')
                if not result:
                    time.sleep(5)
                else:
                    self.pugLocked = True
                    # Lock the server to prevent other instances from using it with mode-aware tuple
                    if self.parent:
                        lock_key = (self.channelId, self.mode)
                        self.parent.serverLocks[serverRef] = lock_key
                        log.debug(f'setupPug() - Locked server {serverRef} for channel {self.channelId} mode {self.mode}')
                    if self.gameServer.matchCode in [None,'']:
                        # Generate a temporary match code which can be updated later
                        self.gameServer.matchCode = f'temp-{datetime.now().strftime("%Y%m%d%H%M%S")}'
                    self.storeLastPug(matchCode=self.gameServer.matchCode)
                    return True, 'ok'
            self.pugTempLocked = 0
        return False, 'failed'

    def storeLastPug(self, appendstr: str = '', redScore: int = 0, blueScore: int = 0, matchCode: str = '', viaReset: bool = False):
        if self.matchReady:
            fmt = []
            if matchCode in [None,'','N/A']:
                if self.gameServer.matchCode in [None,'','N/A']:
                    self.gameServer.matchCode = f'temp-{datetime.now().strftime("%Y%m%d%H%M%S")}'
                matchCode = self.gameServer.matchCode

            # Legacy last pug storage (deprecated)
            fmt.append(f'Last **{self.desc}** ({{}} ago)')
            fmt.append(self.format_teams())
            if self.ranked:
                fmt.append(f'Red RP: {str(self.redPower)}; Blue RP: {str(self.bluePower)}')
            fmt.append(f'Maps ({self.maps.maxMaps}):\n{self.maps.format_current_maplist}')
            self.lastPugStr = '\n'.join(fmt)
            self.lastPugTimeStarted = datetime.now()
            self.lastPlayedMode = self.mode

            # New last pug storage
            self.lastPug = {
                'timestarted' : datetime.now().isoformat(),
                'timeended' : '',
                'pugstr' : '\n'.join(fmt), # legacy
                'length' : self.maps.maxMaps,
                'players' : self.numPlayers,
                'maxplayers' : self.maxPlayers,
                'teamred' : self.format_red_players(),
                'teamblue' : self.format_blue_players(),
                'scorered' : redScore,
                'scoreblue': blueScore,
                'maplist' : f'{self.maps.format_current_maplist}',
                'rankedinfo' : f'Red RP: {str(self.redPower)}; Blue RP: {str(self.bluePower)}' if self.ranked else '',
                'servername': self.gameServer.format_current_serveralias,
                'serveraddr' : self.gameServer.format_gameServerURL,
                'matchcode' : matchCode,
                'completed': False
            }

            if self.ranked:
                log.debug(f'storeLastPug(viaReset={viaReset}) - Calling via matchReady - storeRankedPug({self.mode},{matchCode},{redScore},{blueScore},{self.lastPugTimeStarted})')
                if self.storeRankedPug(self.mode, matchCode, redScore, blueScore, self.lastPugTimeStarted.isoformat(), False):
                    log.debug('storeRankedPug() - Stored game successfully via storeLastPug matchReady')
                else:
                    log.debug('storeRankedPug() - Failed to store game successfully via storeLastPug matchReady')
            return True
        elif len(appendstr):
            # Legacy storage (deprecated, but function will still use appendstr to determine updates)
            fmt = []
            fmt.append(self.lastPugStr)
            fmt.append(appendstr)
            self.lastPugStr = '\n'.join(fmt)
            # New last pug schema:
            self.lastPug['scorered'] = redScore
            self.lastPug['scoreblue'] = blueScore
            self.lastPug['timeended'] = datetime.now().isoformat()
            self.lastPug['completed'] = not(viaReset)
            if self.ranked:
                log.debug(f'storeLastPug(viaReset={viaReset}) - Calling storeRankedPug({self.mode},{matchCode},{redScore},{blueScore},{self.lastPugTimeStarted})')
                if viaReset: # do not track as a ranked game if reset before completion
                    if self.storeRankedPug(self.mode, matchCode, redScore, blueScore, self.lastPugTimeStarted.isoformat(), False):
                        log.debug('storeRankedPug() - Stored game successfully via storeLastPug (update, via reset)')
                    else:
                        log.debug('storeRankedPug() - Failed to store game successfully via storeLastPug (update, via reset)')
                else:
                    if self.storeRankedPug(self.mode, matchCode, redScore, blueScore, self.lastPugTimeStarted.isoformat(), self.gameServer.endMatchPerformed):
                        log.debug('storeRankedPug() - Stored game successfully via storeLastPug (update, natural conclusion)')
                        self.matchReportPending = True
                    else:
                        log.debug('storeRankedPug() - Failed to store game successfully via storeLastPug (update, natural conclusion)')
                    self.convertQueuedPlayers()
            elif viaReset == False:
                self.convertQueuedPlayers()
            return True
        return False

    def resetPug(self, manualReset = False):
        self.pugTempLocked = 1
        if manualReset and self.pugLocked and self.ranked and len(self.maps):
            self.maps.adjustRankedMapDesirability()
            self.ratings['maps']['maplist'] = self.maps.mapListWeighting
            self.savePugRatings(self.ratingsFile)
        self.maps.resetMaps()
        self.fullPugTeamReset(manualReset)
        self.redPower = 0
        self.bluePower = 0
        if self.pugLocked or (self.gameServer and self.gameServer.matchInProgress):
        # Is this a good idea? Might get abused.
            self.gameServer.endMatch(manualReset)
        self.gameServer.utQueryReporterActive = False
        self.gameServer.utQueryStatsActive = False
        self.pugTempLocked = 0
        self.pugLocked = False
        if self.ranked:
            self.setRankedMode(self.ranked, True)
        
        # Release server lock
        serverRef = self.gameServer.gameServerRef
        if self.parent and serverRef in self.parent.serverLocks:
            # Find the mode for this pug instance
            mode = self._getModeForPug()
            lock_key = (self.channelId, mode)
            # Check if this is our lock
            if self.parent.serverLocks[serverRef] == lock_key:
                del self.parent.serverLocks[serverRef]
                log.debug(f'resetPug() - Released server lock for channel {self.channelId} mode {mode} server {serverRef}')
        
        # Restore players from temporary queues in other instances
        if self.parent:
            self.parent.restoreMultiInstancePlayers(self)
        
        return True
    
    def setRankedMode(self, rankedMode: bool, skipResets: bool = False):
        # Perform any checks needed when switching between ranked and non-ranked modes
        self.maps.rankedMode = self.ranked = False
        self.maps.filteredMapsList = self.maps.availableMapsList
        self.maps.startMapFromPick = 0
        self.maps.startMap = ''
        self.ratings = None
        self.roleRequired = None
        if rankedMode == False and self.ratingsFile != '':
            log.debug(f'setRankedMode({rankedMode}) - Calling savePugRatings({self.ratingsFile})')
            self.savePugRatings(self.ratingsFile)
        if skipResets != True:
            log.debug(f'setRankedMode({rankedMode}) - Calling softPugTeamReset()')
            self.softPugTeamReset() # clear any caps / picks
            log.debug(f'setRankedMode({rankedMode}) - Calling configurePlayersRankedMode({self.ranked},{self.roleRequired})')
            self.configurePlayersRankedMode(self.ranked, self.roleRequired) # reconfigure teams and players
            log.debug(f'setRankedMode({rankedMode}) - Calling maps.resetMaps()')
            self.maps.resetMaps() # reset map selection
        if rankedMode and self.ratingsFile != '':
            log.debug(f'setRankedMode({rankedMode}) - Calling loadPugRatings({self.ratingsFile})')
            if self.loadPugRatings(self.ratingsFile):
                log.debug(f'setRankedMode({rankedMode}) - Checking self.ratings...')
                if self.ratings is None or self.ratings == {}:
                    log.debug(f'setRankedMode({rankedMode}) - Checked self.ratings - requires initialising.')
                    newMode =  {
                        'mode': self.mode,
                        'maps':{},
                        'eligibility':'',
                        'registrations':[],
                        'ratings':[],
                        'lastsync':'',
                        'fixedpicklimit':0,
                        'capMode':2,
                        'capWindow':0,
                        'capRole':'Ranked Captains',
                        'games':[],
                        'scoring':{
                            'mode': 'permap',
                            'teamWin': 3,
                            'teamLose': -3,
                            'capWin': 1,
                            'capLose': 0,
                            'volCapWin': 0,
                            'volCapLose': 0
                        },
                        'lastupdated':'',
                        'startmapfrompick':'',
                        'randomorder':''
                    }
                    self.parent.allRatings['rankedgames'].append(newMode)
                    self.savePugRatings(ratingsFile=self.ratingsFile, ratingsUpdates=self.parent.allRatings)
                    self.loadPugRatings(self.ratingsFile)
                if self.ratings is not None:
                    log.debug(f'setRankedMode({rankedMode}) - self.ratings present.')
                    # Set any missing defaults
                    if 'capMode' not in self.ratings:
                        self.ratings['capMode'] = 0
                    if 'capRole' not in self.ratings:
                        self.ratings['capRole'] = ''
                    if 'capWindow' not in self.ratings:
                        self.ratings['capWindow'] = 0
                    if 'games' not in self.ratings:
                        self.ratings['games'] = []
                    # Determine if all players have a ratings entry and remove those who don't or are not eligible
                    if self.ratings['eligibility'] is not None and self.ratings['eligibility'] != '':
                        self.roleRequired = self.ratings['eligibility'] # Requires a specific discord role
                    log.debug(f'setRankedMode({rankedMode}) - Calling checkRankedPlayersEligibility({self.players})')
                    if self.checkRankedPlayersEligibility(self.players):
                        self.maps.rankedMode = self.ranked = True
                        log.debug(f'setRankedMode({rankedMode}) - Ranked mode setup, calling configurePlayersRankedMode({self.ranked},{self.roleRequired},(JSON))')
                        self.configurePlayersRankedMode(self.ranked, self.roleRequired, self.ratings)
                        if 'maps' in self.ratings:
                            if 'randomorder' in self.ratings['maps']:
                                self.maps.autoPickShuffled = self.ratings['maps']['randomorder']
                            if 'cooldownpool' in self.ratings['maps']:
                                self.maps.cooldownMaps = self.ratings['maps']['cooldownpool']
                            if 'cooldowncount' in self.ratings['maps']:
                                self.maps.cooldownCount = max(0, min(int(self.ratings['maps']['cooldowncount']), 5))
                            if 'fixedpicklimit' in self.ratings['maps'] and self.ratings['maps']['fixedpicklimit'] > 0:
                                self.maps.setMaxMaps(self.ratings['maps']['fixedpicklimit'])
                            if 'startmapfrompick' in self.ratings['maps']:
                                self.maps.startMapFromPick = max(0, int(self.ratings['maps']['startmapfrompick']))
                            if 'maplist' in self.ratings['maps'] and self.ratings['maps']['maplist'] is not None:
                                # build a filtered map list array from this data
                                self.maps.filteredMapsList = []
                                for x in self.ratings['maps']['maplist']:
                                    self.maps.filteredMapsList.append(x['map'])
                                self.maps.filteredMapsList = list(set(self.maps.filteredMapsList))
                                self.maps.mapListWeighting = self.ratings['maps']['maplist']
                else:
                    log.debug(f'setRankedMode({rankedMode}) - Failed to establish ratings data for given mode.')
                    return False
        return self.ranked
    
    def setMode(self, requestedMode: str, ignoreLimits: bool = False):
        # Dictionaries are case sensitive, so we'll do a map first to test case-insensitive input, then find the actual key after
        if requestedMode.upper() in map(str.upper, MODE_CONFIG):
            ## Iterate through the keys to find the actual case-insensitive mode
            requestedMode = next((key for key, _ in MODE_CONFIG.items() if key.upper()==requestedMode.upper()), None)
            if (self.modeLimit > 0 and MODE_CONFIG[requestedMode].modeGroup not in [0, self.modeLimit]) and ignoreLimits == False:
                return False, 'Mode limitations are in effect in this channel. Please select from a list of valid modes.'
            ## ProAS and iAS are played with a different maximum number of players.
            ## Can't change mode from std to pro/ias if more than the maximum number of players allowed for these modes are signed.
            if len(self.players) > MODE_CONFIG[requestedMode].maxPlayers:
                return False, str(MODE_CONFIG[requestedMode].maxPlayers) + ' or fewer players must be signed for a switch to ' + requestedMode
            else:
                ## If max players is more than mode max and there aren't more than mode max players signed, automatically reduce max players to mode max.
                if self.maxPlayers > MODE_CONFIG[requestedMode].maxPlayers:
                    self.setMaxPlayers(MODE_CONFIG[requestedMode].maxPlayers)
                self.mode = requestedMode
                self.name = MODE_CONFIG[requestedMode].name
                additionalInfo = ''
                if MODE_CONFIG[requestedMode].isRanked:
                    log.debug(f'Setting up ranked mode - {requestedMode}')
                    if self.setRankedMode(MODE_CONFIG[requestedMode].isRanked, False):
                        additionalInfo = ' (ranked, best of '+str(self.maps.maxMaps)+' maps)'
                    else:
                        log.debug(f'setRankedMode({MODE_CONFIG[requestedMode].isRanked}) failed')
                        requestedMode = None
                else:
                    self.setRankedMode(False, False)
                self.desc = self.name + ' (' + self.mode + ') PUG'
                if (self.mode == requestedMode):
                    return True, 'Pug mode changed to: **' + self.mode + '**' + additionalInfo
                else:
                    return False, 'Could not synchronise ratings, or not all players are eligible for ranked mode. Pug mode reverted to: **' + self.mode + '**'
        else:
            outStr = ['Mode not recognised. Valid modes are:']
            for k in MODE_CONFIG:
                outStr.append(PLASEP + '**' + k + '**')
            outStr.append(PLASEP)
            return False, ' '.join(outStr)

    def loadPugRatings(self, ratingsFile, returnDataOnly: bool = False):
        """Loads the ranked game ratings data from the JSON configuration file"""
        return self.parent.loadPugRatings(ratingsFile, returnDataOnly)
        # Legacy code - now handled by parent
        self.ratings = None # save before load?
        log.debug(f'loadPugRatings({ratingsFile}) started')
        with open(ratingsFile, 'r') as f:
            log.debug(f'loadPugRatings({ratingsFile}) json.load enter')
            try:
                ratingsData = json.load(f)
            except:
                ratingsData = None
            log.debug(f'loadPugRatings({ratingsFile}) json.load finished')
            if ratingsData:
                if returnDataOnly: # For in-line updates
                    return ratingsData
                if 'syncapi' in ratingsData:
                    self.ratingsSyncAPI = ratingsData['syncapi']
                if 'rankedgames' in ratingsData:
                    # Find the mode and specific ratings data
                    for gamedata in ratingsData['rankedgames']:
                        if str(gamedata['mode']).upper() == self.mode.upper():
                            self.ratings = gamedata
                            log.debug(f'loadPugRatings({ratingsFile}) stored ratings data for {self.mode}')
                            return True
                else:
                    # Generate an empty ranked schema with the default mode
                    rkData = {
                        'syncapi': self.ratingsSyncAPI,
                        'rankedgames': [
                            {
                                'mode': MODE_RANKED_DEFAULT,
                                'maps':{},
                                'eligibility':'',
                                'registrations':[],
                                'ratings':[],
                                'lastsync':''
                            }
                        ]
                    }
                    self.savePugRatings(ratingsFile, rkData)
                    self.loadPugRatings(ratingsFile)
            else:
                log.debug(f'loadPugRatings({ratingsFile}) ratingsData is not a valid object')
        return False
        
    def savePugRatings(self, ratingsFile, ratingsUpdates = None):
        """Saves the ranked game ratings data to the JSON configuration file"""
        return self.parent.savePugRatings(ratingsFile, ratingsUpdates)
        # Legacy code - now handled by parent
        with open(ratingsFile) as fr:
            try:
                ratingsData = json.load(fr)
            except:
                ratingsData = None
        if ratingsData not in [None,''] or ratingsUpdates not in [None,'']:
            with open(ratingsFile,'w') as fw:
                # Update the specific ratings data section before dumping it
                if ratingsUpdates not in [None,'']:
                    if 'rankedgames' in ratingsUpdates and ratingsUpdates['rankedgames'] is not None:
                        log.debug(f'savePugRatings({ratingsFile}) updating ratingsData directly from provided ratingsUpdates.')
                        ratingsData['rankedgames']  = ratingsUpdates['rankedgames'] # pass valid data straight into the file
                else:
                    if self.ratings not in [None,'']:
                        if ratingsData['rankedgames'] is not None:
                            for gamedata in ratingsData['rankedgames']:
                                if gamedata['mode'] == self.ratings['mode']:
                                    self.ratings['lastupdated'] = datetime.now().isoformat()
                                    for key in ['maps','eligibility','registrations','ratings','lastsync','fixedpicklimit','startmapfrompick','capMode','capWindow','capRole','games','scoring','lastupdated','randomorder']:
                                        if key not in self.ratings:
                                            if key in ['registrations','ratings','games']:
                                                self.ratings[key] = []
                                            elif key in ['maps','scoring']:
                                                self.ratings[key] = {}
                                            else:
                                                self.ratings[key] = ""
                                        gamedata[key] = self.ratings[key]
                        else:
                            ratingsData['rankedgames'].append(self.ratings)
                    else:
                        log.warning(f'savePugRatings({ratingsFile}) failed to generate ratingsData. Cached ratings not present and updates not provided.')
                ratingsData['savedate'] = datetime.now().isoformat()
                json.dump(ratingsData, fw, indent=4)
                fw.close()
        return True

    def makeRatedTeams(self, simulatedRatings=[]):
        """Uses bitmask comparison bin-sorting to work out a balanced set of teams"""
        if len(simulatedRatings):
            log.debug('makeRatedTeams() - Beginning simulated rated teams sorting...')
        else:
            log.debug('makeRatedTeams() - Beginning rated teams sorting...')
        if (self.teamsFull and len(simulatedRatings) == 0):
            log.debug('makeRatedTeams() - Teams already filled.')
            return
        playerRatings = []
        playerIDs = []
        playerMap = {}
        simRed = []
        simBlue = []
        if self.ratings in [None, '']:
            self.setRankedMode(MODE_CONFIG[self.mode].isRanked, False)
        if 'ratings' not in self.ratings:
            self.setRankedMode(MODE_CONFIG[self.mode].isRanked, False)
        if len(simulatedRatings):
            for p in simulatedRatings:
                playerIDs.append(p['did'])
                playerRatings.append(p['ratingvalue'])
                playerMap[int(p['did'])] = p['ratingvalue']
        else:
            for p in self.players: playerIDs.append(p.id)
            for rankedPlayer in self.ratings['ratings']:
                if str(rankedPlayer['did']) in playerIDs or int(rankedPlayer['did']) in playerIDs:
                    playerRatings.append(rankedPlayer['ratingvalue'])
                    playerMap[int(rankedPlayer['did'])] = rankedPlayer['ratingvalue']
        log.debug(f'makeRatedTeams() - playerIDs = {playerIDs}; playerRatings = {playerRatings}')
        minDiff = float('inf')
        for mask in range(1 << len(playerRatings)):
            if bin(mask).count('1') == len(playerRatings)/2:
                x = [playerRatings[i] for i in range(len(playerRatings)) if mask & (1 << i)]
                y = [playerRatings[i] for i in range(len(playerRatings)) if not mask & (1 << i)]
                diff = abs(sum(x) - sum(y))
                if diff < minDiff:
                    minDiff = diff
                    rankedRed = x
                    rankedBlue = y
        log.debug(f'makeRatedTeams() masked values: red={rankedRed}, blue={rankedBlue}')
        if len(simulatedRatings) == 0:
            self.redPower = sum(rankedRed)
            self.bluePower = sum(rankedBlue)
            msg = f'Red RP: {str(self.redPower)}; Blue RP: {str(self.bluePower)}'
        else:
            msg = f'Red RP: {str(sum(rankedRed))}; Blue RP: {str(sum(rankedBlue))}'
        # Establish self.red and self.blue 
        if len(simulatedRatings):
            for p in simulatedRatings:
                if (playerMap[p['id']] in rankedRed and p not in simRed and p not in simBlue):
                    simRed.append(p)
                    rankedRed.remove(playerMap[p['id']])
                if (playerMap[p['id']] in rankedBlue and p not in simRed and p not in simBlue):
                    simBlue.append(p)
                    rankedBlue.remove(playerMap[p['id']])
        else:
            for p in self.players:
                if (playerMap[p.id] in rankedRed and p not in self.red and p not in self.blue):
                    self.red.append(p)
                    rankedRed.remove(playerMap[p.id])
                if (playerMap[p.id] in rankedBlue and p not in self.red and p not in self.blue):
                    self.blue.append(p)
                    rankedBlue.remove(playerMap[p.id])
        if 'capMode' in self.ratings:
            redCapPicks = []
            blueCapPicks = []
            capMode = self.ratings['capMode']
            if capMode > 2:
                capMode = 2 # not yet supported, to be added in future as this function would need splitting into two parts
            if capMode == 2:
                if 'capRole' in self.ratings and len(self.ratings['capRole']) > 0:
                    for p in self.players:
                        for role in p.roles:
                            if str(role.name).lower() == str(self.ratings['capRole']).lower():
                                if p in self.red or p in simRed:
                                    redCapPicks.append(p)
                                if p in self.blue or p in simBlue:
                                    blueCapPicks.append(p)
                if len(redCapPicks) == 0 or len(blueCapPicks) == 0:
                    capMode = 1 # fall back to random
            if capMode == 1:
                if len(redCapPicks) == 0 and len(simulatedRatings) == 0:
                    redCapPicks = self.red
                elif len(redCapPicks) == 0 and len(simulatedRatings) > 0:
                    redCapPicks = simRed
                if len(blueCapPicks) == 0 and len(simulatedRatings) == 0: 
                    blueCapPicks = self.blue
                elif len(blueCapPicks) == 0 and len(simulatedRatings) > 0:
                    blueCapPicks = simBlue

            if capMode > 0 and len(redCapPicks) > 0 and len(blueCapPicks) > 0:
                redCap = random.choice(redCapPicks)
                if len(simulatedRatings):
                    simRed.remove(redCap)
                    simRed.insert(0,redCap)
                else:
                    self.red.remove(redCap)
                    self.red.insert(0,redCap)
                blueCap = random.choice(blueCapPicks)
                if len(simulatedRatings):
                    simBlue.remove(blueCap)
                    simBlue.insert(0,blueCap)
                else:
                    self.blue.remove(blueCap)
                    self.blue.insert(0,blueCap)
                if len(simulatedRatings) == 0:
                    msg = msg+f'\nRed captain: {redCap.mention}\nBlue captain: {blueCap.mention}'
        if len(simulatedRatings):
            msg = msg+f'\nSimulated Red team: {PLASEP.join(sp["name"] for sp in simRed)}'
            msg = msg+f'\nSimulated Blue team: {PLASEP.join(sp["name"] for sp in simBlue)}'
        msg = msg.replace("\n","; ")
        log.debug(f'makeRatedTeams() completed: {msg}')
        return msg

    def storeRankedPug(self, mode: str = '', matchCode: str = '', redScore: int = 0, blueScore: int = 0, timeStarted: str = '', hasEnded: bool = False, redPlayers: list = [], bluePlayers: list = [], maps: list = [], redPower = 0, bluePower = 0, timeEnded = ''):
        """Stores ranked pug match data and handles end-game scenarios"""
        if mode in [None, ''] and self.ranked:
            mode = self.mode
        if matchCode in [None,'','N/A']:
            if self.gameServer.matchCode in [None,'','N/A']:
                self.gameServer.matchCode = f'temp-{datetime.now().strftime("%Y%m%d%H%M%S")}'
            matchCode = self.gameServer.matchCode
            log.debug(f'storeRankedPug() - grabbed mode from gameServer.matchCode - {matchCode}')
        if matchCode in [None,'','N/A']:
            return False
        if hasEnded:
            timeEnded = datetime.now().isoformat()
        self.savePugRatings(self.ratingsFile) # ensure any recent changes to ratings data are saved before we load it again to update with match results
        rkData = self.loadPugRatings(self.ratingsFile, True)
        rkNewMatch = True
        rkUpdated = False
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode'] # update formatting
                    x['maps']['maplist'] = self.maps.mapListWeighting
                    if 'games' not in x:
                        x['games'] = []
                    for g in x['games']:
                        if g['gameref'].upper() == matchCode.upper():
                            rkNewMatch = False
                            if self.redPower > 0:
                                g['rpred'] = self.redPower
                            if self.bluePower > 0:
                                g['rpblue'] = self.bluePower
                            g['scorered'] = redScore
                            g['scoreblue'] = blueScore
                            g['completed'] = hasEnded
                            if (hasEnded):
                                g['enddate'] = timeEnded
                            if len(self.red) > 0:
                                g['teamred'] = self.returnPIDs(self.red)
                            if len(self.blue) > 0:
                                g['teamblue'] = self.returnPIDs(self.blue)
                            g['completed'] = hasEnded
                            if hasEnded:
                                rkData = self.applyRankedScoring(rkData, mode, g)
                            rkUpdated = True
                    if rkNewMatch:
                        if len(redPlayers) == 0:
                            redPlayers = self.red
                        if len(bluePlayers) == 0:
                            bluePlayers = self.blue
                        if len(maps) == 0:
                            maps = self.maps.maps
                        if redPower == 0 and self.redPower > 0:
                            redPower = self.redPower
                        if bluePower == 0 and self.bluePower > 0:
                            bluePower = self.bluePower
                        m = {
                            'gameref': matchCode,
                            'startdate': timeStarted,
                            'enddate': timeEnded,
                            'completed': hasEnded,
                            'maplist': maps,
                            'teamred': self.returnPIDs(redPlayers),
                            'teamblue': self.returnPIDs(bluePlayers),
                            'rpred': redPower,
                            'rpblue': bluePower,
                            'scorered': redScore,
                            'scoreblue': blueScore
                        }
                        m['capred'] = {
                            'id': m['teamred'][0],
                            'volunteered': False # adjust when capmode 3 is supported
                        }
                        m['capblue'] ={
                            'id': m['teamblue'][0],
                            'volunteered': False # adjust when capmode 3 is supported
                        }
                        x['games'].append(m)
                        if hasEnded:
                            rkData = self.applyRankedScoring(rkData, mode, m)
                        rkUpdated = True
        if rkUpdated:
            if self.savePugRatings(self.ratingsFile, rkData):
                if (self.ranked):
                    if self.parent.ratingsLock:
                        self.parent.ratingsLock = False # unlock ratings for other instances to access
                    self.setRankedMode(self.ranked, True)
        return True

    def applyRankedScoring(self, rkData: object, mode: str, match: object, void: bool = False, player: int = 0):
        """Searches for the given match code and applies given scoring logic to players"""
        # TO-DO Add voluntary captain scoring when capmode 3 is supported
        winners = []
        losers = []
        modeData = {}
        winCap = []
        loseCap = []
        winScore = 0
        loseScore = 0
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    modeData = x
        elif 'mode' in rkData:
            if str(rkData['mode']).upper() == mode.upper():
                modeData = rkData
        if modeData != {} and match != {}:            
            if match['scorered'] > match['scoreblue']:
                winners = match['teamred']
                losers = match['teamblue']
                winScore = match['scorered']
                loseScore = match['scoreblue']
                winCap.append(match['capred']['id'])
                loseCap.append(match['capblue']['id'])
            elif match['scoreblue'] > match['scorered']:
                winners = match['teamblue']
                losers = match['teamred']
                winScore = match['scoreblue']
                loseScore = match['scorered']
                winCap.append(match['capblue']['id'])
                loseCap.append( match['capred']['id'])
            else:
                winners = match['teamred'] + match['teamblue']
                losers = match['teamred'] + match['teamblue']
                winScore = match['scorered']
                loseScore = match['scoreblue']
                winCap.append(match['capred']['id'])
                winCap.append(match['capblue']['id'])
                loseCap.append( match['capred']['id'])
                loseCap.append( match['capblue']['id'])
            if void:
                match['completed'] = False
                log.debug(f'applyRankedScoring() - Voided match {match["gameref"]}')
            elif 'scoring' in modeData:
                scoremode = modeData['scoring']['mode']
                capWinRP = modeData['scoring']['capWin']
                capLoseRP = modeData['scoring']['capLose']
                if scoremode == 'permap':
                    winRP = (modeData['scoring']['teamWin']*winScore) + (modeData['scoring']['teamLose']*loseScore)
                    loseRP = (modeData['scoring']['teamWin']*loseScore) + (modeData['scoring']['teamLose']*winScore)
                else:
                    winRP = modeData['scoring']['teamWin']
                    loseRP = modeData['scoring']['teamLose']
                for p in modeData['ratings']:
                    if p['lastgameref'] != match['gameref']:
                        if len(p['lastgameref']) == 0:
                            p['lastgameref'] = 'admin-set'
                        if len(p['lastgamedate']) == 0:
                            p['lastgamedate'] = p['ratingdate']
                    if p['did'] == player or (player == 0 and p['lastgameref'] != match['gameref']):
                        if p['did'] in winners or p['did'] in losers:
                            # Move last game to history
                            if 'ratinghistory' not in p:
                                p['ratinghistory'] = []
                            if p['ratinghistory'] in [None,'']:
                                p['ratinghistory'] = []
                            p['ratinghistory'].append({
                                'matchref': p['lastgameref'],
                                'matchdate': p['lastgamedate'],
                                'ratingbefore': p['ratingprevious'],
                                'ratingafter': p['ratingvalue']
                            })
                            # Update latest rating block
                            p['ratingprevious'] = p['ratingvalue']
                            if p['did'] in winners:
                                p['ratingvalue'] = p['ratingvalue']+winRP
                                if p['did'] in winCap:
                                    p['ratingvalue'] = p['ratingvalue']+capWinRP
                            if p['did'] in losers:
                                p['ratingvalue'] = p['ratingvalue']+loseRP
                                if p['did'] in loseCap:
                                    p['ratingvalue'] = p['ratingvalue']+capLoseRP
                            if 'ratinghistory' in p:
                                p['ratinghistory'] = sorted(p['ratinghistory'], key=lambda g: datetime.fromisoformat(g['matchdate'])) 
                                #if len(p['ratinghistory']) > 150:
                                #    p['ratinghistory'][:] = p['ratinghistory'][-150:]
                            p['lastgamedate'] = match['startdate']
                            if 'gameref' in match and len(match['gameref']):
                                p['lastgameref'] = match['gameref']
                            else:
                                p['lastgameref'] = 'admin-set'
                            if player > 0:
                                return p
        return rkData

    def returnPIDs(self, players):
        """Returns a list of Discord Player IDs (PIDs)"""
        pids = []
        for p in players: 
            if type(p) is int:
                pids.append(p)
            else:
                pids.append(p.id)
        return pids
    
    def convertQueuedPlayers(self):
        """Converts a player queue to active players"""
        log.debug(f'convertQueuedPlayers() - Queue length: {str(len(self.queuedPlayers))}; Pug Locked: {str(self.pugLocked)}')
        if len(self.queuedPlayers):
            self.players = self.queuedPlayers
        self.queuedPlayers = []
        return True

    def _getModeForPug(self):
        """Helper method to get the mode for this pug instance"""
        return self.mode

#########################################################################################
# Static methods for cogs.
#########################################################################################
def isActiveChannel_Check(ctx): return ctx.bot.get_cog('PUG').isActiveChannel(ctx)

def isPugInProgress_Warn(ctx): return ctx.bot.get_cog('PUG').isPugInProgress(ctx, warn=True)

def isPugInProgress_Ignore(ctx): return ctx.bot.get_cog('PUG').isPugInProgress(ctx, warn=False)

#########################################################################################
# Custom Exceptions
#########################################################################################
class PugIsInProgress(commands.CommandError):
    """Raised when a pug is in progress"""
    pass

#########################################################################################
# Main pug cog class.
#########################################################################################
class PUG(commands.Cog):
    def __init__(self, bot, configFile=DEFAULT_CONFIG_FILE):
        self.bot = bot
        self.activeChannel = None 
        self.customStaticEmojis = {}
        self.customAnimatedEmojis = {}
        self.utReporterChannel = None
        self.cachedServers = None
        self.configLoadTime = 0
        self.configFile = configFile
        self.ratingsFile = DEFAULT_RATING_FILE
        self.allRatings = {} # centrally cached ratings data for all modes, keyed by mode name (e.g., 'rASPlus') - used to reduce file I/O and for quick access when switching modes or validating player eligibility
        self.ratingsLock = False # simple lock to prevent multiple simultaneous ratings file accesses, as these can cause conflicts and data loss.
        self.ratingsSyncAPI = {'matchDataURL':'','ratingsDataURL':'','playerDataURL':'','apiKey':''}

        self.pugInstances = {}  # dict[channelId, dict[mode, AssaultPug]] - supports multiple pugs per channel by mode
        self._defaultPugInfo = AssaultPug(numPlayers=DEFAULT_PLAYERS, numMaps=DEFAULT_MAPS, pickModeTeams=DEFAULT_PICKMODETEAMS, pickModeMaps=DEFAULT_PICKMODEMAPS, configFile=self.configFile, ratingsFile=self.ratingsFile, modeLimit=0, mode=MODE_DEFAULT, parent=self, channelId=None)

        # Track players across all instances to prevent multi-instance conflicts
        self.playerInstances = {}  # playerId -> set of (channelId, mode) tuples
        self.playerPreferences = {}  # playerId -> set of (mode, maps) tuples
        self.tempQueuedPlayers = {}  # channelId -> list of (player, flags) tuples temporarily removed
        self.serverLocks = {}  # serverRef -> (channelId, mode) tuple (prevents multiple instances using same server)
        self.modePugLastActivity = {}  # (channelId, mode) -> datetime (tracks when pug was last updated for default selection)

        self.loadPugConfig(configFile)
        self.cacheGuildEmojis()

        # Used to keep track of if both teams have requested a reset while a match is in progress.
        # We'll only make use of this in the reset() function so it only needs to be put back to
        # False when a new match is setup.
        self.resetRequestRed = True
        self.resetRequestBlue = True

        # Start the looped task which checks the server when a pug is in progress (to detect match finished)
        self.updateGameServer.add_exception_type(asyncpg.PostgresConnectionError)
        self.updateGameServer.start()

        # Start the match report loop task
        self.sendMatchReport.start()

        # Start the GameSpy query loops
        self.updateUTQueryReporter.start()
        self.updateUTQueryStats.start()
        
        # Start the Emoji update loop
        self.updateGuildEmojis.start()

        # Start the looped task for server rotation
        self.updateServerRotation.start()
        
        self.lastPokeTime = datetime.now()
        self.lastAPISyncTime = datetime.now()

    @property
    def pugInfo(self):
        """Deprecated: returns default pug info. Use getPugForChannel() or getPugForModeInChannel() for explicit access."""
        if self.activeChannel is None: 
            log.debug('pugInfo property accessed (deprecated), returning defaultPugInfo')
            return self._defaultPugInfo 
        log.debug('pugInfo property accessed (deprecated), returning most active pug for active channel.')
        return self.getPugForChannel(self.activeChannel.id)

    @property
    def currentPugInfo(self):
        """Get the pug instance for the current active channel.
        
        Returns:
            AssaultPug instance for the current channel, or _defaultPugInfo if no active channel
        """
        if self.activeChannel is None:
            return self._defaultPugInfo
        return self.getPugForChannel(self.activeChannel.id)

    def removePugForModeInChannel(self, channelId, mode):
        targetPug = self.getPugForModeInChannel(channelId=channelId, mode=mode, ignoreMissing=True)
        if targetPug is None:
            return True
        log.debug(f'removePugForModeInChannel({channelId},{targetPug.mode}) - Removing PUG')
        mode = targetPug.mode
        del self.pugInstances[channelId][targetPug.mode]
        log.debug(f'removePugForModeInChannel({channelId},{mode}) - Saving config')
        return self.savePugConfig(self.configFile)

    def getPugForModeInChannel(self, channelId, mode, ignoreMissing: bool = False):
        """Get or create a pug instance for a specific channel and mode.
        
        Args:
            channelId: ID of the Discord channel
            mode: Game mode string (e.g., 'stdAS', 'proAS', 'rASplus')
            ignoreMissing: True/False - if True and a PUG for the mode + channel are not present, it won't create them
            
        Returns:
            AssaultPug instance or _defaultPugInfo if channel is None, or None if ignoreMissing is True and instance is not present
        """
        if channelId is None or mode is None:
            return self._defaultPugInfo
        
        if channelId not in self.pugInstances:
            self.pugInstances[channelId] = {}
        
        mode = next((key for key, _ in MODE_CONFIG.items() if key.upper()==mode.upper()), None)

        if mode not in self.pugInstances[channelId] and ignoreMissing == False:
            # Create new pug instance for this mode in this channel
            self.pugInstances[channelId][mode] = AssaultPug(
                numPlayers=DEFAULT_PLAYERS,
                numMaps=DEFAULT_MAPS,
                pickModeTeams=DEFAULT_PICKMODETEAMS,
                pickModeMaps=DEFAULT_PICKMODEMAPS,
                configFile=DEFAULT_CONFIG_FILE,
                ratingsFile=DEFAULT_RATING_FILE,
                modeLimit=0,
                mode=mode,
                parent=self,
                channelId=channelId
            )
            for s in self.pugInstances[channelId][mode].servers:
                s.parent = self.pugInstances[channelId][mode] # Ensure servers have reference to their specific pug instance
            modeCheck = self.pugInstances[channelId][mode].setMode(mode)
            if not modeCheck[0]:
                log.debug(f'getPugForModeInChannel() - Failed to set mode {mode} for new pug instance in channel {channelId}. Error: {modeCheck[1]}')
                del self.pugInstances[channelId][mode] # Remove the instance if mode setup failed
                return None
        elif mode not in self.pugInstances[channelId] and ignoreMissing == True:
            return None
        return self.pugInstances[channelId][mode]

    def getDefaultPugByActivity(self, channelId):
        """Get the most active pug in a channel based on player count, recent activity, and creation time.
        
        Args:
            channelId: ID of the Discord channel
            
        Returns:
            tuple (mode, AssaultPug) for the most active pug, or (MODE_DEFAULT, _defaultPugInfo) if no pugs exist
        """
        if channelId is None or channelId not in self.pugInstances:
            log.debug(f'getDefaultPugByActivity() - No pugs found for channelId {channelId}, returning defaultPugInfo.')
            return (MODE_DEFAULT, self._defaultPugInfo)
        
        modes_dict = self.pugInstances[channelId]
        if not modes_dict:
            log.debug(f'getDefaultPugByActivity() - No pugs instances for channelId {channelId}, returning defaultPugInfo.')
            return (MODE_DEFAULT, self._defaultPugInfo)
        
        # Sort by: most players, then most recent activity, then most recent creation
        def pug_sort_key(item):
            mode, pug = item
            player_count = len(pug.players)
            ready_state = int(pug.captainsReady) + int(pug.mapsReady) + int(pug.matchReady)
            # Get last activity time; default to creation time if not set
            last_activity = self.modePugLastActivity.get((channelId, mode), datetime.now())
            # Return tuple: (-player_count for descending), (-last_activity timestamp for descending)
            return (-ready_state, -player_count, -last_activity.timestamp())
        
        sorted_modes = sorted(modes_dict.items(), key=pug_sort_key)
        most_active_mode, most_active_pug = sorted_modes[0]
        return (most_active_mode, most_active_pug)

    def getPugForChannel(self, channelId, mode=None):
        """Get a pug for a channel, optionally filtered by mode.
        
        Args:
            channelId: Discord channel ID
            mode: Optional game mode string. If None, returns the most active pug.
            
        Returns:
            AssaultPug instance or _defaultPugInfo if not found
        """
        if channelId is None:
            return self._defaultPugInfo
        
        if mode is None:
            # Return most active pug in channel
            _, pug = self.getDefaultPugByActivity(channelId)
            return pug
        else:
            # Return specific mode pug
            return self.getPugForModeInChannel(channelId=channelId, mode=mode)

    def validatePugChannel(self, channel, mode=None):
        """Ensure a pug exists for the given channel and mode.
        
        Args:
            channel: Discord channel object
            mode: Game mode string. If None, returns most active pug.
            
        Returns:
            AssaultPug instance or _defaultPugInfo if channel is None
        """
        if channel is None:
            return self._defaultPugInfo
        
        if len(self.pugInstances) == 0 and channel.id not in self.pugInstances:
            if mode is None:
                mode = MODE_DEFAULT 

        if mode is None:
            # Return most active pug
            _, pug = self.getDefaultPugByActivity(channel.id)
            return pug
        else:
            # Create if needed and return
            return self.getPugForModeInChannel(channelId=channel.id, mode=mode)

    def getAllPugsInChannel(self, channelId):
        """Get all pug instances in a channel.
        
        Args:
            channelId: Discord channel ID
            
        Returns:
            dict[mode, AssaultPug] of all pugs in the channel, or empty dict if none
        """
        if channelId is None or channelId not in self.pugInstances:
            return {}
        return self.pugInstances[channelId]

    def getAllActivePugs(self):
        """Iterate over all active pug instances across all channels.
        
        Yields:
            tuple (channelId, mode, AssaultPug)
        """
        for channelId, modes_dict in self.pugInstances.items():
            for mode, pug in modes_dict.items():
                yield (channelId, mode, pug)

    def updatePugActivity(self, channelId, mode):
        """Update the last activity timestamp for a pug.
        
        Args:
            channelId: Discord channel ID
            mode: Game mode string
        """
        self.modePugLastActivity[(channelId, mode)] = datetime.now()

    def trackPlayerJoin(self, player, channelId, mode, updateConfig: bool = False):
        """Track that a player joined a pug.
        
        Args:
            player: Discord user object
            channelId: Discord channel ID
            mode: Game mode string
        """
        player_id = player.id
        if player_id not in self.playerInstances:
            self.playerInstances[player_id] = set()
        self.playerInstances[player_id].add((channelId, mode))
        self.updatePugActivity(channelId, mode)
        if updateConfig:
            self.savePugConfig(self.configFile)

    def trackPlayerLeave(self, player, channelId, mode):
        """Track that a player left a pug.
        
        Args:
            player: Discord user object
            channelId: Discord channel ID
            mode: Game mode string
        """
        player_id = player.id
        if player_id in self.playerInstances:
            self.playerInstances[player_id].discard((channelId, mode))

    def getPlayerActivePugs(self, player):
        """Get all (channelId, mode) tuples where player is active.
        
        Args:
            player: Discord user object
            
        Returns:
            set of (channelId, mode) tuples
        """
        return self.playerInstances.get(player.id, set())

    def getPlayerActivePugChannel(self, player):
        """Get the first active pug channel for a player (backward compat wrapper).
        
        In the new multi-mode architecture, a player can be in multiple channels/modes.
        This returns the first channel found, or None if no active pugs.
        
        Args:
            player: Discord user object
        
        Returns:
            Channel ID or None if no active pugs
        """
        activePugs = self.getPlayerActivePugs(player)
        if activePugs:
            channelId, _ = next(iter(activePugs))
            return channelId
        return None
    
    def setActiveChannel(self, channel): 
        if channel is None: 
            return None 
        self.activeChannel = channel 
        self.validatePugChannel(channel) 
        return channel 
    
    def cog_unload(self):
        self.updateGameServer.cancel()
        self.sendMatchReport.cancel()
        self.updateUTQueryReporter.cancel()
        self.updateUTQueryStats.cancel()
        self.updateGuildEmojis.cancel()
        self.updateServerRotation.cancel()

    def getPlayerInstances(self, player):
        """Get all (channelId, mode) tuples where a player is currently signed up.
        
        Wrapper for backward compatibility; use getPlayerActivePugs() for new code.
        """
        return self.getPlayerActivePugs(player)

    def getPlayerActivePugs(self, player):
        """Get all (channelId, mode) tuples where a player is currently signed up."""
        return self.playerInstances.get(player.id, set())

    def getPlayerActivePugChannel(self, player):
        """Get the channel ID where a player is active, or None if not active.
        
        For backward compatibility - returns first channel found, but players can now be in multiple.
        """
        activePugs = self.getPlayerActivePugs(player)
        if activePugs:
            return next(iter(activePugs))[0]  # Return first channelId
        return None

    def handleMultiInstanceConflicts(self, player, targetChannelId, targetMode):
        """Handle conflicts when a player tries to join multiple pugs.
        
        Allows intra-channel multi-mode participation but prevents cross-channel conflicts.
        """
        activePugs = self.getPlayerActivePugs(player)
        targetkey = (targetChannelId, targetMode)
        
        # Check for cross-channel conflicts
        conflictingChannels = {channelId for channelId, mode in activePugs if channelId != targetChannelId}
        if conflictingChannels:
            return False, f'You are already in active pugs in other channels: {", ".join(str(cid) for cid in conflictingChannels)}'
        
        # Allow intra-channel multi-mode (same channel, different modes)
        # No conflicts within the same channel
        return True, None

    def pushMultiInstancePlayers(self, channelId, activeMode):
        """Puts a hold on players in the queues of non-active games while an active game is underway"""
        if channelId not in self.pugInstances:
            return False, f'No pugs found for channel ID {channelId}'
        playerList = []
        for mode, pug in self.pugInstances[channelId].items():
            if mode == activeMode:
                playerList = pug.players + pug.queuedPlayers + pug.red + pug.blue
                activeServer = pug.gameServer.gameServerRef
                continue
        
        for mode, pug in self.pugInstances[channelId].items():
            if mode != activeMode:
                if pug.gameServer.gameServerRef == activeServer:
                    pug.pugTempLocked = 2 # set long-lock due to same server being used
                if len(playerList):
                    for player in pug.players:
                        if player in playerList:
                            #self.trackPlayerLeave(player, channelId, mode)
                            pug.pugTempLocked = 2 # set long-lock to prevent matchmaking while current game is active
                            if channelId not in self.tempQueuedPlayers:
                                self.tempQueuedPlayers[channelId] = []
                            self.tempQueuedPlayers[channelId].append((player, {'mode': mode}))
        return True

    def popMultiInstancePlayers(self, channelId, activeMode):
        """Adds players back to the queue of non-active games after an active game has completed"""
        if channelId not in self.pugInstances:
            return False, f'No pugs found for channel ID {channelId}'

        for mode, pug in self.pugInstances[channelId].items():
            if mode != activeMode:
                if channelId in self.tempQueuedPlayers:
                    log.error(f'popMultiInstancePlayers() - Enumerating tempQueuedPlayers: {str(self.tempQueuedPlayers[channelId])}')
                    for player, data in self.tempQueuedPlayers[channelId]:
                        if 'mode' in data and str(data['mode']).upper() == str(mode).upper():
                            try:
                                self.trackPlayerJoin(player, channelId, mode)
                                data = {}
                                player = 0
                            except Exception as e:
                                log.error(f'popMultiInstancePlayers() - Error re-queuing player {player} to mode {mode} in channel {channelId}: {e}')
                    log.error(f'popMultiInstancePlayers() - Final tempQueuedPlayers: {str(self.tempQueuedPlayers[channelId])}')
                if pug.pugTempLocked > 1:
                    log.error(f'popMultiInstancePlayers() - Removed long-lock from {mode} pug in channel {channelId}')
                    pug.pugTempLocked = 0
                    #self.processPugStatus(None, pug=pug)
        return True

    def restoreMultiInstancePlayers(self, pug):
        """Restore players to a pug instance after reload/server restart.
        
        Updates playerInstances tracking based on current pug state.
        """
        channelId = pug.channelId
        mode = pug.mode
        
        for player in pug.players:
            self.trackPlayerJoin(player, channelId, mode)
        
        for player in pug.queuedPlayers:
            # Queued players are not tracked as active
            pass

    def loadPugRatings(self, ratingsFile, returnDataOnly: bool = False):
        """Loads the ranked game ratings data from the JSON configuration file"""
        self.allRatings = None # save before load?
        log.debug(f'loadPugRatings({ratingsFile}) started')
        with open(ratingsFile, 'r') as f:
            log.debug(f'loadPugRatings({ratingsFile}) json.load enter')
            try:
                ratingsData = json.load(f)
            except:
                ratingsData = None
            log.debug(f'loadPugRatings({ratingsFile}) json.load finished')
            if ratingsData:
                log.debug(f'loadPugRatings({ratingsFile}) ratingsData loaded')
                if returnDataOnly: # For in-line updates
                    return ratingsData
                if 'syncapi' in ratingsData:
                    self.ratingsSyncAPI = ratingsData['syncapi']
                if 'rankedgames' in ratingsData:
                    self.allRatings = ratingsData
                    # Find the mode and specific ratings data for eligible ranked PUGs
                    syncedModes = []
                    for gamedata in self.allRatings['rankedgames']:
                        log.debug(f'loadPugRatings({ratingsFile}) looking for mode to update: {gamedata["mode"]}')
                        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=gamedata['mode'], ignoreMissing=True)
                        if pug is not None:
                            pug.ratings = gamedata
                            log.debug(f'loadPugRatings({ratingsFile}) cached ratings data for {pug.mode}')
                            syncedModes.append(gamedata['mode'])
                        else:
                            log.debug(f'loadPugRatings({ratingsFile}) could not devolve ratings data for {gamedata["mode"]}')
                    return True
                else:
                    # Generate an empty ranked schema with the default mode
                    rkData = {
                        'syncapi': self.ratingsSyncAPI,
                        'rankedgames': [
                            {
                                'mode': MODE_RANKED_DEFAULT,
                                'maps':{},
                                'eligibility':'',
                                'registrations':[],
                                'ratings':[],
                                'lastsync':''
                            }
                        ]
                    }
                    self.savePugRatings(ratingsFile, rkData)
                    log.debug(f'loadPugRatings({ratingsFile}) established new ratings data.')
                    self.loadPugRatings(ratingsFile)
            else:
                log.debug(f'loadPugRatings({ratingsFile}) ratingsData is not a valid object')
        return False
        
    def savePugRatings(self, ratingsFile, ratingsUpdates = None):
        with open(ratingsFile) as fr:
            try:
                ratingsData = json.load(fr)
            except:
                ratingsData = None
        if ratingsData not in [None,''] or ratingsUpdates not in [None,'']:
            with open(ratingsFile,'w') as fw:
                # Update the specific ratings data section before dumping it
                if ratingsUpdates not in [None,'']:
                    if 'rankedgames' in ratingsUpdates and ratingsUpdates['rankedgames'] is not None:
                        log.debug(f'savePugRatings({ratingsFile}) updating ratingsData directly from provided ratingsUpdates.')
                        ratingsData['rankedgames']  = ratingsUpdates['rankedgames'] # pass valid data straight into the file
                else:
                    if self.allRatings not in [None,'',{}]:
                        log.debug(f'savePugRatings({ratingsFile}) updating ratingsData from cached allRatings object...')
                        for cacheddata in self.allRatings['rankedgames']:
                            if ratingsData['rankedgames'] is not None:
                                for gamedata in ratingsData['rankedgames']:
                                    if str(gamedata['mode']).upper() == str(cacheddata['mode']).upper():
                                        cacheddata['lastupdated'] = datetime.now().isoformat()
                                        for key in ['maps','eligibility','registrations','ratings','lastsync','fixedpicklimit','startmapfrompick','capMode','capWindow','capRole','games','scoring','lastupdated','randomorder']:
                                            if key not in cacheddata:
                                                if key in ['registrations','ratings','games']:
                                                    cacheddata[key] = []
                                                elif key in ['maps','scoring']:
                                                    cacheddata[key] = {}
                                                else:
                                                    cacheddata[key] = ""
                                            gamedata[key] = cacheddata[key]
                            else:
                                ratingsData['rankedgames'].append(cacheddata)
                    else:
                        log.warning(f'savePugRatings({ratingsFile}) failed to generate ratingsData. Cached ratings not present and updates not provided.')
                ratingsData['savedate'] = datetime.now().isoformat()
                json.dump(ratingsData, fw, indent=4)
                fw.close()
        return True

#########################################################################################
# Loops.
#########################################################################################
    @tasks.loop(seconds=60.0)
    async def updateGameServer(self):
        # Iterate over all active pugs across all channels and modes
        for channelId, mode, pug in list(self.getAllActivePugs()):
            if not pug.pugLocked:
                continue
            queueCheck = False
            log.info(f'Updating game server for channel {channelId} mode {mode} [pugLocked=True]..')
            if not pug.gameServer.updateServerStatus():
                log.warning('Cannot contact game server.')
            if len(pug.queuedPlayers):
                queueCheck = True
            if pug.gameServer.processMatchFinished():
                self.savePugConfig(self.configFile)
                channel = discord.Client.get_channel(self.bot, channelId)
                if channel is None:
                    continue
                msg = f'Match finished. Resetting pug ({mode})'
                if pug.ranked:
                    msg = msg + ' and updating player RP.'
                    await channel.send(msg)
                else:
                    msg = msg + '...'
                    await channel.send(msg)
                if pug.resetPug():
                    await channel.send(pug.format_pug())
                    log.info('Match over.')
                    if queueCheck and pug.playersFull:
                        await channel.send(f'Queued players have been added and the pug is full. When ready, start the next pug by sending **!pug {pug.mode}**')
                    continue
                await channel.send('Reset failed.')
                log.error('Reset failed')

    @updateGameServer.before_loop
    async def before_updateGameServer(self):
        log.info('Waiting before updating game server...')
        await self.bot.wait_until_ready()
        log.info('Ready.')

    @tasks.loop(seconds=4.0)
    async def updateUTQueryReporter(self):
        if self.utReporterChannel is None:
            return
        for channelId, mode, pug in self.getAllActivePugs():
            if pug.gameServer.utQueryReporterActive:
                channel = discord.Client.get_channel(self.bot, channelId)
                if channel is None:
                    continue
                await self.queryServerConsole()
        return

    @updateUTQueryReporter.before_loop
    async def before_updateUTQueryReporter(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=60.0)
    async def updateUTQueryStats(self):
        if self.utReporterChannel is None:
            return
        for channelId, mode, pug in self.getAllActivePugs():
            channel = discord.Client.get_channel(self.bot, channelId)
            if channel is None:
                continue
            if pug.gameServer.utQueryStatsActive:
                if ('laststats' not in pug.gameServer.utQueryData) or ('laststats' in pug.gameServer.utQueryData and int(time.time()) - int(pug.gameServer.utQueryData['laststats']) > 55):
                    await self.queryServerStats(cacheonly=False, pug=pug)
            elif pug.gameServer.utQueryReporterActive and pug.pugLocked:
                # Skip one cycle, then re-enable stats
                pug.gameServer.utQueryStatsActive = True
        return

    @updateUTQueryStats.before_loop
    async def before_updateUTQueryStats(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=10.0)
    async def sendMatchReport(self):
        for channelId, mode, pug in self.getAllActivePugs():
            if pug.matchReportPending and pug.pugLocked != True:
                channel = discord.Client.get_channel(self.bot, channelId)
                if channel is None:
                    continue
                matchref = 'last'
                if len(pug.gameServer.lastMatchCode):
                    matchref = pug.gameServer.lastMatchCode
                elif len(pug.gameServer.matchCode):
                    matchref = pug.gameServer.matchCode
                log.debug(f'Sending match report to {self.activeChannel}; last mode={pug.lastPlayedMode}; last matchref={matchref};')
                pug.matchReportPending = False
                await self.rkrp(self.activeChannel, mode=pug.lastPlayedMode, matchref=matchref)
        return
    
    @sendMatchReport.after_loop
    async def on_sendMatchReport_cancel(self):
        for channelId, mode, pug in self.getAllActivePugs():
            pug.matchReportPending = False

    @tasks.loop(minutes=5)
    async def updateGuildEmojis(self):
        self.cacheGuildEmojis()
        return
    
    @tasks.loop(hours=1)
    async def updateServerRotation(self):
        # Only auto-rotate between 6:00 and 9:59 am on a Monday
        if datetime.now().weekday() == 0 and datetime.now().hour >= 6 and datetime.now().hour <= 9:
            for channelId, mode, pug in self.getAllActivePugs():
                if not pug.pugLocked:
                    log.debug(f'updateServerRotation loop - calling checkServerRotation() for {channelId} {mode} pug')
                    pug.gameServer.checkServerRotation()
        return
#########################################################################################
# Utilities.
#########################################################################################
    def loadPugConfig(self, configFile):
        with open(configFile) as f:
            info = json.load(f)
            if info and 'pug' in info:
                # Load reporter settings first
                if 'reporterchannelid' in info['pug']:
                    channelID = info['pug']['reporterchannelid']
                    channel = discord.Client.get_channel(self.bot, channelID)
                    if channel:
                        self.utReporterChannel = channel
                if 'reporterconsolewatermark' in info['pug']:
                    self._defaultPugInfo.gameServer.utQueryConsoleWatermark = info['pug']['reporterconsolewatermark']
                # Load player preferences
                if 'playerprefs' in info['pug'] and isinstance(info['pug']['playerprefs'], dict):
                    self.playerPreferences = info['pug']['playerprefs']
                # Load per-channel PUG states
                if 'channels' in info['pug'] and isinstance(info['pug']['channels'], dict):
                    for channelId_str, gameInfo in info['pug']['channels'].items():
                        try:
                            channelId = int(channelId_str)
                        except Exception:
                            continue
                        channel = discord.Client.get_channel(self.bot, channelId)
                        if not channel:
                            continue
                        if self.activeChannel is None:
                            self.setActiveChannel(channel)
                        for modeIndex, modeData in gameInfo.items():
                            pug = None
                            log.debug(f'loadPugConfig() - iterating through pug.channels.{str(modeIndex)}: {str(modeData)}')
                            if 'active' not in modeData or ('active' in modeData and modeData['active'] != False): # only restore active pugs
                                pug = self.getPugForModeInChannel(channelId=channelId, mode=modeIndex)
                            else:
                                pug = self.getPugForModeInChannel(channelId=channelId, mode=modeIndex, ignoreMissing=True)
                            if pug is not None:
                                if 'modelimit' in modeData:
                                    pug.modeLimit = modeData['modelimit']
                                if 'playerlimit' in modeData:
                                    pug.setMaxPlayers(modeData['playerlimit'])
                                if 'maxmaps' in modeData:
                                    pug.maps.setMaxMaps(modeData['maxmaps'])
                                if 'server' in modeData:
                                    pug.gameServer.useServer(index=-1,autostart=False,byref=modeData['server'])
                                if 'timesaved' in modeData:
                                    try:
                                        time_saved = datetime.fromisoformat(modeData['timesaved'])
                                    except Exception:
                                        time_saved = None
                                    if time_saved and (datetime.now() - time_saved).total_seconds() < 60 and 'signed' in modeData:
                                        players = modeData['signed']
                                        if players:
                                            for playerId in players:
                                                player = channel.guild.get_member(playerId)
                                                if player:
                                                    pug.addPlayer(player)
                                        if pug.ranked and pug.playerQueueFull:
                                            self.processPugStatus(ctx=channel, pug=pug) # consider checking whether already running
                                if 'lastpug' in modeData and isinstance(modeData['lastpug'], dict):
                                    lastpug = modeData['lastpug']
                                    pug.lastPug = lastpug # store the whole object for embed
                                    if 'pugstr' in lastpug:
                                        pug.lastPugStr = lastpug['pugstr'] # legacy handling
                                    if 'timestarted' in lastpug:
                                        try:
                                            pug.lastPugTimeStarted = datetime.fromisoformat(lastpug['timestarted'])
                                        except Exception:
                                            pug.lastPugTimeStarted = None
                                self.restoreMultiInstancePlayers(pug)
                                log.debug(f'loadPugConfig() - restored game state for {pug.mode} in {pug.channelId}')
                            else:
                                log.debug(f'loadPugConfig() - ignored game state restoration for {modeIndex} in {channelId}')
                    if 'activechannelid' in info['pug']:
                        channel = discord.Client.get_channel(self.bot, info['pug']['activechannelid'])
                        if channel and channel.id in self.pugInstances:
                            self.activeChannel = channel
                elif 'activechannelid' in info['pug']:
                    # Fall back to single-channel mode, legacy config
                    channelId = info['pug']['activechannelid']
                    channel = discord.Client.get_channel(self.bot, channelId)
                    log.info(f'Loaded active channel id: {channelId} => channel: {channel}')
                    if channel:
                        self.setActiveChannel(channel)
                        pug = self.getPugForChannel(channelId=channelId)
                        if 'current' in info['pug']:
                            current = info['pug']['current']
                            modelimit = 0
                            if 'mode' in current:
                                pug = self.getPugForModeInChannel(channelId=channelId, mode=current['mode'], ignoreMissing=True)
                                if pug is None:
                                    pug = self.getPugForChannel(channelId=channelId)
                                pug.modeLimit = modelimit
                                pug.setMode(current['mode'],True) # TEST ME
                            else:
                                pug = self.getPugForChannel(channelId=channelId)
                            if 'modelimit' in current:
                                pug.modeLimit = current['modelimit']
                            if 'playerlimit' in current:
                                pug.setMaxPlayers(current['playerlimit'])
                            if 'maxmaps' in current:
                                pug.maps.setMaxMaps(current['maxmaps'])
                            if 'timesaved' in current:
                                try:
                                    time_saved = datetime.fromisoformat(current['timesaved'])
                                except Exception:
                                    time_saved = None
                                if time_saved and (datetime.now() - time_saved).total_seconds() < 60 and 'signed' in current:
                                    players = current['signed']
                                    if players:
                                        for playerId in players:
                                            player = channel.guild.get_member(playerId)
                                            if player:
                                                pug.addPlayer(player)
                        if 'lastpug' in info['pug']:
                            lastpug = info['pug']['lastpug']
                            if 'pugstr' in lastpug:
                                pug.lastPugStr = lastpug['pugstr']
                            if 'timestarted' in lastpug:
                                try:
                                    pug.lastPugTimeStarted = datetime.fromisoformat(lastpug['timestarted'])
                                except Exception:
                                    pug.lastPugTimeStarted = None
                        # Restore player tracking after loading pug
                        self.restoreMultiInstancePlayers(pug)
                    else:
                        log.warning('No active channel id found in config file.')
            else:
                log.error(f'PUG: Config file could not be loaded: {configFile}')
            f.close()
            self.configLoadTime = datetime.now().isoformat()
        return True

    def savePugConfig(self, configFile):
        with open(configFile) as f:
            info = json.load(f)
            if 'pug' not in info:
                info['pug'] = {}
            if self.activeChannel:
                info['pug']['activechannelid'] = self.activeChannel.id
            else:
                info['pug']['activechannelid'] = 0
            if 'playerprefs' not in info['pug']:
                info['pug']['playerprefs'] = {}
            if len(self.playerPreferences) > 0:
                info['pug']['playerprefs'] = self.playerPreferences
            if self.utReporterChannel:
                info['pug']['reporterchannelid'] = self.utReporterChannel.id
            else:
                info['pug']['reporterchannelid'] = 0
            prevChannelData = {}
            if 'channels' in info['pug']:
                prevChannelData = info['pug']['channels']
            info['pug']['channels'] = {}
            activeModes = []
            for channelId, mode, pug in self.getAllActivePugs():
                channel = discord.Client.get_channel(self.bot, channelId)
                if channel is None:
                    continue
                # Ensure channel entry exists in config
                if str(channelId) not in info['pug']['channels']:
                    info['pug']['channels'][str(channelId)] = {}
                # Create pug state for this mode
                activeModes.append(pug.mode.upper())
                pug_cfg = {
                    'timesaved': str(datetime.now().isoformat()),
                    'signed': [],
                    'active': True,
                    'mode': pug.mode,
                    'modelimit': pug.modeLimit,
                    'playerlimit': pug.maxPlayers,
                    'maxmaps': pug.maps.maxMaps,
                    'server': pug.gameServer.gameServerRef
                }
                if len(pug.players) > 0:
                    pug_cfg['signed'] = [p.id for p in pug.all if p not in [None]]
                pug_cfg['lastpug'] = {}
                if pug.lastPugTimeStarted: # legacy
                    pug_cfg['lastpug']['timestarted'] = pug.lastPugTimeStarted.isoformat()
                if pug.lastPugStr: # legacy
                    pug_cfg['lastpug']['pugstr'] = pug.lastPugStr
                if pug.lastPug not in [None,'',{}] and 'serveraddr' in pug.lastPug:
                    pug_cfg['lastpug'] = pug.lastPug

                # Store under mode key
                info['pug']['channels'][str(channelId)][mode] = pug_cfg
            pug = self.getPugForChannel(self.activeChannel.id)
            if pug and pug.gameServer.utQueryConsoleWatermark > 0:
                info['pug']['reporterconsolewatermark'] = pug.gameServer.utQueryConsoleWatermark
            else:
                info['pug']['reporterconsolewatermark'] = 0
            for channelId in prevChannelData:
                for mode in prevChannelData[channelId]:
                    if mode.upper() not in activeModes:
                        prevChannelData[channelId][mode]['active'] = False
                        info['pug']['channels'][str(channelId)][mode] = prevChannelData[channelId][mode]
                        
        with open(configFile, 'w') as f:
            json.dump(info, f, indent=4)
        return True
   
    #########################################################################################
    # Formatted strings:
    #########################################################################################

    def format_pick_next_player(self, mention: bool = False, pug = None, mode = ''):
        if pug is None:
            if mode not in (None, '') and mode.upper() in map(str.upper, MODE_CONFIG):
                pug = self.getPugForModeInChannel(self.activeChannel.id, mode)
            else:
                pug = self.getPugForChannel(self.activeChannel.id)
        player = pug.currentCaptainToPickPlayer
        return f'{player.mention if mention else display_name(player)} to pick next player (**!pick <number>**)'

    def format_pick_next_map(self, mention: bool = False, pug = None, mode = ''):
        if pug is None:
            if mode not in (None, '') and mode.upper() in map(str.upper, MODE_CONFIG):
                pug = self.getPugForModeInChannel(self.activeChannel.id, mode)
            else:
                pug = self.getPugForChannel(self.activeChannel.id)
        player = pug.currentCaptainToPickMap
        return f'{player.mention if mention else display_name(player)} to pick next map (use **!map <number>** to pick and **!listmaps** to view available maps)'

    #########################################################################################
    # Functions:
    #########################################################################################

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        pug = self.getPugForChannel(self.activeChannel.id)
        if type(error) is PugIsInProgress:
            # To handle messages returned when disabled commands are used when pug is already in progress.
            msg = ['A match is currently in progress.']
            if ctx.message.author in pug:
                msg.append(f'{ctx.message.author.mention},  please, join the match or find a sub.')
                msg.append('If the match has just ended, please, wait at least 60 seconds for the pug to reset.')
            else:
                msg.append('Pug will reset when it is finished; otherwise use !jq <mode> to join the queue for the next game.')
            await ctx.send('\n'.join(msg))

    def isActiveChannel(self, ctx):
        """Check if channel has any active pugs (any modes).
        
        In the new mode-based architecture, this checks if the channel has
        any pug instances regardless of mode and sets the active channel.
        """
        channel = ctx.message.channel
        if channel.id in self.pugInstances and self.pugInstances[channel.id]:
            self.setActiveChannel(channel)
            return True
        return False
    
    async def checkOnDemandServer(self, ctx, pug=None):
        if pug is None:
            pug = self.getPugForChannel(self.activeChannel.id)
        if pug.gameServer.gameServerState in ('N/A','N/AN/A') and pug.gameServer.gameServerOnDemand is True:
            await ctx.send(f'Starting on-demand server: {pug.gameServer.gameServerName}...')
            info = pug.gameServer.controlOnDemandServer('start')
            if (info):
                log.info(f'On-demand server start {pug.gameServer.gameServerName} returned: {info["cloudManagementResponse"]}')
                return True
            else:
                log.error(f'Failed to start on-demand server: {pug.gameServer.gameServerName}')
                await ctx.send(f'Failed to start on-demand server: {pug.gameServer.gameServerName}. Select another server before completing map selection.')
                return False
        return True

    async def processPugStatus(self, ctx, pug=None):
        # Big function to test which stage of setup we're at:
        if pug == None:
            pug = self.getPugForChannel(self.activeChannel.id)

        if not pug.playersFull:
            # Not filled, nothing to do.
            return
        holdMessage = f'[**{pug.mode}**] Match is currently on hold while another game is in progress on the selected server or with the selected players.'
        # Work backwards from match ready.
        # Note match is ready once players are full, captains picked, players picked and maps picked.
        if pug.mapsReady and pug.matchReady:
            if pug.pugTempLocked > 1:
                if ctx is not None:
                    await ctx.send(holdMessage)
                return
            elif pug.pugTempLocked > 0:
                # Avoid repeating a setup when multiple conditions are true
                return
            if pug.ranked:
                msg = '\n'.join([f'[**{pug.mode}**] Ranked mode map selection complete. Setting up match now.',pug.maps.format_current_maplist])
                pug.pugTempLocked = 1
                self.ratingsLock = True
                await ctx.send(msg)
            if pug.gameServer.gameServerOnDemand and not pug.gameServer.gameServerOnDemandReady:
                if ctx is not None:
                    await ctx.send(f'Waiting for {pug.gameServer.gameServerName} to be ready for action...')
            setupPug = pug.setupPug()
            if setupPug[0]:
                await self.sendPasswordsToTeams(self.activeChannel.id, pug.mode)
                if ctx is not None:
                    await ctx.send(f'[**{pug.mode}**] {pug.format_match_is_ready}')
                pug.gameServer.utQueryConsoleWatermark = pug.gameServer.format_new_watermark
                pug.gameServer.utQueryData = {}
                pug.gameServer.utQueryReporterActive = True
                pug.gameServer.utQueryStatsActive = True
                self.resetRequestRed = False # only need to reset this here because we only care about this when a match is in progress.
                self.resetRequestBlue = False # only need to reset this here because we only care about this when a match is in progress.
                self.pushMultiInstancePlayers(self.activeChannel.id, pug.mode) # Move any players from other pugs in the same channel to temp storage while match is in progress
            else:
                if setupPug[1].lower() == 'locked':
                    pug.pugTempLocked = 2 # enforce long temporary lock
                    if ctx is not None:
                        await ctx.send(holdMessage)
                else:
                    if ctx is not None:
                        await ctx.send(f'[**{pug.mode}**] **PUG Setup Failed**. Use **!retry** to attempt setting up again with current configuration, or **!reset** to start again from the beginning.')
            return

        if pug.teamsReady:
            # Need to pick maps.
            if (pug.ranked):
                pug.maps.autoPickRankedMaps()
                if pug.maps.mapListWeighting is not None:
                    pug.ratings['maps']['maplist'] = pug.maps.mapListWeighting
                await self.processPugStatus(ctx, pug) # loop back around
            else:
                await ctx.send(self.format_pick_next_map(mention=True, pug=pug))
            return
        
        if pug.captainsReady:
            # Special case to display captains on the first pick.
            if len(pug.red) == 1 and len(pug.blue) == 1:
                await ctx.send(pug.red[0].mention + f' is captain for the {pug.mode} **Red Team**')
                await ctx.send(pug.blue[0].mention + f' is captain for the {pug.mode} **Blue Team**')
            # Need to pick players.
            msg = '\n'.join([
                pug.format_remaining_players(number=True),
                pug.format_teams(),
                self.format_pick_next_player(mention=True, pug=pug)])
            await ctx.send(msg)
            # Check server state and fire a start-up command if needed
            await self.checkOnDemandServer(ctx)
            return
        
        if pug.numCaptains == 1:
            # Need second captain.
            await ctx.send(f'[**{pug.mode}**] Waiting for 2nd captain. Type **!captain** to become a captain. To choose a random captain type **!randomcaptains**')
            return

        if pug.playersReady:
            if pug.ranked:
                # Logic is reversed, fill teams and then nominate random captains
                costmsg = pug.makeRatedTeams()
                msg = '\n'.join([f'[**{pug.mode}**] Ranked teams have been established:',pug.format_teams(),costmsg])
                await ctx.send(msg)
                await self.checkOnDemandServer(ctx)
                await self.processPugStatus(ctx, pug) # loop back around
                return
            else:
                # Need captains.
                msg = [f'**{pug.desc}** has filled.']
                if len(pug) == 2 and pug.playersFull:
                    # Special case, 1v1: assign captains instantly, so jump straight to map picks.
                    pug.setCaptain(pug.players[0])
                    pug.setCaptain(pug.players[1])
                    await ctx.send(f'[**{pug.mode}**] Teams have been automatically filled.\n{pug.format_teams(mention=True)}')
                    await ctx.send(f'{self.format_pick_next_map(mention=False, pug=pug)}')
                    # Check server state and fire a start-up command if needed
                    await self.checkOnDemandServer(ctx)
                    return
                # Standard case, moving to captain selection.
                msg.append(pug.format_pug(mention=True))
                # Need first captain
                msg.append(f'[**{pug.mode}**] Waiting for captains. Type **!captain** to become a captain. To choose random captains type **!randomcaptains**')
            await ctx.send('\n'.join(msg))
            return

    async def sendPasswordsToTeams(self, channelId, mode):
        pug = self.getPugForChannel(channelId, mode)
        if pug.matchReady:
            msg_redPassword = pug.gameServer.format_red_password
            msg_redServer = pug.gameServer.format_gameServerURL_red
            msg_bluePassword = pug.gameServer.format_blue_password
            msg_blueServer = pug.gameServer.format_gameServerURL_blue
            if pug.channelId is not None:
                channel = discord.Client.get_channel(self.bot, pug.channelId)
            else:
                channel = self.activeChannel
            for player in pug.red:
                try:
                    await player.send(f'{msg_redPassword}\nJoin the server @ **{msg_redServer}**')
                except:
                    await channel.send(f'Unable to send password to {player.mention} - are DMs enabled? Please ask your teammates for the red team password.')
            for player in pug.blue:
                try:
                    await player.send(f'{msg_bluePassword}\nJoin the server @ **{msg_blueServer}**')
                except:
                    await channel.send(f'Unable to send password to {player.mention} - are DMs enabled? Please ask your teammates for the blue team password.')
        if channel:
            await channel.send('Check private messages for server passwords.')
        return True

    async def isPugInProgress(self, ctx, warn: bool=False):
        if not self.isActiveChannel(ctx):
            return False
        pug = self.getPugForChannel(self.activeChannel.id)
        if warn and pug.pugLocked:
            log.warning('Raising PugIsInProgress')
            raise PugIsInProgress('Pug In Progress')
        return not pug.pugLocked
    
    async def queryServerConsole(self):
        # Fetch watermark from previous messages
        pug = self.getPugForChannel(self.activeChannel.id)
        consoleWatermark = pug.gameServer.utQueryConsoleWatermark
        reportToChannel = self.utReporterChannel
        # Fetch console log
        if pug.gameServer.utQueryServer('consolelog') and reportToChannel is not None:
            if 'code' in pug.gameServer.utQueryData and pug.gameServer.utQueryData['code'] == 200:
                if 'consolelog' in pug.gameServer.utQueryData:
                    bReportScoreLine = False
                    # Attempt to serialize to JSON, otherwise if server doesn't support this, use simple string manipulation
                    try:
                        utconsole = json.loads(pug.gameServer.utQueryData['consolelog'])
                    except:
                        utconsole = {}
                        utconsole['messages'] = str(pug.gameServer.utQueryData['consolelog']).split('|')

                    for m in utconsole['messages']:
                        try:
                            # Message format: {"stamp":"20220101133700666", "type":"Say", "gametime":"120", "displaytime":"02:00", "message": ":robot::guitar:", "teamindex":"0", "team":"Red", "player":"Sizzl"}
                            if 'message' in m and 'stamp' in m and int(m['stamp']) > pug.gameServer.utQueryConsoleWatermark:
                                if 'type' in m and m['type'] == 'Say':
                                    for em in self.customStaticEmojis:
                                        m['message']  = re.compile(em).sub(f'<{em}{self.customStaticEmojis[em]}>', m['message'])
                                    for em in self.customAnimatedEmojis:
                                        m['message']  = re.compile(em).sub(f'<a{em}{self.customAnimatedEmojis[em]}>', m['message'])
                                    if 'team' in m:
                                        if m['team'] == 'Spectator':
                                            await reportToChannel.send(f'[{m["displaytime"]}] {m["player"].strip()} (*{m["team"]}*): {m["message"].strip()}')
                                        else:
                                            await reportToChannel.send(f'[{m["displaytime"]}] {m["player"].strip()} (**{m["team"]}**): {m["message"].strip()}')
                                    else:
                                        await reportToChannel.send(f'[{m["displaytime"]}] {m["player"].strip()}: {m["message"].strip()}')
                                else:
                                    if re.search('1\sminutes\suntil\sgame\sstart|conquered\sthe\sbase|defended\sthe\sbase',m['message'],re.IGNORECASE) is not None:
                                        bReportScoreLine = True
                                    if len(m['message'].strip()) > 0:
                                        await reportToChannel.send(f'[{m["displaytime"]}] {m["message"].strip()}')
                                consoleWatermark = int(m['stamp'])
                        except:
                            try:
                                # Message format: 20220101133700666 [13:37] Player: Message
                                # We won't do any fancy replacements here, just drop the message verbatim.
                                stamp = int(m[:17])
                            except:
                                stamp = 0
                            if stamp > pug.gameServer.utQueryConsoleWatermark:
                                await reportToChannel.send(f'{m[-(len(m)-18):]}')
                                if re.search('1\sminutes\suntil\sgame\sstart|conquered\sthe\sbase|defended\sthe\sbase',m,re.IGNORECASE) is not None:
                                    bReportScoreLine = True
                            if stamp > 0:
                                consoleWatermark = stamp
                            else:
                                consoleWatermark = pug.gameServer.format_new_watermark
                    pug.gameServer.utQueryConsoleWatermark = consoleWatermark

                    if pug.gameServer.utQueryStatsActive is False:
                        # Picking up a deferred stats request (from bReportScoreLine)
                        await self.queryServerStats(cacheonly=False, pug=pug)
                        # Reset the requirement for scoreline and re-enable the infrequent stats embed
                        bReportScoreLine = False
                        pug.gameServer.utQueryStatsActive = True

                    if bReportScoreLine:
                        # Defer a scoreline report to the next cycle of this function by disabling the infrequent stats embed
                        pug.gameServer.utQueryStatsActive = False
            elif 'code' in pug.gameServer.utQueryData and pug.gameServer.utQueryData['code'] == 408 and pug.pugLocked == False:
                pug.gameServer.utQueryStatsActive = False
                pug.gameServer.utQueryReporterActive = False
        return True

    async def queryServerStats(self, cacheonly: bool=False, pug=None):
        if pug is None:
            pug = self.getPugForChannel(self.activeChannel.id)
        spacer = "\u2800"*3
        embedInfo = discord.Embed(color=discord.Color.greyple(),title=pug.gameServer.format_current_serveralias,description='Waiting for server info...')
        # Send "info" to get basic server details and confirm online
        if pug.gameServer.utQueryServer('info'):
            if 'code' in pug.gameServer.utQueryData and pug.gameServer.utQueryData['code'] == 200:
                if cacheonly is False:
                    # Rate-limit reporter-channel stats cards to one a minute, even after an on-demand stats call
                    pug.gameServer.utQueryData['laststats'] = int(time.time())

                # Send multi-query request for lots of info
                if pug.gameServer.utQueryServer('status\\\\level_property\\timedilation\\\\game_property\\teamscore\\\\game_property\\teamnamered\\\\game_property\\teamnameblue\\\\player_property\\Health\\\\game_property\\elapsedtime\\\\game_property\\remainingtime\\\\game_property\\bmatchmode\\\\game_property\\friendlyfirescale\\\\game_property\\currentdefender\\\\game_property\\bdefenseset\\\\game_property\\matchcode\\\\game_property\\fraglimit\\\\game_property\\timelimit\\\\rules'):
                    queryData = pug.gameServer.utQueryData
                    log.debug(queryData)

                    # Build embed data
                    summary = {
                        'Colour': discord.Color.greyple(),
                        'Title': 'Pug Match',
                        'RoundStatus': '',
                        'Map': '',
                        'Objectives': '',
                        'Hostname': '',
                        'PlayerCount': ''
                    }
                    for x in range(0,4):
                        summary[f'PlayerList{x}'] = '*(No players)*'
                        summary[f'PlayerList{x}_data'] = ''
                    summary['PlayerList255'] = '*(No Spectators)*'
                    summary['PlayerList255_data'] = ''
                    # Pick out generic UT info
                    if 'hostname' in queryData:
                        if 'mutators' in queryData and re.search('Lag\sCompensator',str(queryData['mutators']),re.IGNORECASE) is not None:
                            summary['Title'] = summary['Hostname'] = queryData['hostname'].replace('| StdAS |','| lcAS |')
                        else:
                            summary['Title'] = summary['Hostname'] = queryData['hostname'].replace('| iAS | zp|','| zp-iAS |')
                    if 'mapname' in queryData:
                        embedInfo.set_thumbnail(url=f'{pug.gameServer.thumbnailServer}{str(queryData["mapname"]).lower()}.jpg')
                        summary['Map'] = queryData['mapname']
                    if 'remainingtime' in queryData:
                        summary['RemainingTime'] = f'{str(time.strftime("%M:%S",time.gmtime(int(queryData["remainingtime"]))))}'
                    elif 'elapsedtime' in queryData:
                        summary['ElapsedTime'] = f'{str(time.strftime("%M:%S",time.gmtime(int(queryData["elapsedtime"]))))}'
                    elif 'timelimit' in queryData and int(queryData['timelimit']) > 0:
                        summary['TimeLimit'] = f'{int(queryData["timelimit"])}:00'
                    if 'maptitle' in queryData:
                        summary['Map'] = queryData['maptitle']
                    if 'numplayers' in queryData and 'maxplayers' in queryData:
                        summary['PlayerCount'] = f'{queryData["numplayers"]}/{queryData["maxplayers"]}'
                        if 'maxteams' in queryData and int(queryData['numplayers']) > 0:
                            for x in range(int(queryData['numplayers'])):
                                if f'player_{x}' in queryData:
                                    player = {}
                                    player['Name'] = queryData[f'player_{x}'].replace('`','').strip()
                                    if len(player['Name']) > 14:
                                        player['Name'] = f'{player["Name"][:12]}...'.strip()
                                    player['Frags'] = '0'
                                    if f'frags_{x}' in queryData:
                                        player['Frags'] = queryData[f'frags_{x}'].strip()                                                                                        
                                    player['Ping'] = '0'
                                    if f'ping_{x}' in queryData:
                                        player['Ping'] = queryData[f'ping_{x}'].strip()
                                        if len(str(player['Ping'])) > 3:
                                            player['Ping'] = '---'
                                    if f'team_{x}' in queryData:
                                        player['TeamId'] = queryData[f'team_{x}']
                                    team_id = player['TeamId']
                                    player_list_key = f'PlayerList{team_id}_data'
                                    if player['TeamId'] == '255':
                                        summary[player_list_key] = f'{summary[player_list_key]}\n{player["Name"].ljust(15)}{"".rjust(5)}{player["Ping"].rjust(4)}'
                                    else:
                                        summary[player_list_key] = f'{summary[player_list_key]}\n{player["Name"].ljust(15)}{player["Frags"].rjust(5)}{player["Ping"].rjust(4)}'

                                    for x in range(int(queryData['maxteams'])):
                                        key = f'PlayerList{x}'
                                        data_key = f'PlayerList{x}_data'
                                        if summary[data_key] not in ['',None]:
                                            summary[key] = f'```Player Name{spacer}\t Score Ping'
                                            summary[key] = f'{summary[key]}{summary[data_key]}\n```'
                            
                            if summary['PlayerList255_data'] not in ['',None]:
                                summary['PlayerList255'] = f'```Name       {spacer}\t       Ping'
                                summary['PlayerList255'] = f'{summary["PlayerList255"]}{summary["PlayerList255_data"]}\n```'

                    # Set basic embed info
                    embedInfo.color = summary['Colour']
                    embedInfo.title = summary['Title']
                    embedInfo.description = f'```unreal://{queryData["ip"]}:{queryData["game_port"]}```'

                    if 'password' in queryData and queryData['password'] == 'True' and pug.gameServer.format_gameServerURL==f'unreal://{queryData["ip"]}:{queryData["game_port"]}':
                        embedInfo.set_footer(text=f'Spectate @ {pug.gameServer.format_gameServerURL}/?password={pug.gameServer.spectatorPassword}')

                    # Pick out info for UTA-only games
                    if 'bmatchmode' in queryData and 'gametype' in queryData and queryData['gametype'] == 'Assault':
                        # Send individual requests for objectives and UTA-enhanced team info, refresh local variable
                        pug.gameServer.utQueryServer('objectives')
                        pug.gameServer.utQueryServer('teams')
                        queryData = pug.gameServer.utQueryData
    
                        if 'AdminName' in queryData and queryData['AdminName'] not in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                            # Match mode is active
                            if 'score_0' in queryData and 'score_1' in queryData:
                                if queryData['score_0'] > queryData['score_1']:
                                    summary['Colour'] = discord.Color.red()
                                elif queryData['score_0'] < queryData['score_1']:
                                    summary['Colour'] = discord.Color.blurple()
                                if 'teamnamered' in queryData and 'teamnameblue' in queryData:
                                    summary['Title'] = f'{pug.desc} | {queryData["teamnamered"]} {queryData["score_0"]} - {queryData["score_1"]} {queryData["teamnameblue"]}'
                                else:
                                    summary['Title'] = f'{pug.desc} | RED {queryData["score_0"]} - {queryData["score_1"]} BLUE'
                            summary['Hostname'] = f'```unreal://{queryData["ip"]}:{queryData["game_port"]}```'
                        elif 'AdminName' in queryData and queryData['AdminName'] in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                            summary['Hostname'] = f'```unreal://{queryData["ip"]}:{queryData["game_port"]}```'
                        # Build out round info
                        if 'bdefenseset' in queryData and 'currentdefender' in queryData:
                            if queryData['bdefenseset'] in ['true','True','1']:
                                summary['RoundStatus'] = '2/2'
                            else:
                                summary['RoundStatus'] = '1/2'
                            if queryData['currentdefender'] == '1':
                                if 'teamnamered' in queryData and queryData['AdminName'] not in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                                    summary['RoundStatus'] = f'{summary["Hostname"]}\tRound {summary["RoundStatus"]}; {queryData["teamnamered"]} attacking'
                                else:
                                    summary['RoundStatus'] = f'{summary["Hostname"]}\tRound {summary["RoundStatus"]}; Red Team attacking'
                            else:
                                if 'teamnameblue' in queryData and queryData['AdminName'] not in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                                    summary['RoundStatus'] = f'{summary["Hostname"]}\tRound {summary["RoundStatus"]}; {queryData["teamnameblue"]} attacking'
                                else:
                                    summary['RoundStatus'] = f'{summary["Hostname"]}\tRound {summary["RoundStatus"]}; Blue Team attacking'
                        if 'fortcount' in queryData:
                            summary['Objectives'] = ''
                            for x in range(int(queryData['fortcount'])):
                                if x == 0:
                                    summary['Objectives'] = f' \t {str(queryData[f"fort_{x}"])} - {str(queryData[f"fortstatus_{x}"])}'
                                else:
                                    summary['Objectives'] = f'{summary["Objectives"]}\n \t {str(queryData[f"fort_{x}"])} - {str(queryData[f"fortstatus_{x}"])}'
                        # Build out embed card with UTA enhanced information
                        embedInfo.color = summary['Colour']
                        embedInfo.title = summary['Title']
                        embedInfo.description = summary['RoundStatus']
                        embedInfo.add_field(name='Map',value=summary['Map'],inline=True)
                        embedInfo.add_field(name='Players',value=summary['PlayerCount'],inline=True)
                        if 'RemainingTime' in summary:
                            embedInfo.add_field(name='Time Left',value=summary['RemainingTime'],inline=True)
                        embedInfo.add_field(name='Objectives',value=summary['Objectives'],inline=False)
                    else:
                        # No UTA enhanced information available, report basic statistics
                        queryData = pug.gameServer.utQueryData
                        embedInfo.add_field(name='Map',value=summary['Map'],inline=True)
                        embedInfo.add_field(name='Players',value=summary['PlayerCount'],inline=True)
                        if 'RemainingTime' in summary:
                            embedInfo.add_field(name='Time Left',value=summary['RemainingTime'],inline=True)
                        elif 'ElapsedTime' in summary:
                            embedInfo.add_field(name='Time Elapsed',value=summary['ElapsedTime'],inline=True)
                        elif 'TimeLimit' in summary:
                            embedInfo.add_field(name='Time Limit',value=summary['TimeLimit'],inline=True)
                        elif 'goalteamscore' in queryData and int(queryData['goalteamscore']) > 0:
                            embedInfo.add_field(name='Req. Team Score',value=queryData['goalteamscore'],inline=True)
                        elif 'fraglimit' in queryData and int(queryData['fraglimit']) > 0:
                            embedInfo.add_field(name='Frag Limit',value=queryData['fraglimit'],inline=True)
                        elif 'gametype' in queryData:
                            embedInfo.add_field(name='Mode',value=queryData['gametype'],inline=True)
                    if 'numplayers' in queryData and int(queryData['numplayers']) > 0:
                        embedInfo.add_field(name='Red Team',value=summary['PlayerList0'],inline=False)
                        embedInfo.add_field(name='Blue Team',value=summary['PlayerList1'],inline=False)

                    if summary['PlayerList255_data'] != '':
                        embedInfo.add_field(name='Spectators',value=summary['PlayerList255'],inline=False)

                    if cacheonly is False:
                        await self.utReporterChannel.send(embed=embedInfo)
                # Store the embed data for other functions to use
                pug.gameServer.utQueryEmbedCache = embedInfo.to_dict()

        if ('code' not in pug.gameServer.utQueryData) or ('code' in pug.gameServer.utQueryData and pug.gameServer.utQueryData['code'] > 400):
            # Server offline
            embedInfo.color = discord.Color.darker_gray()
            if pug.gameServer.gameServerOnDemand is True:
                embedInfo.description = f'```{pug.gameServer.format_gameServerURL}```\nOn-demand server is currently offline. Start a !pug to use this server.'
                pug.gameServer.utQueryEmbedCache = embedInfo.to_dict()
            else:
                pug.gameServer.utQueryEmbedCache = {} # fall back to old method
        return True

    def cacheGuildEmojis(self):
        if self.activeChannel is not None:
            for x in self.activeChannel.guild.emojis:
                if x.animated:
                    self.customAnimatedEmojis[f':{x.name}:'] = x.id
                else:
                    self.customStaticEmojis[f':{x.name}:'] = x.id

    def getPlayerPreferences(self, player: int):
        """Gets the preferences for a given player
        
        Args:
            player: ID of the Discord user
            
        Returns:
            tuple (mode, maps)
        """
        if self.configLoadTime == 0:
            # Config not loaded yet, can't get preferences.
            return False
        if str(player) not in self.playerPreferences:
            log.debug(f'getPlayerPreferences({player}) - Discord user preferences not present.')
            return False
        log.debug(f'getPlayerPreferences({player}) - returning tuple: {str(self.playerPreferences[str(player)])}')
        return self.playerPreferences[str(player)]
    
    def setPlayerPreferences(self, player: int, mode: str = '', maps: str = '', save: bool = False):
        """Sets the preferences for a given player
        
        Args:
            player: ID of the Discord user
            mode: preferred mode
            maps: preferred maplist
            save: forces a config save
            
        Returns:
            bool: True (where successful), False (where unsuccessful)
        """
        if self.configLoadTime == 0:
            # Config not loaded yet, can't set preferences.
            return False
        if str(player) not in self.playerPreferences:
            log.debug(f'getPlayerPreferences({player}) - Discord user preferences not present; creating entry')
            self.playerPreferences[str(player)] = {
                'mode': '',
                'maps': []
            }
        self.playerPreferences[str(player)]['mode'] = ''
        modemaps = None
        if (len(mode) > 0) and mode.upper() in map(str.upper, MODE_CONFIG):
            mode = next((key for key, _ in MODE_CONFIG.items() if key.upper()==mode.upper()), None)
            self.playerPreferences[str(player)]['mode'] = mode
            targetPug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
            if targetPug is not None:
                modemaps = targetPug.gameServer.maps.filteredMapsList.copy()
        if len(maps) > 0:
            maplist = maps.replace(';',',').replace('/',',').split(',')
            validmaps = []
            if modemaps is None:
                modemaps = self._defaultPugInfo.gameServer.configMaps.copy()
            for m in maplist:
                if len(m) > 1:
                    validmap = None
                    if m.upper() in modemaps:
                        validmap = next((key for key, _ in modemaps if key.upper()==m.upper()), None)
                    else:
                        pattern = re.compile(f'.*{re.escape(m)}.*', re.IGNORECASE)
                        filtermaps = list(filter(pattern.match, modemaps))
                        if len(filtermaps) == 1:
                            validmap = filtermaps[0]
                    if validmap not in ['',None]:
                        validmaps.append(validmap)
            self.playerPreferences[str(player)]['maps'] = validmaps

        if save:
            self.savePugConfig(self.configFile)
        return True

    def ratingsMatchInfo(self, mode, matchCode: str = ''):
        matchInfo = {}
        if self.ratingsLock != True:
            self.savePugRatings(self.ratingsFile)
        rkData = self.loadPugRatings(self.ratingsFile, True)
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    if 'games' in x:
                        if matchCode == 'last':
                            matchInfo = sorted(x['games'], key=lambda g: datetime.fromisoformat(g['startdate']), reverse=True)[0]
                        else:
                            for g in x['games']:
                                if g['gameref'].upper() == matchCode.upper():
                                    matchInfo = g
        return matchInfo
    
    def ratingsMatchReport(self, mode, teamRed: list = [], teamBlue: list = [], matchref: str = '', playerid: int = 0):
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        cards = []
        embedInfo = discord.Embed(color=discord.Color.greyple(),title=f'Ranked mode {mode} match report',description='')
        if len(matchref) == 0 and playerid == 0:
            if len(pug.gameServer.matchCode):
                matchref = pug.gameServer.matchCode
            elif len(pug.gameServer.lastMatchCode):
                matchref = pug.gameServer.lastMatchCode
        if len(matchref) > 0:
            embedInfo.description = f'Match reference: `{matchref}`'
            matchInfo = self.ratingsMatchInfo(mode,matchref)
            if matchInfo == {}:
                embedInfo.title = 'Match not found'
                return embedInfo
            else:
                matchref = matchInfo['gameref']
                embedInfo.description = f'Match reference: `{matchref}` ([stats]({DEFAULT_STATS_MATCH_URL}{matchref}))'
                started = datetime.fromisoformat(matchInfo['startdate'])
                embedInfo.add_field(name='Match started',value=started.strftime('%d/%b/%Y @ %H:%M'),inline=True)
                if matchInfo['completed']:
                    ended = datetime.fromisoformat(matchInfo['enddate'])
                    embedInfo.add_field(name='Match ended',value=ended.strftime('%d/%b/%Y @ %H:%M'),inline=True)
                    embedInfo.add_field(name='Duration',value=f'{getDuration(started,ended,"minutes")} mins',inline=True)
                    embedInfo.color = discord.Color.brand_green()
                else:
                    embedInfo.add_field(name='Status',value='Incomplete / Void',inline=True)
                embedInfo.add_field(name="Map list",value=PLASEP.join(matchInfo['maplist']),inline=False)
                embedInfo.add_field(name="Team Power",value=f'Red {matchInfo["rpred"]} - {matchInfo["rpblue"]} Blue')
                embedInfo.add_field(name="Score",value=f'Red {matchInfo["scorered"]} - {matchInfo["scoreblue"]} Blue')
            cards.append(embedInfo)
            if 'teamred' in matchInfo:
                teamRed = matchInfo['teamred']
            if 'teamblue' in matchInfo:
                teamBlue = matchInfo['teamblue']
        if len(teamRed) > 0:
            embedInfo = discord.Embed(color=discord.Color.red(),title=f'Team Red player ratings {GRAPHUP}',description='')
            if matchInfo['scorered'] < matchInfo['scoreblue']:
                embedInfo.title = embedInfo.title+GRAPHDN
            elif matchInfo['scorered'] > matchInfo['scoreblue']:
                embedInfo.title = embedInfo.title+GRAPHUP
            else:
                embedInfo.title = embedInfo.title
            report = self.ratingsPlayerReport(mode=mode,players=teamRed,matchref=matchref)
            if 'cap_name' in report and report['cap_name'] not in [None,'']:
                embedInfo.add_field(name='Captain',value=f'{report["cap_name"]}',inline=True)
                embedInfo.add_field(name='Power',value=f'{report["cap_rp"]}',inline=True)
                embedInfo.description = ''
            if 'players' in report and report['players'] not in [None,'']:
                #embedInfo.add_field(name="\u200B", value="\u200B")
                #embedInfo.add_field(name='Players',value='{0}'.format(report['players']),inline=True)
                #embedInfo.add_field(name='Power',value='{0}'.format(report['players_rp']),inline=True)
                embedInfo.add_field(name='Player ratings',value=f'{report["players_sum"]}',inline=False)
                embedInfo.description = ''
            if embedInfo.description != 'Data not found':
                cards.append(embedInfo)
        if len(teamBlue) > 0:
            embedInfo = discord.Embed(color=discord.Color.blurple(),title='Team Blue player ratings ',description='Data not found')
            if matchInfo['scorered'] > matchInfo['scoreblue']:
                embedInfo.title = embedInfo.title+GRAPHDN
            elif matchInfo['scorered'] < matchInfo['scoreblue']:
                embedInfo.title = embedInfo.title+GRAPHUP
            else:
                embedInfo.title = embedInfo.title
            report = self.ratingsPlayerReport(mode=mode,players=teamBlue,matchref=matchref)
            if 'cap_name' in report and report['cap_name'] not in [None,'']:
                embedInfo.add_field(name='Captain',value=f'{report["cap_name"]}',inline=True)
                embedInfo.add_field(name='Power',value=f'{report["cap_rp"]}',inline=True)
                embedInfo.description = ''
            if 'players' in report and report['players'] not in [None,'']:
                #embedInfo.add_field(name="\u200B", value="\u200B")
                #embedInfo.add_field(name='Players',value='{0}'.format(report['players']),inline=True)
                #embedInfo.add_field(name='Power',value='{0}'.format(report['players_rp']),inline=True)
                embedInfo.add_field(name='Player ratings',value=f'{report["players_sum"]}',inline=False)
                embedInfo.description = ''
            if embedInfo.description != 'Data not found':
                cards.append(embedInfo)
        if playerid > 0:
            embedInfo = discord.Embed(color=discord.Color.greyple(),title='Player rating history',description='Data not found')
            report = self.ratingsPlayerReport(mode=mode,playerid=playerid)
            if 'player_name' in report and report['player_name'] not in [None,'']:
                embedInfo.description = f'Ratings history for {report["player_name"]}'
                embedInfo.add_field(name='Last game',value=f'{report["player_last"]}',inline=False)
                if len(report['player_hist']):
                    embedInfo.add_field(name=f'Previous {report["player_hist_count"]} game(s)',value=f'{report["player_hist"]}',inline=False)
            if embedInfo.description != 'Data not found':
                cards.append(embedInfo)
        return cards
    
    def ratingsPlayerReport(self, mode, players: list = [], matchref: str = '', playerid: int = 0):
        def updn(score1,score2):
            if score1 > score2:
                status = UP
            elif score2 > score1:            
                status = DN
            else:
                status = MODSEP                    
            return status
        capSummary = ''
        report = {'cap_name':'','cap_rp':'','players':'','players_rp':'','players_sum':'','player_name':'','player_last':'','player_hist':'','player_hist_count': 0}
        if len(players) > 0:
            cap = self.ratingsPlayerDataHandler('rkget', mode, players[0])
            if cap['lastgameref'].upper() == matchref.upper() or len(matchref) == 0:
                capSummary = f'RP: {cap["ratingvalue"]} {updn(cap["ratingvalue"],cap["ratingprevious"])} Previous RP: {cap["ratingprevious"]}'
            else:
                for h in cap['ratinghistory']:
                    if h['matchref'].upper() == matchref.upper() or len(matchref) == 0:
                        capSummary = f'RP before: {h["ratingbefore"]} {updn(h["ratingafter"],h["ratingbefore"])} RP after: {h["ratingafter"]}; Current RP: {cap["ratingvalue"]}'
            report['cap_name'] = cap['dlastnick']
            report['cap_rp'] = capSummary
        elif playerid > 0:
            player = self.ratingsPlayerDataHandler('rkget', mode, playerid)
            report['player_name'] = player['dlastnick']
            if len(player['lastgamedate']) > 0:
                g_startdate = datetime.fromisoformat(player['lastgamedate']).strftime('%d/%b/%Y @ %H:%M')
                if player['lastgameref'] == 'admin-set':
                    if player['ratingprevious'] == 0:
                        pSummary = f'Admin seeded rating of **{player["ratingvalue"]}** on {g_startdate}\n'
                    else:
                        pSummary = f'Admin set rating on {g_startdate}: RP before: **{player["ratingprevious"]}** {updn(player["ratingvalue"],player["ratingprevious"])} RP after: **{player["ratingvalue"]}**\n'
                else:
                    matchInfo = self.ratingsMatchInfo(mode, player['lastgameref'])
                    if matchInfo != {}:
                        if playerid in matchInfo['teamred']:
                            pteam = 'Red'
                        else:
                            pteam = 'Blue'
                        if matchInfo['capred']['id'] == playerid or matchInfo['capblue']['id'] == playerid:
                            pteam = pteam+', captain'
                        if (matchInfo['completed']):
                            pSummary = f'Match: `{player["lastgameref"]}` @ {g_startdate}\n> Team: {pteam}\n> Score: Red {matchInfo["scorered"]} - {matchInfo["scoreblue"]} Blue;\n> RP before: **{player["ratingprevious"]}** {updn(player["ratingvalue"],player["ratingprevious"])} RP after: **{player["ratingvalue"]}**\n'
                        else:
                            pSummary = f'Match: `{player["lastgameref"]}` @ {g_startdate}\n> Team: {pteam}\n> Status: Incomplete / voided match\n'
                    else:
                        pSummary = f'Match: `{player["lastgameref"]}` @ {g_startdate}\n> RP before: **{player["ratingprevious"]}** {updn(player["ratingvalue"],player["ratingprevious"])} RP after: **{player["ratingvalue"]}**\n'
            else:
                g_startdate = datetime.fromisoformat(player['ratingdate']).strftime('%d/%b/%Y')
                pSummary = f'Seed rating, set on {g_startdate}: **{player["ratingvalue"]}**\n'
            report['player_last'] = pSummary
            pSummary = ''
            if 'ratinghistory' in player:
                history = sorted(player['ratinghistory'], key=lambda g: datetime.fromisoformat(g['matchdate']), reverse=True)
                i = 0
                for h in history:
                    if i < 5:
                        g_startdate = datetime.fromisoformat(h['matchdate']).strftime('%d/%b/%Y @ %H:%M')
                        if h['matchref'] == 'admin-set':
                            if h['ratingbefore'] == 0:
                                pSummary = f'{pSummary}Admin seeded rating of **{h["ratingafter"]}** on {g_startdate}\n'
                            else:
                                pSummary = f'{pSummary}Admin set rating on {g_startdate}\n> RP before: **{h["ratingbefore"]}** {updn(h["ratingafter"],h["ratingbefore"])} RP after: **{h["ratingafter"]}**\n'
                        else:
                            matchInfo = self.ratingsMatchInfo(mode, h['matchref'])
                            if matchInfo != {}:
                                if playerid in matchInfo['teamred']:
                                    pteam = 'Red'
                                else:
                                    pteam = 'Blue'
                                if (matchInfo['completed']):
                                    pSummary = f'{pSummary}Match: `{h["matchref"]}` @ {g_startdate}\n> Team: {pteam}\n> Score: Red {matchInfo["scorered"]} - {matchInfo["scoreblue"]} Blue\n> RP before: **{h["ratingbefore"]}** {updn(h["ratingafter"],h["ratingbefore"])} RP after: **{h["ratingafter"]}**\n'
                                else:
                                    pSummary = f'{pSummary}Match: `{h["matchref"]}` @ {g_startdate}\n> Team: {pteam}\n> Status: Incomplete / voided match\n'
                            else:
                                pSummary = f'{pSummary}Match: `{h["matchref"]}` @ {g_startdate}\n> RP before: **{h["ratingbefore"]}** {updn(h["ratingafter"],h["ratingbefore"])} RP after: **{h["ratingafter"]}**\n'
                        i += 1
            report['player_hist'] = pSummary
            report['player_hist_count'] = i
        if len(players) > 1:
            for p in players[1:]:
                player = self.ratingsPlayerDataHandler('rkget',mode,p)
                report['players'] = report['players']+player['dlastnick']+'\n'
                report['players_sum'] = report['players_sum']+'**'+player['dlastnick']+'**\n> '
                if player['lastgameref'].upper() == matchref.upper() or len(matchref) == 0:
                    pSummary = f'RP: {player["ratingvalue"]} {updn(player["ratingvalue"],player["ratingprevious"])} Previous RP: {player["ratingprevious"]}\n'
                    report['players_rp'] = report['players_rp']+pSummary
                    report['players_sum'] = report['players_sum']+pSummary
                else:
                    for h in player['ratinghistory']:
                        if h['matchref'].upper() == matchref.upper() or len(matchref) == 0:
                            pSummary = f'RP before: {h["ratingbefore"]} {updn(h["ratingafter"],h["ratingbefore"])} RP after: {h["ratingafter"]}; Current RP: {player["ratingvalue"]}\n'
                            report['players_rp'] = report['players_rp']+pSummary
                            report['players_sum'] = report['players_sum']+pSummary
        return report
    
    def ratingsPlayerDataHandler(self, action, mode, player, rating: int = 0, toggle: bool = False, additionalid: int = 0):
        log.debug('ratingsPlayerDataHandler() reloading data from JSON file')
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if self.ratingsLock != True:
            self.savePugRatings(self.ratingsFile)
        rkData = self.loadPugRatings(self.ratingsFile, True)
        rkReload = False
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' not in x:
                    rkReload = True
                elif pug is not None and 'mode' in x and pug.ranked and pug.mode.upper() == mode.upper() and pug.ratings not in [None,'']:
                    x = pug.ratings # stamp cached ratings back
        if rkReload:
            self.savePugRatings(self.ratingsFile)
            rkData = self.loadPugRatings(self.ratingsFile, True)
        if type(player) is int:
            pid = player
            pdn = ''
            dplayer = self.activeChannel.guild.get_member(pid)
        elif type(player) is str:
            pid = -1
            pdn = player
        else:
            pid = player.id
            pdn = player.display_name
            dplayer = player
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode'] # update formatting
                    if action == 'rkget' or action == 'rkrecalc':
                        msg = f'Error - Player not registered for ranked games in {mode}'
                        if pid in x['registrations'] or pid < 0:
                            for r in x['ratings']:
                                if r['did'] == pid or (pid < 0 and 'dlastnick' in r and str(r['dlastnick']).lower() == pdn.lower()):
                                    if action == 'rkget':
                                        return r
                                    elif action == 'rkrecalc':
                                        if pid < 0: 
                                            pid = r['did']
                                        admsets = []
                                        log.debug(f'ratingsPlayerDataHandler(rkrecalc) - Recalculating rank for {r["dlastnick"]}')
                                        if rating == 0:
                                            # Find seed rating
                                            if r['ratingprevious'] == 0:
                                                rating = r['ratingvalue']
                                                rating_date = r['ratingdate']
                                        else:
                                            rating_date = (datetime.fromisoformat(r['ratingdate']) - timedelta(minutes=5)).isoformat()
                                        if 'ratinghistory' in r:
                                            history = sorted(r['ratinghistory'], key=lambda g: datetime.fromisoformat(g['matchdate']))
                                        if len(history):
                                            for h in history:
                                                if h['matchref'] == 'admin-set':
                                                    if rating == 0:
                                                        rating = h['ratingafter']
                                                        rating_date = h['matchdate']
                                                    admsets.append(h)
                                        if len(r['lastgameref']) == 0 or r['lastgameref'] == 'admin-set':
                                            admsets.append({
                                                'matchref': r['lastgameref'],
                                                'matchdate': r['ratingdate'],
                                                'ratingbefore': r['ratingprevious'],
                                                'ratingafter': r['ratingvalue']
                                            })
                                        if rating == 0:
                                            return f'Initial seed rating could not be found for {r["dlastnick"]}, please provide a seed rating.'
                                        log.debug(f'ratingsPlayerDataHandler({mode}) - Seed rating for {r["dlastnick"]} = {rating}')
                                        r['ratinghistory'] = admsets # Reset player rating history to admin-set only
                                        msg = ''
                                        r['ratingdate'] = rating_date
                                        r['ratingvalue'] = rating
                                        r['ratingprevious'] = 0
                                        matches = sorted(x['games'], key=lambda g: datetime.fromisoformat(g['startdate']))
                                        g_last = None
                                        for m in matches:
                                            if pid in m['teamred'] or pid in m['teamblue']:
                                                if len(msg) == 0:
                                                    g_date = datetime.fromisoformat(rating_date).strftime('%d/%b/%Y @ %H:%M')
                                                    msg = f'RP recalculated for {r["dlastnick"]}:\n> Seed rating on {g_date}: **{rating}**\n'
                                                if m['completed']:
                                                    # Determine whether rating has been adjusted before sending to recalc
                                                    for aset in admsets:
                                                        if len(r['lastgamedate']) == 0:
                                                            r['lastgamedate'] = r['ratingdate']
                                                        if datetime.fromisoformat(aset['matchdate']) > datetime.fromisoformat(r['lastgamedate']) and datetime.fromisoformat(m['startdate']) > datetime.fromisoformat(aset['matchdate']):
                                                            log.debug(f'ratingsPlayerDataHandler({mode}) - admin adjusted rating present between matches (last game: {r["lastgamedate"]}; update date: {aset["matchdate"]}; next match started: {m["startdate"]}). Adjusting seed for {r["dlastnick"]} to {aset["ratingafter"]} without adjusting history')
                                                            r['ratingdate'] = aset['matchdate']
                                                            r['ratingprevious'] = r['ratingvalue']
                                                            r['ratingvalue'] = aset['ratingafter']
                                                            g_date = datetime.fromisoformat(aset['matchdate']).strftime('%d/%b/%Y %H:%M')
                                                            msg = f'{msg}> Admin updated @ {g_date}: RP before: **{r["ratingprevious"]}**; RP after: **{r["ratingvalue"]}**\n'
                                                    if pid in m['teamred']:
                                                        pteam = 'Red'
                                                    else:
                                                        pteam = 'Blue'
                                                    if m['capred']['id'] == pid or m['capblue']['id'] == pid:
                                                        pteam = pteam+', captain'
                                                    g_last = datetime.fromisoformat(m['startdate'])
                                                    g_date = g_last.strftime('%d/%b/%Y %H:%M')
                                                    log.debug(f'ratingsPlayerDataHandler({mode}) - {r["dlastnick"]} present in match {m["gameref"]} - calculating RP')
                                                    if pug is not None:
                                                        rk = pug.applyRankedScoring(x, mode=mode, match=m, player=pid)
                                                        if 'lastgameref' in rk and 'ratingprevious' in rk:
                                                            log.debug(f'ratingsPlayerDataHandler({mode}) - Updated player data from applyRankedScoring() for match: {m["gameref"]} = {rk["lastgameref"]}; RP before: {rk["ratingprevious"]}, RP after: {rk["ratingvalue"]}')
                                                        else:
                                                            log.debug(f'ratingsPlayerDataHandler({mode}) - Updated player data from applyRankedScoring() for match: {m["gameref"]} = (unknown). rk = {str(rk)}')
                                                        if 'did' in rk and rk['did'] == pid:
                                                            r = rk
                                                        msg = f'{msg}> Match: `{r["lastgameref"]}` @ {g_date} (team {pteam}); Score: Red {m["scorered"]} - {m["scoreblue"]} Blue. RP before: **{r["ratingprevious"]}**; RP after: **{r["ratingvalue"]}**\n'
                                                    else:
                                                        msg = f'{msg}> Failed to apply ranked update for match `{m["gameref"]}`. PUG could not be found.'
                                                else:
                                                    log.debug(f'ratingsPlayerDataHandler({mode}) - {r["dlastnick"]} present in voided/incomplete match {m["gameref"]} - ignoring RP. Last completed game: {r["lastgameref"]} on {r["lastgamedate"]}')
                                        if len(r['ratinghistory']):
                                            r['ratinghistory'] = sorted(r['ratinghistory'], key=lambda g: datetime.fromisoformat(g['matchdate'])) 
                                            r['ratinghistory'] = r['ratinghistory'][:-1]
                                        for aset in admsets:
                                            if g_last != None:
                                                if g_last < datetime.fromisoformat(aset['matchdate']) and (len(aset['matchref']) == 0 or aset['matchref'] == 'admin-set'):
                                                    r['ratinghistory'].append({
                                                        'matchref': r['lastgameref'],
                                                        'matchdate': r['lastgamedate'],
                                                        'ratingbefore': r['ratingprevious'],
                                                        'ratingafter': r['ratingvalue']
                                                    })
                                                    r['lastgameref'] = aset['matchref']
                                                    r['lastgamedate'] = aset['matchdate']
                                                    r['ratingprevious'] = r['ratingvalue']
                                                    r['ratingvalue'] = aset['ratingafter']
                                                    msg = f'{msg}> Admin updated @ {g_date}: RP before: **{r["ratingprevious"]}**; RP after: **{r["ratingvalue"]}**\n'
                                                    g_last = datetime.fromisoformat(aset['matchdate'])
                                        if len(r['ratinghistory']):
                                            r['ratinghistory'] = sorted(r['ratinghistory'], key=lambda g: datetime.fromisoformat(g['matchdate']))
                                            last = r['ratinghistory'][-1]
                                            if last['matchref'] == r['lastgameref'] and last['matchdate'] == r['lastgamedate']:
                                                r['ratinghistory'] = r['ratinghistory'][:-1]
                                        if len(msg) == 0:
                                            msg = self.ratingsPlayerDataHandler('rkset',mode,player,rating)
                    elif action == 'rkset':
                        if pid > -1 and pid not in x['registrations']:
                            x['registrations'].append(pid) # register player as eligible
                            log.debug(f'{action}({pid},{mode},{rating},{toggle}) - registering new pid for {pdn}.')
                        rkUpdate = False
                        for r in x['ratings']:
                            if r['did'] == pid:
                                log.debug(f'{action}({pid},{mode},{rating},{toggle}) - updating existing rank data for {pdn}.')
                                if len(pdn):
                                    r['dlastnick'] = pdn
                                if (additionalid > 0 or 'externalpid' not in r):
                                    r['externalpid'] = additionalid
                                r['ratingdate'] = datetime.now().isoformat()
                                if 'ratinghistory' in r:
                                    if r['ratinghistory'] in [None,'']:
                                        r['ratinghistory'] = []
                                else:
                                    r['ratinghistory'] = []
                                if len(r['lastgameref']) == 0:
                                    r['lastgameref'] = 'admin-set'
                                if len(r['lastgamedate']) == 0:
                                    r['lastgamedate'] = r['ratingdate']
                                r['ratinghistory'].append({
                                    'matchref': r['lastgameref'],
                                    'matchdate': r['lastgamedate'],
                                    'ratingbefore': r['ratingprevious'],
                                    'ratingafter': r['ratingvalue']
                                })
                                #if len(r['ratinghistory']) > 150:
                                #     r['ratinghistory'][:] = r['ratinghistory'][-150:]
                                r['ratingprevious'] = r['ratingvalue']
                                r['ratingvalue'] = rating
                                r['lastgamedate'] = r['ratingdate']
                                r['lastgameref'] = 'admin-set'
                                rkUpdate = True
                        if rkUpdate == False and pid > -1: # new entry required
                            log.debug(f'{action}({pid},{mode},{rating},{toggle}) - adding new rank data for {pdn}.')
                            x['ratings'].append({
                                'did': pid,
                                'dlastnick': pdn,
                                'externalpid': additionalid,
                                'ratingdate': datetime.now().isoformat(),
                                'ratingprevious': 0,
                                'ratingvalue': rating,
                                'ratinghistory': [],
                                'lastgamedate': datetime.now().isoformat(),
                                'lastgameref': 'admin-set'
                            })
                            # Message player with registration details
                            if dplayer != None and additionalid < 1:
                                dplayer.send(f'Welcome. You have been registered for ranked play in {mode}.\n\nYour UTA account could not be automatically reconciled, please link your Discord ID with your UTA account, or register a new UTA account at {DEFAULT_ACCOUNT_URL} to continue.')
                        if pid > -1:
                            msg = f'Rank configured with a rating of {rating} for {pdn} (id:{pid}) in game mode {mode}'
                        else:
                            msg = f'Player ID could not be established for {pdn}'
                    elif action == 'rkdel':
                        if pid in x['registrations']:
                            x['registrations'].remove(pid)
                        for r in x['ratings']:
                            if r['did'] == pid:
                                x['ratings'].remove(r)
                        if pid > -1:
                            msg = f'Ranked player rating removed for {pdn} (id:{pid}) in game mode {mode}'
                        else:
                            msg = f'Player ID could not be established for {pdn}'
                    else:
                        msg = 'Unsupported action called.'
        if self.savePugRatings(self.pugInfo.ratingsFile, rkData):
            log.debug(f'ratingsPlayerDataHandler({mode}) - saved updated ratings')
            if (pug is not None and pug.ranked): # reload data for current ranked mode
                pug.setRankedMode(pug.ranked, True)
                log.debug(f'ratingsPlayerDataHandler({mode}) - loaded ratings back into memory')
        else:
                msg = 'Error - rank data could not be saved; check bot logs.'
        if action == 'rkget':
            msg = None
        return msg

    def ratingsSync(self, endpoint: str = '', body: str = '', authkey: str = '', restrict: bool = False, delay: int = 0):
        if restrict and (datetime.now() - self.lastAPISyncTime).total_seconds() < delay: # 5 second delay between requests when restricted.
            log.debug(f'API request throttled. Last API sync: {self.lastAPISyncTime.strftime("%d/%m/%Y %H:%M:%S")}')
            return None
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'X-API-Secret': authkey,
            'Accept':'application/json'
        }
        if len(body):
            log.debug('Sending API request, fetching sync API data...')
            r = self.pugInfo.gameServer.makePostRequest(endpoint, headers)
        else:
            log.debug('Sending API request, sending sync API data...')
            r = self.pugInfo.gameServer.makePostRequest(endpoint, headers, body)
        self.lastAPISyncTime = datetime.now()
        if(r):
            try:
                validatedJSON = r.json()
                log.debug('Sync API response validated.')
                return validatedJSON
            except:
                log.error(f'Invalid JSON returned from Sync API, URL: {r.url} HTTP response: {r.status_code}; content:{r.content}')
                return {}
        else:
            return {}

    #########################################################################################
    # Bot Admin ONLY commands.
    #########################################################################################
    @commands.hybrid_command(aliases=['enable','create'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def pugenable(self, ctx, mode: str):
        """Enables PUG commands in this channel. Admin only."""
        if ctx.message.channel.id in self.pugInstances:
            if (len(mode) > 0) and mode.upper() in map(str.upper, MODE_CONFIG):
                self.getPugForChannel(ctx.message.channel.id,mode)
                msg = f'Pug Instance established - {mode} in {ctx.message.channel.mention}'
            else:
                await ctx.send(f'PUG commands are already enabled in {ctx.message.channel.mention}. Specify a valid mode to establish a new queue.')
                return
        else:
            self.setActiveChannel(ctx.message.channel)
            msg = f'PUG commands are enabled in {ctx.message.channel.mention}'
        self.savePugConfig(self.configFile)
        await ctx.send(msg)

    @commands.hybrid_command(aliases=['limitmodes'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def modelimit(self, ctx, modeGroup: str = ''):
        """Limits which modes can be selected in this channel. Admin only."""
        if ctx.message.channel.id in self.pugInstances:
            self.setActiveChannel(ctx.message.channel)
            pug = self.getPugForChannel(ctx.message.channel.id)
            if modeGroup.upper() == 'ALL' or int(modeGroup) == 0:
                pug.modeLimit = 0
                await ctx.send(f'Mode limit removed in {ctx.message.channel.mention}.')
                self.savePugConfig(self.configFile)
                return
            elif int(modeGroup) > 0:
                pug.modeLimit = int(modeGroup)
            await ctx.send(f'Mode limit set to group \'{modeGroup}\' in {ctx.message.channel.mention}')
            self.savePugConfig(self.configFile)
            return
        await ctx.send('This is not an active PUG channel.')

    @commands.command(aliases=['aa','aap','adminaddplayer'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminadd(self, ctx, mode:str = '', *players: discord.Member): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Adds a player to the pug. Admin only"""
        failed = False
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
        showList = False
        for player in players:
            if targetPug.ranked:
                if targetPug.checkRankedPlayersEligibility([player]):
                    if not targetPug.addRankedPlayer(player):
                        failed = True
                        if targetPug.playersReady:
                            await ctx.send(f'Cannot add {display_name(player)}: Ranked pug is already full.')
                        else:
                            await ctx.send(f'Cannot add {display_name(player)}: They are already signed.')
                    else:
                        await ctx.send(f'{display_name(player)} is elgible for ranked play and was added by an admin.')
                        showList = True
                else:
                    failed = True
                    await ctx.send(f'Cannot add {display_name(player)}: They are inelgible to join a ranked pug.')
            else:
                if not targetPug.addPlayer(player):
                    failed = True
                    if targetPug.playersReady:
                        await ctx.send(f'Cannot add {display_name(player)}: Pug is already full.')
                    else:
                        await ctx.send(f'Cannot add {display_name(player)}: They are already signed.')
                else:
                    await ctx.send(f'{display_name(player)} was added by an admin.')
                    showList = True
        if showList:
            await self.listpugs(ctx)
        if not failed:
            await self.processPugStatus(ctx, targetPug)

    @commands.command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminremove(self, ctx, mode:str = '', *players: discord.Member): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Removes a player from the pug. Admin only"""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(ctx.message.channel.id)
        if targetPug.pugLocked:
            await ctx.send('Match is in progress, players cannot be removed at this time.')
            return
        for player in players:
            if targetPug.removePlayerFromPug(player):
                await ctx.send(f'**{display_name(player)}** was removed by an admin.')
            else:
                await ctx.send(f'{display_name(player)} is not in the pug.')
        await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command(aliases=['setserver','setactiveserver'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminsetserver(self, ctx, mode:str = '', idx: int=0):
        """!setserver <mode> <#>. Sets the active server to the index chosen from the pool of available servers"""
        svindex = idx - 1 # offset as users see them 1-based index.
        if mode.upper() not in map(str.upper, MODE_CONFIG):
            await ctx.send('Invalid mode specified. Please specify a valid mode to set the server for.')
            return
        targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        if targetPug == None:
            await ctx.send('No active pug found for the specified mode in this channel.')
            return
        if targetPug.pugLocked != True and targetPug.gameServer.useServer(svindex,targetPug.captainsReady): # auto start eligible servers when caps are ready
            await ctx.send(f'Server was activated by an admin for {mode} - {targetPug.gameServer.format_current_serveralias}.')
            targetPug.gameServer.utQueryConsoleWatermark = targetPug.gameServer.format_new_watermark
            if targetPug.gameServer.gameServerState in ('N/A','N/AN/A'):
                # Check whether server is being changed when captains are already ready
                if not targetPug.captainsReady:
                    await ctx.send('Server is currently offline, but will be fired up upon Captains being selected.')

            # Bit of a hack to get around the problem of a match being in progress when this is initialised. - TODO consider off state too
            # Will improve this later.
            if targetPug.gameServer.lastSetupResult == 'Match In Progress':
                targetPug.pugLocked = True
            else:
                tempLocked = False
                for activeMode, pug in self.pugInstances[ctx.message.channel.id].items():
                    if mode != activeMode:
                        if pug.gameServer.gameServerRef == targetPug.gameServer.gameServerRef:
                            if pug.matchReady or pug.pugLocked:
                                targetPug.pugTempLocked = 2 # set long-lock on the mode being altered due to server clash against already in-progress game
                                tempLocked = True
                            else:
                                pug.pugTempLocked = 2 # set long-lock on other pug due to same server being used
                                tempLocked = True
                if not tempLocked:
                    pass # consider differentiating between locks for players and servers by iterating over tempQueuedPlayers
                await self.processPugStatus(ctx, pug=targetPug)
        else:
            await ctx.send(f'Selected server **{idx}** could not be activated.')
    
    @commands.hybrid_command(aliases=['startserver'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminstartserver(self, ctx, idx: int):
        """Starts up an on-demand server. Admin only"""
        previousRef = self.pugInfo.gameServer.gameServerRef
        svindex = idx - 1 # offset as users see them 1-based index.
        if self.pugInfo.gameServer.useServer(svindex, True):
            await ctx.send(f'**{self.pugInfo.gameServer.gameServerName}** is starting up (allow up to 60s).')
        else:
            await ctx.send(f'Selected server **{idx}** could not be activated.')
        self.pugInfo.gameServer.useServer(-1, True,previousRef) # return to active server
        return True

    @commands.hybrid_command(aliases=['stopserver'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminstopserver(self, ctx, idx: int):
        """Queues up an on-demand server to shut down. Admin only"""
        svindex = idx - 1 # offset as users see them 1-based index.
        if self.pugInfo.gameServer.stopOnDemandServer(svindex):
            if len(self.pugInfo.gameServer.allServers[svindex][1]) > 0:
                await ctx.send(f'**{self.pugInfo.gameServer.allServers[svindex][1]}** is queued for shut-down.')
        else:
            await ctx.send(f'Selected server **{idx}** could not be activated.')

    @commands.hybrid_command(aliases=['refreshservers'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminrefreshservers(self, ctx):
        """Refreshes the server list within the available pool. Admin only"""
        if self.pugInfo.gameServer.validateServers():
            if len(self.pugInfo.gameServer.gameServerRotation) > 0:
                await ctx.send('Server list refreshed. Check whether the server rotation is still valid.')
            else:
                await ctx.send('Server list refreshed.')
        else:
            await ctx.send('Server list could not be refreshed.')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminremoveserver(self, ctx, svref: str):
        # Removed add server in favour of pulling from API; left remove server in here in case one needs temporarily removing until restart
        """Removes a server from available pool. Admin only"""
        if self.pugInfo.gameServer.removeServerReference(svref):
            await ctx.send('Server was removed from the available pool by an admin.')
        else:
            await ctx.send('Server could not be removed. Is it even in the list?')
    
    @commands.command(aliases=['setrotation','rotate'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminsetserverrotation(self, ctx, *rotation: str): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Rotates servers weekly based on the provided servers. Admin only"""
        tempRotation = self.pugInfo.gameServer.gameServerRotation
        self.pugInfo.gameServer.gameServerRotation = []
        for index in rotation:
            if index.isdigit() and (int(index) > 0 and int(index) <= len(self.pugInfo.gameServer.allServers)):
                self.pugInfo.gameServer.gameServerRotation.append(int(index))
        # Reset to previous selection if given rotation was invalid
        if self.pugInfo.gameServer.gameServerRotation == [] and tempRotation != []:
            self.pugInfo.gameServer.gameServerRotation = tempRotation
            await ctx.send('Server rotation unchanged.')
        else:
            self.pugInfo.gameServer.saveServerConfig(self.pugInfo.gameServer.configFile)
            await ctx.send(f'Server rotation set to: {", ".join(map(str, self.pugInfo.gameServer.gameServerRotation))}')

    @commands.hybrid_command(aliases=['checkrotation','checkrotate'])
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def checkserverrotation(self, ctx):
        """Checks current server and rotates accordingly."""
        tempRotation = self.pugInfo.gameServer.gameServerRef
        if len(self.pugInfo.gameServer.gameServerRotation) > 0:
            self.pugInfo.gameServer.checkServerRotation()
            if self.pugInfo.gameServer.gameServerRef != tempRotation:
                await ctx.send(f'Server rotation changed server to: {self.pugInfo.gameServer.format_current_serveralias}.')
            else:
                await ctx.send('Server is already correctly set.')
        else:
            await ctx.send('Server rotation is not configured.')

    @commands.hybrid_command(aliases=['getrotation'])
    @commands.check(isActiveChannel_Check)
    async def getserverrotation(self, ctx):
        """Shows server rotation."""
        if len(self.pugInfo.gameServer.gameServerRotation) > 0:
            thisWeek = int(self.pugInfo.gameServer.gameServerRotation[int('{:0}{:0>2}'.format(datetime.now().year,datetime.now().isocalendar()[1]))%len(self.pugInfo.gameServer.gameServerRotation)])
            nextWeek = int(self.pugInfo.gameServer.gameServerRotation[int('{:0}{:0>2}'.format((datetime.now()+timedelta(weeks=1)).year,(datetime.now()+timedelta(weeks=1)).isocalendar()[1]))%len(self.pugInfo.gameServer.gameServerRotation)])
            await ctx.send('Server rotation:')
            for x in self.pugInfo.gameServer.gameServerRotation:
                svindex = int(x)-1
                if svindex >= 0 and svindex < len(self.pugInfo.gameServer.allServers):
                    if (thisWeek == int(x)):
                        await ctx.send(f' - {self.pugInfo.gameServer.allServers[svindex][1]} :arrow_forward: This week')
                        thisWeek = -1
                    elif (nextWeek == int(x)):
                        await ctx.send(f' - {self.pugInfo.gameServer.allServers[svindex][1]} :fast_forward: Next week')
                        nextWeek = -1
                    else:
                        await ctx.send(f' - {self.pugInfo.gameServer.allServers[svindex][1]}')
        else:
            await ctx.send('Server rotation is not configured.')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminaddmap(self, ctx, map: str):
        """Adds a map to the available map list. Admin only"""
        if self.pugInfo.maps.addMapToAvailableList(map):
            self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile, self.pugInfo.maps.availableMapsList)
            await ctx.send(f'**{map}** was added to the available maps by an admin. The available maps are now:\n{self.pugInfo.maps.format_available_maplist}')
        else:
            await ctx.send(f'**{map}** could not be added. Is it already in the list?')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def admininsertmap(self, ctx, index: int, map: str):
        """Insert a map into the available map list at the given index. Admin only"""
        if index > 0 and index <= self.pugInfo.maps.maxMapsLimit + 1:
            offset_index = index - 1 # offset as users see them 1-based index
            if self.pugInfo.maps.insertMapIntoAvailableList(offset_index, map):
                self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile, self.pugInfo.maps.availableMapsList)
                await ctx.send(f'**{map}** was inserted into the available maps by an admin. The available maps are now:\n{self.pugInfo.maps.format_available_maplist}')
            else:
                await ctx.send(f'**{map}** could not be inserted. Is it already in the list?')
        else:
            await ctx.send('The valid format of this command is, for example: !admininsertmap # AS-MapName, where # is in the range (1, NumMaps + 1).')

    @commands.command()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminreplacemap(self, ctx, *mapref: str): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Replaces a map within the available map list. Admin only"""
        if len(mapref) == 2 and mapref[0].isdigit() and (int(mapref[0]) > 0 and int(mapref[0]) <= len(self.pugInfo.maps.availableMapsList)):
            index = int(mapref[0]) - 1 # offset as users see in a 1-based index; the range check is performed before it gets here
            map = mapref[1]
            oldmap = self.pugInfo.maps.availableMapsList[index]
            if self.pugInfo.maps.substituteMapInAvailableList(index, map):
                self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile, self.pugInfo.maps.availableMapsList)
                await ctx.send(f'**{map}** was added to the available maps by an admin in position #{mapref[0]}, replacing {oldmap}. The available maps are now:\n{self.pugInfo.maps.format_available_maplist}')
            else:
                await ctx.send(f'**{map}** could not be added in slot {mapref[0]}. Is it already in the list? Is the position valid?')
        else:
            await ctx.send('The valid format of this command is, for example: !adminreplacemap # AS-MapName, where # is in the range (1, NumMaps).')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def adminremovemap(self, ctx, map: str):
        """Removes a map to from available map list. Admin only"""
        if map.isdigit():
            index = int(map) - 1 # offset as users see in a 1-based index
            mapNameToRemove = self.pugInfo.maps.getMapFromAvailableList(index)
        else:
            mapNameToRemove = map
        if self.pugInfo.maps.removeMapFromAvailableList(mapNameToRemove):
            self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile,self.pugInfo.maps.availableMapsList)
            await ctx.send(f'**{mapNameToRemove}** was removed from the available maps by an admin.\n{self.pugInfo.maps.format_available_maplist}')
        else:
            await ctx.send(f'**{map}** could not be removed. Is it in the list?')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def passwords(self, ctx):
        """Provides current game passwords to the requesting administrator. Admin only"""
        if self.isPugInProgress:
            await ctx.message.author.send(f'For the game currently running at {self.pugInfo.gameServer.format_gameServerURL}')
            await ctx.message.author.send(f'{self.pugInfo.gameServer.format_red_password} - {self.pugInfo.gameServer.format_blue_password}')
            await ctx.send('Check your private messages!')
        else:
            await ctx.send('There is no game in progress.')
    
    # Ranked mode commands
    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rksave(self, ctx):
        """Calls for a configuration save"""
        if self.ratingsLock:
            await ctx.send(f'A ranked match is already underway at {self.pugInfo.gameServer.format_gameServerURL}')
            await ctx.send('Please try again after the match has concluded.')
        else:
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            await ctx.send('Rank configuration saved.')
        return True

    @commands.hybrid_command(aliases=['setrk','rankset','addrk','rankadd','rkadd'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkset(self, ctx, player: discord.Member, mode: str = MODE_RANKED_DEFAULT, rating: int = 500, externalpid: int = 0):
        """Adds or sets a player rating within a game mode: PlayerNick GameMode(e.g. rASPlus) Weight(e.g., 500)"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock:
            await ctx.send(f'A ranked match is already underway at {self.pugInfo.gameServer.format_gameServerURL}')
            await ctx.send('Please try again after the match has concluded.')
        else:
            msg = self.ratingsPlayerDataHandler('rkset',mode,player,rating,False,externalpid)
            await ctx.send(msg)
        return True
    
    @commands.hybrid_command(aliases=['rmrk','rkrm','rkdelete','rankdel','rankremove'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkdel(self, ctx, player: discord.Member, mode: str = MODE_RANKED_DEFAULT):
        """Removes a player rating within a game mode: PlayerNick GameMode(e.g. rASPlus)"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock:
            await ctx.send(f'A ranked match is already underway at {self.pugInfo.gameServer.format_gameServerURL}')
            await ctx.send('Please try again after the match has concluded.')
        else:
            msg = self.ratingsPlayerDataHandler('rkdel',mode,player,0)
            await ctx.send(msg)
        return True
    
    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rksync(self, ctx, mode: str = MODE_RANKED_DEFAULT, item: str = '', direction: str = 'outbound', redcap: discord.Member = None, bluecap: discord.Member = None):
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if (pug.pugLocked or self.ratingsLock):
            await ctx.send('Ranked data cannot be synchronised while a game is in progress. Please try again later.')
            return True
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if (item in [None,'']):
            await ctx.send('A valid match reference or object type must be specified.')
            return True
        if pug.ranked and pug.mode.upper() == mode.upper():
            mode = pug.mode
        matchInfo = self.ratingsMatchInfo(mode,item)
        if matchInfo == {} and direction == 'outbound':
            await ctx.send('The provided valid match reference could not be found for outbound sync.')
        elif matchInfo != {} and direction == 'outbound':
            started = datetime.fromisoformat(matchInfo['startdate'])
            await ctx.send(f'Synchronising match `{matchInfo["gameref"]}` played on {started.strftime("%d/%m/%Y")} at {started.strftime("%H:%M:%S")}...')
            self.savePugRatings(pug.ratingsFile)
            rk = pug.loadPugRatings(pug.ratingsFile, True)
            if 'rankedgames' in rk:
                for x in rk['rankedgames']:
                    if 'games' in x and 'mode' in x and x['mode'].upper() == mode.upper():
                        for g in x['games']:
                            if 'gameref' in g and g['gameref'].upper() == item.upper():
                                log.debug(f'rksync() - Found match data in rk; completed={g["completed"]}')
            # pug.savePugRatings(pug.ratingsFile,rk)
            # pug.setRankedMode(MODE_CONFIG[pug.mode].isRanked, False)
        elif direction == 'inbound':
            if item in ['all','matches','player','players']:
                log.debug(f'rksync() - Batch fetching data from API: {item}')
                # await ctx.send('{0} {1} data from {2}...'.format('Fetching',item,'Sync API'))
                await ctx.send(f'Synchronisation of {item} data for `{mode}` has not yet been implemented.')
            else:
                if pug.ranked:
                    endpoint = f'{pug.ratingsSyncAPI["matchDataURL"]}?&matchcode={item}'
                else:
                    rkData = pug.loadPugRatings(pug.ratingsFile, True)
                    endpoint = f'{rkData["syncapi"]["matchDataURL"]}?&matchcode={item}'
                log.debug(f'rksync() - Fetching provided match from API: {endpoint}')
                await ctx.send(f'Fetching match `{item}` from {pug.ratingsSyncAPI["matchDataURL"]}...')
                syData = self.ratingsSync(endpoint, body='', restrict=True, delay=5)
                if syData not in [{},None,''] and 'match_summary' in syData:
                    log.debug('rksync() - Match data fetched and valid.')
                    g_start = datetime.strptime(str(syData['time_start']),'%Y%m%d%H%M%S').isoformat()
                    g_end = datetime.strptime(str(syData['time_end']),'%Y%m%d%H%M%S').isoformat()
                    g_maps = []
                    g_red = []
                    g_blue = []
                    g_red_rp = 0
                    g_blue_rp = 0
                    invalid_players = []
                    for s in syData['match_summary']:
                        if 'did' in s and int(s['did']) > 0:
                            if 'team' in s and s['team'] == 'Red':
                                g_red.append(s['did'])
                                p = self.ratingsPlayerDataHandler('rkget', mode, s['did'])
                                if p not in [None,'',{}] and 'ratingvalue' in p:
                                    g_red_rp += int(p['ratingvalue'])
                                else:
                                    log.debug(f'rksync() - Player lookup failed for ID: {s["did"]}')
                            elif 'team' in s and s['team'] == 'Blue':
                                g_blue.append(s['did'])
                                p = self.ratingsPlayerDataHandler('rkget', mode, s['did'])
                                if p not in [None,'',{}] and 'ratingvalue' in p:
                                    g_blue_rp += int(p['ratingvalue'])
                                else:
                                    log.debug(f'rksync() - Player lookup failed for ID: {s["did"]}')
                        else:
                            if 'teamcode' in s and s['teamcode'] > 250:
                                log.debug(f'rksync() - Player spectating: {s["playername"]}')
                            else:
                                invalid_players.append(s['playername'])
                    if redcap not in ['',None] and redcap.id in g_red:
                        log.debug(f'rksync() - Overriding Red captain from: {g_red[0]}, to: {redcap.id}')
                        g_red.remove(redcap.id)
                        g_red.insert(0,redcap.id)
                    if bluecap not in ['',None] and bluecap.id in g_blue:
                        log.debug(f'rksync() - Overriding Blue captain from: {g_blue[0]}, to: {bluecap.id}')
                        g_blue.remove(bluecap.id)
                        g_blue.insert(0,bluecap.id)
                    for m in syData['maps']:
                        if 'map' in m and m['map'] not in g_maps:
                            g_maps.append(m['map'])
                    if len(invalid_players):
                        log.debug(f'rksync() - Abandonded sync of match data for {item} due to invalid players found.')
                        await ctx.send(f'Could not sync match `{item}` for mode {mode}. One or more invalid or unregistered players found: {", ".join(invalid_players)}')
                        return True
                    else:
                        log.debug(f'rksync() - Preparing to store match data for {item}.')
                        if pug.storeRankedPug(mode=mode, matchCode=item, redScore=syData['score_red'], blueScore=syData['score_blue'], timeStarted=g_start, hasEnded=False, redPlayers=g_red, bluePlayers=g_blue, maps=g_maps, redPower=g_red_rp, bluePower=g_blue_rp, timeEnded=g_end):
                            log.debug('rksync() - Stored match successfully.')
                            pug.setRankedMode(MODE_CONFIG[pug.mode].isRanked, False)
                            if len(g_red) == len(g_blue):
                                log.debug('rksync() - Match marked as completed successfully.')
                                await self.rkvoidmatch(ctx,mode,item)
                                await ctx.send('Match data synchronised successfully.')
                        return True
                if syData == None:
                    await ctx.send(f'Could not sync match `{item}` for mode {mode}. API requests throttled, please try again in 10s.')
                else:
                    await ctx.send(f'Could not sync match `{item}` for mode {mode}. Please check the reference is valid.')
        return True

    @commands.hybrid_command(aliases=['rankrecalc','rankcalc','rkrpcalc'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkrecalc(self, ctx, player: discord.Member, mode: str = MODE_RANKED_DEFAULT, seed: int = 0, pid: int = 0):
        """Recalculates RP of a player: PlayerNick GameMode(e.g. rASPlus) <optional seed value>"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)        
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'RP cannot be reclculated while a ranked match is already underway at {pug.gameServer.format_gameServerURL}')
        else:
            if pid > 0:
                await ctx.send(f'Recalculating RP (override ID={pid})...')
                msg = self.ratingsPlayerDataHandler('rkrecalc',mode,pid,seed)
            else:
                await ctx.send('Recalculating RP...')
                msg = self.ratingsPlayerDataHandler('rkrecalc',mode,player,seed)
            if len(msg) > 2000:
                for m in msg.split('\n'):
                    if len(m):
                        await ctx.send(m)
            else:
                await ctx.send(msg)
        return True

    @commands.hybrid_command(aliases=['rkgamesim','rksim'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def rkgamesimulation(self, ctx, player1=None, player2=None, player3=None, player4=None, player5=None, player6=None, player7=None, player8=None, player9=None, player10=None, player11=None, player12=None, player13=None, player14=None):
        """Simulates player picks for the active ranked mode. Use player:(+-)100 modifiers to test changes."""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=self.pugInfo.mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if not pug.ranked:
            await ctx.send('Ranked mode must be active for player pick simulations to occur.')
            return True
        players = []
        invalid = []
        log.debug('rkgamesimulation() - starting')
        for p in player1, player2, player3, player4, player5, player6, player7, player8, player9, player10, player11, player12, player13, player14:
            player = None
            if p not in ['', None]:
                pid = re.search(r'<@(\d*)>', p)
                if (pid):
                    player = int(pid[1])
                modifier = re.search(r'(.*):(\+{0,1})(\-{0,1})(\d{0,4})', str(p))
                adjust = 0
                override = False
                if modifier == None:
                    if player == None:
                        player = p
                    log.debug(f'rkgamesimulation() - simulation pre-processed player: {player}')
                else:
                    if player == None:
                        player = modifier[1]
                    if len(modifier[4]):
                        adjust = int(modifier[4])
                    if len(modifier[3]) == 0 and len(modifier[2]) == 0:
                        override = True
                    elif len(modifier[3]):
                        adjust = 0-adjust
                pstats = self.ratingsPlayerDataHandler('rkget', pug.mode, player)
                if pstats not in ['', None]:
                    if adjust > 0:
                        if override:
                            ratingValue = adjust
                        else:
                            ratingValue = pstats['ratingvalue']+adjust
                        log.debug(f'rkgamesimulation() - adjusted player rating for {player} from {pstats["ratingvalue"]} to {ratingValue}')
                    else:
                        ratingValue = pstats['ratingvalue']
                        log.debug(f'rkgamesimulation() - adding player {player} at RP {pstats["ratingvalue"]} to simulation')
                    players.append({
                        'id': pstats['did'],
                        'did': pstats['did'],
                        'name': pstats['dlastnick'],
                        'ratingvalue': ratingValue
                    })
                else:
                    invalid.append(player)
        if len(players) > 1 and len(players) % 2 == 0:
            msg = pug.makeRatedTeams(simulatedRatings=players)
            await ctx.send(f'Simulated player pick for {str(len(players))} provided and registered players:')
            await ctx.send(msg)
        else:
            if len(invalid):
                await ctx.send(f'Invalid player(s): {", ".join(invalid)}')
            await ctx.send('Provide two or more valid players for simulation.')
        return True

    @commands.command(aliases=['clearmaplist'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkclearmaps(self, ctx, mode: str = ''):
        """Clears all available maps from a ranked mode maplist. Parameters: GameMode"""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified to clear maps.')
            return True
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'Maps could not be cleared - a ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Please try again after the match has concluded.')
        else:
            # Save then load the current ratings data before manipulating and saving again
            pug.savePugRatings(pug.ratingsFile)
            rkData = pug.loadPugRatings(pug.ratingsFile, True)
            rkUpdate = False
            if 'rankedgames' in rkData:
                log.debug(f'rkclearmaps({mode}) - ranked games present.')
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        log.debug(f'rkclearmaps({mode}) - updating mode {x["mode"]}.')
                        mode = x['mode'] # update formatting
                        if 'maps' in x:
                            if 'maplist' in x['maps']:
                                x['maps']['maplist'] = []
                            x['fixedpicklimit'] = 0
                            x['startmapfrompick'] = 0
                        rkUpdate = True
            if rkUpdate and pug.savePugRatings(pug.ratingsFile, rkData):
                await ctx.send(f'Map list, pick limit and start map settings cleared for ranked game mode {mode}')
                if (pug.ranked): # reload data for current ranked mode
                    pug.setRankedMode(pug.ranked, True)
            else:
                await ctx.send('Error - ranked map data could not be saved; check bot logs.')
        return True

    @commands.command(aliases=['rkaddmap'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkaddmaps(self, ctx, mode: str = '', *maps: str):
        """Adds maps to a ranked mode maplist. Parameters: GameMode Map:Order:Weight"""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified to add maps.')
            return True
        if len(maps) == 0:
            await ctx.send('Provide one or more maps in the format: MapName1 MapName2:Order:Weight\nOptional parameters - **Order** represents the pick order (0 is any order) and **Weight** will apply a multiplier on chances of being picked - higher weight = higher chance of being picked, default weight is 1.')
            return True
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Maps cannot be added while a match is in progress.')
        else:
            pug.savePugRatings(pug.ratingsFile)
            rkData = pug.loadPugRatings(pug.ratingsFile, True)
            rkUpdate = False
            if 'rankedgames' in rkData:
                log.debug(f'rkaddmaps({mode},{maps}) - ranked games present.')
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        log.debug(f'rkaddmaps({mode},{maps}) - updating mode {x["mode"]}.')
                        mode = x['mode'] # update formatting
                        maplist = []
                        if 'maps' in x:
                            if 'maplist' in x['maps']:
                                maplist = x['maps']['maplist']
                        else:
                            x['maps'] = {
                                'maplist': [],
                                'fixedpicklimit': 0,
                                'startmapfrompick': 0,
                                'randomorder': True
                            }
                        for m in maps:
                            o = 0
                            w = 1
                            map = m
                            if m.find(':'):
                                mx = m.split(':')
                                if len(mx) == 3:
                                    o = int(mx[1])
                                    w = int(mx[2])
                                else:
                                    o = int(mx[1])
                                map = mx[0]
                            maplist.append({
                                'map': map,
                                'order': o,
                                'weight': w
                            })
                            if 'startmapfrompick' in x['maps']:
                                if int(x['maps']['startmapfrompick']) < o:
                                    x['maps']['startmapfrompick'] = o
                            else:
                                x['maps']['startmapfrompick'] = o
                        rkUpdate = True
            if rkUpdate and pug.savePugRatings(pug.ratingsFile, rkData):
                await ctx.send(f'Map list updated for ranked game mode {mode}')
                if (pug.ranked): # reload data for current ranked mode
                    pug.setRankedMode(pug.ranked, True)
            elif rkUpdate==False:
                await ctx.send('Error - a ranked map limit could not be saved - game mode not found.')
            else:
                await ctx.send('Error - ranked map data could not be added; check bot logs.')
        return True

    @commands.hybrid_command(aliases=['rklimit','rksetlimit', 'rksetmaps','rksetmaplimit'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkmaplimit(self, ctx, mode: str = '', limit: int = 5, shuffle: str = ''):
        """Sets the pick limit and shuffle mode for a ranked mode maplist."""
        if (mode in [None,''] or limit in [None, '']):
            await ctx.send('A valid ranked mode and map limit must be specified.')
            return True
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL} [{pug.pugLocked},{pug.ranked}]')
            await ctx.send('Map limits cannot be modified while a match is in progress.')
        else:
            pug.savePugRatings(pug.ratingsFile)
            rkData = pug.loadPugRatings(pug.ratingsFile, True)
            rkUpdate = False
            if 'rankedgames' in rkData:
                log.debug(f'rkmaplimit({mode},{limit}) - ranked games present.')
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        log.debug(f'rkmaplimit({mode},{limit}) - updating mode {x["mode"]}.')
                        mode = x['mode'] # update formatting
                        if 'maps' in x:
                            x['maps']['fixedpicklimit'] = limit
                        else:
                            x['maps'] = {
                                'maplist': [],
                                'fixedpicklimit': limit,
                                'startmapfrompick': 0,
                                'randomorder': False
                            }
                        if len(shuffle) > 0:
                            if shuffle[:1].lower() == 'o':
                                x['maps']['randomorder'] = False
                            else:
                                x['maps']['randomorder'] = True
                            if shuffle[1:2].isnumeric():
                                x['maps']['startmapfrompick'] = max(0, min(int(shuffle[1:2]),limit))
                        rkUpdate = True
            if rkUpdate and pug.savePugRatings(pug.ratingsFile, rkData):
                await ctx.send(f'Map limit updated for ranked game mode {mode}')
                if (pug.ranked): # reload data for current ranked mode
                    pug.setRankedMode(pug.ranked, True)
            elif rkUpdate==False:
                await ctx.send('Error - a ranked map limit could not be saved - game mode not found.')
            else:
                await ctx.send('Error - a ranked map limit could not be saved; check bot logs.')
        return True
    
    @commands.hybrid_command(aliases=['rkmapsim','rksimmap'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkmapsimulation(self, ctx, mode: str = MODE_RANKED_DEFAULT, count=5):
        """Simulates a given number of auto-picks for a given ranked mode. Example: !rkmapsim rASPlus 5"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if not pug.ranked:
            await ctx.send('Ranked mode must be chosen for map pick simulations to occur.')
            return True
        cachedMLR = pug.maps.mapListWeighting.copy()
        count = max(1, min(count, 30))
        await ctx.send(f'Simulating map picks for {count} matches:')
        for x in range(count):
            await ctx.send(f'**Simulation {x+1}**: {PLASEP.join(pug.maps.autoPickRankedMaps(simulate=True))}')
        pug.maps.mapListWeighting = cachedMLR # return back to current state
        return True

    @commands.hybrid_command(aliases=['rkresetmaps','rkresetmappri','rkmapresetpri','rkmapresetdesirability'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkresetmapdesirability(self, ctx, mode: str = MODE_RANKED_DEFAULT):
        """Resets map desirability to defaults within an active ranked mode."""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if not pug.ranked:
            await ctx.send('Ranked mode must be active for map desirability factors to be reset.')
            return True
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        pug.maps.adjustRankedMapDesirability(action='resetAll')
        pug.ratings['maps']['maplist'] = pug.maps.mapListWeighting
        if pug.savePugRatings(pug.ratingsFile):
            await ctx.send('Map desirability values reset to pool defaults.')
            return True

    @commands.hybrid_command(aliases=['rkmapboost','rkboost'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkincreasemapdesirability(self, ctx, mode: str = '', map: str = '', factor: int = 2):
        """Inceases map desirability within an active ranked mode. Parameters: Map Factor, e.g. AS-Ballistic 2"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if not pug.ranked:
            await ctx.send('Ranked mode must be active for map desirability factors to be adjusted.')
            return True
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        if pug.maps.adjustRankedMapDesirability(action='mapincrease',map=map, adjustment=factor):
            pug.ratings['maps']['maplist'] = pug.maps.mapListWeighting
            if pug.savePugRatings(pug.ratingsFile):
                await ctx.send('Map desirability value adjusted.')
                return True
        else:
            await ctx.send('Map desirability value was not adjusted. Please check map name and increase factor values are correct.')
            return True

    @commands.hybrid_command(aliases=['rkmapnerf','rknerf'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkdecreasemapdesirability(self, ctx, mode: str = '', map: str = '', divisor: int = 2):
        """Decreases map desirability within an active ranked mode. Parameters: Map Divisor, e.g. AS-Bridge 2"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if not pug.ranked:
            await ctx.send('Ranked mode must be active for map desirability factors to be adjusted.')
            return True
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        if pug.maps.adjustRankedMapDesirability(action='mapdecrease',map=map, adjustment=divisor):
            pug.ratings['maps']['maplist'] = pug.maps.mapListWeighting
            if pug.savePugRatings(pug.ratingsFile):
                await ctx.send('Map desirability value adjusted.')
                return True
        else:
            await ctx.send('Map desirability value was not adjusted. Please check map name and increase factor values are correct.')
            return True

    @commands.hybrid_command(aliases=['rkmodeconf','rkmodeconfig'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkconf(self, ctx, mode: str = '', capmode: int = 0, role: discord.Role=None, window: int = 0):
        """Configures ranked mode core settings."""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        pug.savePugRatings(pug.ratingsFile)
        rkData = pug.loadPugRatings(pug.ratingsFile, True)
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode']
                    previousSettings = ''
                    if 'capMode' in x:
                        previousSettings = previousSettings+f'Captain mode: {RATED_CAP_MODE[x["capMode"]]} ({x["capMode"]}); '
                        if 'capWindow' in x and int(x['capMode']) == 3:
                            previousSettings = previousSettings+f'Time window for captain selection: {str(x["capWindow"])}s; '
                    if 'capRole' in x and len(x['capRole']) > 0:
                        previousSettings = previousSettings+f'Discord role for captain selection: {x["capRole"]}; '
                    x['capMode'] = max(0, min(capmode, 2)) # clamp to 0-2 - future support for mode 3 will be needed
                    newSettings = f'Captain mode: {RATED_CAP_MODE[x["capMode"]]} ({x["capMode"]}); '
                    if capmode == 3:
                        x['capWindow'] = max(30, min(window, 240)) # clamp to 30-240
                        newSettings = newSettings+f'Time window for captain selection: {str(x["capWindow"])}s; '
                    else:
                        x['capWindow'] = 0
                    if capmode == 2 and role is not None:
                        x['capRole'] = role.name
                        newSettings = f'Discord role for captain selection: {x["capRole"]}; '
            if pug.savePugRatings(pug.ratingsFile, rkData):
                await ctx.send(f'Ranked game mode {mode} configuration updated.\nPrevious settings - {previousSettings}\nNew settings - {newSettings}')
                if (pug.ranked): # reload data for current ranked mode
                    pug.setRankedMode(pug.ranked, True)
            else:
                await ctx.send('Error - ranked game config could not be updated; check bot logs.')
        return True

    @commands.hybrid_command(aliases=['rkscoreconfig','rkscoreconf'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkscoring(self, ctx, mode: str = '', scoremode: str = 'permap', teamwin: int = 0, teamlose: int = 0, capwin: int = 0, caplose: int = 0, volcapwin: int = 0, volcaplose: int = 0):
        """Configures ranked mode scoring settings."""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if (scoremode.lower() not in ['permap','pergame']):
            await ctx.send('Settings not saved: Score Mode must be either: "permap" or "pergame".')
            return True
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock or (pug.pugLocked and pug.ranked):
            await ctx.send(f'A ranked match is already underway at {pug.gameServer.format_gameServerURL}')
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        pug.savePugRatings(pug.ratingsFile)
        rkData = pug.loadPugRatings(pug.ratingsFile, True)
        rkUpdate = False
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode']
                    previousSettings = ''
                    if 'scoring' in x:
                        if 'mode' in x['scoring'] and 'teamWin' in x['scoring'] and 'teamLose' in x['scoring'] and 'capWin' in x['scoring'] and 'capLose' in x['scoring']:
                            previousSettings = f'Scoring mode: {x["scoring"]["mode"]}; Points - Winning team: {x["scoring"]["teamWin"]}, Losing Team: {x["scoring"]["teamLose"]}, Winning Cap: {x["scoring"]["capWin"]}, Losing Cap: {x["scoring"]["capLose"]}'
                        if 'capMode' in x and x['capMode'] == 3:
                            if 'volCapWin' in x['scoring'] and (x['scoring']['volCapWin']) > 0:
                                previousSettings = previousSettings+f', Winning Voluntary Captain: {x["scoring"]["volCapWin"]}'
                            if 'volCapLose' in x['scoring'] and (x['scoring']['volCapLose']) != 0:
                                previousSettings = previousSettings+f', Losing Voluntary Captain: {x["scoring"]["volCapLose"]}'
                    x['scoring'] = {
                        'mode': scoremode,
                        'teamWin': max(0, teamwin),
                        'teamLose': teamlose,
                        'capWin': max(0, capwin),
                        'capLose': caplose,
                        'volCapWin': max(0, volcapwin),
                        'volCapLose': volcaplose
                    }
                    newSettings = f'Scoring mode: {x["scoring"]["mode"]}; Points - Winning team: {x["scoring"]["teamWin"]}, Losing Team: {x["scoring"]["teamLose"]}, Winning Cap: {x["scoring"]["capWin"]}, Losing Cap: {x["scoring"]["capLose"]}'
                    if 'capMode' in x and x['capMode'] == 3:
                        newSettings = newSettings+f', Winning Voluntary Captain: {x["scoring"]["volCapWin"]}, Losing Voluntary Captain: {x["scoring"]["volCapLose"]}'
                    rkUpdate = True
        if rkUpdate == True:
            if pug.savePugRatings(pug.ratingsFile, rkData):
                await ctx.send(f'Ranked game mode {mode} configuration updated.\nPrevious settings - {previousSettings}\nNew settings - {newSettings}')
                if (pug.ranked): # reload data for current ranked mode
                        pug.setRankedMode(pug.ranked, True)
                else:
                    await ctx.send('Error - ranked game config could not be updated; check bot logs.')
        else:
            await ctx.send('Error - ranked mode not found, or no ranked configuration exists.')
        return True

    @commands.hybrid_command(aliases=['rklist'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def rkrecent(self, ctx, mode: str = '', last: int = 5, matchref: str = '', completed: str = ''):
        """Returns recent ranked matches"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if (mode in [None,'']):
            if pug.ranked:
                mode = pug.mode
            else:
                await ctx.send('A valid ranked mode must be specified.')
                return True
        games = []
        msg = []
        if self.ratingsLock or (pug.pugLocked != True and pug.ranked):
            pug.savePugRatings(pug.ratingsFile)
        rkData = pug.loadPugRatings(pug.ratingsFile, True)
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode']
                    games = sorted(x['games'], key=lambda g: datetime.fromisoformat(g['startdate']), reverse=True)
                    players = {}
                    if 'ratings' in x:
                        for p in x['ratings']:
                            players[p['did']] = p['dlastnick']
                    if len(matchref):
                        allgames = games
                        games = []
                        for g in allgames:
                            if re.search(re.escape(matchref),g['gameref'], re.IGNORECASE):
                                games.append(g)
        if len(games):
            if len(games) > last:
                g_limit = last
            else:
                g_limit = len(games)
            if len(matchref):
                msg = f'Most recent {g_limit} {mode} ranked games (search criteria: `{matchref}`):\n'
            else:
                msg = f'Most recent {g_limit} {mode} ranked games:\n'
        elif len(matchref):
            msg = f'No games were found for mode: {mode} and search criteria `{matchref}` please specify valid criteria.'
        else:
            msg = f'No games were found for mode: {mode}, please specify a valid mode.'
        i = 0
        for g in games:
            teamred = []
            teamblue = []
            if (i < last) and ((len(completed) > 0 and g['completed']) or len(completed) == 0):
                i+=1
                if g['completed'] and len(g['enddate']):
                    g_enddate = 'Completed @ '+datetime.fromisoformat(g['enddate']).strftime('%d/%b/%Y @ %H:%M')
                else:
                    g_enddate = 'DNF/Void'
                g_startdate = datetime.fromisoformat(g['startdate']).strftime('%a, %d/%b/%Y @ %H:%M')
                for x in g['teamred']:
                    if g['capred']['id'] == x:
                        teamred.append(players[x]+f'({CAPSIGN})')
                    else:
                        teamred.append(players[x])
                for x in g['teamblue']:
                    if g['capblue']['id'] == x:
                        teamblue.append(players[x]+f'({CAPSIGN})')
                    else:
                        teamblue.append(players[x])
                msg = msg+f'{i}) Match Ref: `{g["gameref"]}`; Started {g_startdate}, {g_enddate}\n'
                msg = msg+f'> Red team (RP: {g["rpred"]}): {PLASEP.join(teamred)}\n> Blue team (RP: {g["rpblue"]}): {PLASEP.join(teamblue)}\n'
                msg = msg+f'> Score :red_square: {g["scorered"]} - {g["scoreblue"]} :blue_square:\n\n'
        if len(msg) > 4000:
            for m in msg.split('\n'):
                if len(m):
                    await ctx.send(m)
        else:
            await ctx.send(msg)
        return True

    @commands.hybrid_command(aliases=['rkreport'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def rkrp(self, ctx, mode: str = '', matchref: str = '', player: discord.Member = None):
        """Returns match and player RP reports"""
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)

        pid = re.search(r'<@(\d*)>', mode)
        if (pid):
            player = int(pid.group(1))
            matchref = ''
            mode = ''
        else:
            pid = re.search(r'<@(\d*)>', matchref)
            if (pid):
                player = int(pid.group(1))
                matchref = ''
                mode = ''
        if (mode in [None,'','last']):
            if mode == 'last' and len(matchref) == 0:
                matchref = mode
            if pug.ranked:
                mode = pug.mode
            else:
                await ctx.send('A valid ranked mode must be specified.')
                return True
        else:
            if pug.ranked and pug.mode.upper() == mode.upper():
                mode = pug.mode # fix case
        teamRed = []
        teamBlue = []
        reports = []
        if self.isPugInProgress and pug.ranked and len(matchref) == 0 and player in ['',None]:
            teamBlue = pug.blue
            teamRed = pug.red
        matchref = re.sub(r'\'|"', '', matchref)
        if (len(teamRed) > 0 and len(teamBlue) > 0) or (len(matchref) > 0 and matchref not in ['player','']):
            reports = self.ratingsMatchReport(mode=mode,teamRed=teamRed,teamBlue=teamBlue,matchref=matchref)
        elif player not in ['',None]:
            if (pid):
                reports = self.ratingsMatchReport(mode=mode,playerid=player)
            else:
                reports = self.ratingsMatchReport(mode=mode,playerid=player.id)
        else:
            await ctx.send('Please provide a valid mode, plus match reference or @player.')
        if len(reports) > 0:
            if reports[0].title == 'Match not found':
                await ctx.send('Match report not found. Please provide a valid match reference.')    
            elif reports[0].title == 'Player not found':
                await ctx.send('Player not found or seed rating not defined. Please provide a valid @player, or add with a rating.')
            else:
                for r in reports:
                    await ctx.send(embed=r)
                return True
            return True
        return True

    @commands.hybrid_command(aliases=['rkvoid'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def rkvoidmatch(self, ctx, mode: str = '', matchref: str = ''):
        pug = self.getPugForModeInChannel(channelId=self.activeChannel.id, mode=mode, ignoreMissing=True)
        if pug is None:
            pug = self.getPugForChannel(channelId=self.activeChannel.id)
        if self.ratingsLock or pug.pugLocked:
            await ctx.send('Matches cannot be voided while a game is in progress. Please try again later.')
            return True
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if (matchref in [None,'']):
            await ctx.send('A valid match reference must be specified.')
            return True
        if pug.ranked and pug.mode.upper() == mode.upper():
            mode = pug.mode
        matchInfo = self.ratingsMatchInfo(mode,matchref)
        if matchInfo == {}:
            await ctx.send('The provided valid match reference could not be found.')
        else:
            started = datetime.fromisoformat(matchInfo['startdate'])
            await ctx.send(f'{"Voiding" if matchInfo["completed"] else "Re-establishing"} match `{matchInfo["gameref"]}` played on {started.strftime("%d/%m/%Y")} at {started.strftime("%H:%M:%S")}...')
            pug.savePugRatings(pug.ratingsFile)
            rk = pug.loadPugRatings(pug.ratingsFile, True)
            if 'rankedgames' in rk:
                for x in rk['rankedgames']:
                    if 'games' in x and 'mode' in x and x['mode'].upper() == mode.upper():
                        for g in x['games']:
                            if 'gameref' in g and g['gameref'].upper() == matchref.upper():
                                g['completed'] = not g['completed']
                                log.debug(f'rkvoidmatch() - Found match data in rk; completed={g["completed"]}')
            pug.savePugRatings(pug.ratingsFile,rk)
            pug.setRankedMode(MODE_CONFIG[pug.mode].isRanked, False)
            players = []
            players.extend(matchInfo['teamred'])
            players.extend(matchInfo['teamblue'])
            for p in players:
                player = self.ratingsPlayerDataHandler('rkget', mode, p)
                if player not in [None,{},'']:
                    msg = str(self.ratingsPlayerDataHandler('rkrecalc',mode,p,0)).split("\n")[-2].replace("> ","")
                    await ctx.send(f'> Recalculated RP for {player["dlastnick"]}...\n> - Updated to last event: {msg}')
                else:
                    await ctx.send(f'> Could not recalculate RP for ID `{p}`; player not found.')
        return True

    #########################################################################################
    # Bot commands.
    #########################################################################################
    @commands.hybrid_command(aliases = ['removepug','rm'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    async def disable(self, ctx, mode: str = ''):
        """Disables PUG commands for a given mode or channel. Admin only"""
        if len(mode) > 0 and mode not in [None,'']:
            if mode.upper() in map(str.upper, MODE_CONFIG):
                mode = next((key for key, _ in MODE_CONFIG.items() if key.upper()==mode.upper()), None)
                targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode, ignoreMissing=True)
                if targetPug == None:
                    await ctx.send(f'The PUG for {mode} could not be removed, as it is not yet established in this channel.')
                    return
                if targetPug.pugLocked or len(targetPug.all) > 0:
                    await ctx.send(f'The PUG for {mode} could not be removed. Please `!reset {mode}` first, if it is locked or not empty.')
                    return
                if self.removePugForModeInChannel(channelId=ctx.message.channel.id, mode=mode):
                    await ctx.send(f'The PUG for {mode} has been removed.')
                else:
                    await ctx.send(f'The PUG for {mode} could not be removed.')
        else:
            channel = ctx.message.channel
            if channel.id in self.pugInstances:
                await channel.send('PUG commands now disabled.')
                self.pugInstances.pop(channel.id, None)
                if self.activeChannel == channel:
                    self.activeChannel = None
                self.savePugConfig(self.configFile)
                return
            await ctx.send('PUG commands were not active in this channel.')

    @commands.hybrid_command(aliases = ['pug'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def list(self, ctx, mode: str = ''):
        """Displays pug status. Optionally specify a mode (e.g., !list proAS) to show a specific mode's status."""
        targetPug = None
       
        # Check if mode parameter is a valid mode
        if mode not in [None,'']:
            if mode.upper() in map(str.upper, MODE_CONFIG):
                targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
                if targetPug is None:
                    await ctx.send(f'Could not access pug for mode `{mode}`.')
                    return
            else:
                await ctx.send(f'Invalid mode `{mode}`. Use `!listmodes` to see available modes.')
                return
        else:
            # Back off to the listpugs command
            await self.listpugs(ctx)
            return
        
        # Display the target pug status
        if targetPug.pugLocked:
            # Pug in progress, show the teams/maps.
            await ctx.send(targetPug.format_match_in_progress)
        elif targetPug.teamsReady and targetPug.ranked:
            # Ranked mode, just display teams and maps
            msg = '\n'.join([
                targetPug.format_pug_short,
                targetPug.format_teams(),
                targetPug.maps.format_current_maplist])
            await ctx.send(msg)
        elif targetPug.teamsReady:
            # Picking maps, just display teams.
            msg = '\n'.join([
                targetPug.format_pug_short,
                targetPug.format_teams(),
                targetPug.maps.format_current_maplist,
                self.format_pick_next_map(mention=False, pug=targetPug)])
            await ctx.send(msg)
        elif targetPug.captainsReady:
            # Picking players, show remaining players to pick, but don't
            # highlight the captain to avoid annoyance.
            msg = '\n'.join([
                targetPug.format_pug_short,
                targetPug.format_remaining_players(number=True),
                targetPug.format_teams(),
                self.format_pick_next_player(mention=False, pug=targetPug)])
            await ctx.send(msg)
        else:
            # Default, show sign ups.
            msg = []
            msg.append(targetPug.format_pug())
            if targetPug.playersReady and not targetPug.ranked:
                # Copy of what's in processPugStatus, not ideal, but avoids the extra logic it does.
                if targetPug.numCaptains == 1:
                    # Need second captain.
                    msg.append('Waiting for 2nd captain. Type **!captain** to become a captain. To choose a random captain type **!randomcaptains**')
                else:
                    msg.append('Waiting for captains. Type **!captain** to become a captain. To choose random captains type **!randomcaptains**')
            await ctx.send('\n'.join(msg))
            await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command(aliases = ['pugtime'])
    @commands.guild_only()
    @commands.cooldown(1, 60, commands.BucketType.channel)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Ignore)
    async def promote(self, ctx, mode: str = ''):
        """Promotes the pug. Limited to once per minute alongside poke."""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
        if targetPug is not None:
            self.lastPokeTime = datetime.now()
            await ctx.send(f'Hey @here it\'s PUG TIME!!!\n**{targetPug.playersNeeded}** needed for **{targetPug.desc}**!')
        else:
            await ctx.send(f'There are no active games to promote.')

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.cooldown(1, 60, commands.BucketType.channel)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Ignore)
    async def poke(self, ctx, mode: str = ''):
        """Highlights those signed to pug. Limited to once per minute alongside promote."""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
        if targetPug == None:
            await ctx.send(f'There are no PUGs where players need poking.')
            return
        minPlayers = 2
        if targetPug.numPlayers < minPlayers:
            return
        self.lastPokeTime = datetime.now()
        await ctx.send(f'Poking those signed (you will be unable to poke again for 60 seconds): {targetPug.format_all_players(number=False, mention=True)}')

    @commands.hybrid_command(aliases = ['serverlist'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def listservers(self, ctx):
        await ctx.send(self.pugInfo.gameServer.format_showall_servers)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def server(self, ctx, mode: str = ''):
        """Displays Pug server info."""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
            await ctx.send(targetPug.gameServer.format_game_server)
        else:
            for _, mode, pug in self.getAllActivePugs():
                await ctx.send(f'[**{pug.mode}**] {pug.gameServer.format_game_server}')

    @commands.hybrid_command(aliases = ['serverinfo'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def serverstatus(self, ctx, mode: str = ''):
        """Displays Pug server current status."""
        targetPugs = []
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPugs.append(self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode))
        else:
            for _, mode, pug in self.getAllActivePugs():
                targetPugs.append(pug)
        for pug in targetPugs:
            if pug.pugTempLocked < 2:
                await self.queryServerStats(cacheonly=True, pug=pug)
                if pug.gameServer.utQueryEmbedCache != {}:
                    embedInfo = discord.Embed().from_dict(pug.gameServer.utQueryEmbedCache)
                    # Strip objectives from the card data
                    for x, f in enumerate(embedInfo.fields):
                        if 'Objectives' in f.name:
                            embedInfo.remove_field(x)
                    await ctx.send(embed=embedInfo)
                else:
                    await ctx.send(pug.gameServer.format_game_server_status)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def serverquery(self, ctx, serveraddr: str = '', hideheader: bool = False):
        """Displays status of a given server"""
        serverinfo = {}
        #for _, mode, pug in self.getAllActivePugs():
        #    if (pug.gameServer.utQueryReporterActive or pug.gameServer.utQueryStatsActive):
        #        await ctx.send('Server query cannot be run while any pug reporting is in progress.') # TEST ME
        #        return

        if serveraddr not in ['',None]:
            # Check for valid server input
            for x in ['unreal://','\w+://','\\\\','localhost','^127\.']:
                try:
                    serveraddr = re.compile(x).sub('', serveraddr)
                except:
                    log.error('Failed to parse input to !serverquery')
            # Check for IP with or without port, or FQDN with or without port
            for x in ['^(?P<ip>((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)):(?P<port>\d{1,5})$',
                        '^(?P<ip>((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$',
                        '^(?P<dns>(?=^.{4,253}$)(^((?!-)[a-zA-Z0-9-]{1,63}(?<!-)\.)+[a-zA-Z]{2,63})):(?P<port>\d{1,5})$',
                        '^(?P<dns>(?=^.{4,253}$)(^((?!-)[a-zA-Z0-9-]{1,63}(?<!-)\.)+[a-zA-Z]{2,63})$)']:
                if re.search(x,serveraddr):
                    servermatch = re.match(r'{0}'.format(x),serveraddr)
                    if 'ip' not in servermatch.groupdict() and 'dns' in servermatch.groupdict():
                        try:
                            for ip in dns.resolver.resolve(servermatch['dns'], 'A'):
                                serverinfo['ip'] = ip.address
                        except:
                            log.warning(f'DNS lookup failure for {serveraddr}')
                        if 'port' in servermatch.groupdict():
                            serverinfo['game_port'] = int(servermatch.groupdict()['port'])
                        else:
                            serverinfo['game_port'] = 7777
                    elif 'ip' in servermatch.groupdict() and 'port' in servermatch.groupdict():
                        serverinfo['ip'] = servermatch.groupdict()['ip']
                        serverinfo['game_port'] = int(servermatch.groupdict()['port'])
                    elif 'ip' in servermatch.groupdict():
                        serverinfo['ip'] = servermatch.groupdict()['ip']
                        serverinfo['game_port'] = 7777
        if serverinfo != {}:
            serverinfo['query_port'] = int(serverinfo['game_port'])+1
            # Set the utQueryData base
            self._defaultPugInfo.gameServer.utQueryData = serverinfo
            await self.queryServerStats(cacheonly=True, pug=self._defaultPugInfo)
            if self._defaultPugInfo.gameServer.utQueryEmbedCache != {}:
                embedInfo = discord.Embed().from_dict(self._defaultPugInfo.gameServer.utQueryEmbedCache)
                # Reset caches
                self._defaultPugInfo.gameServer.utQueryData = {}
                self._defaultPugInfo.gameServer.utQueryEmbedCache = {}
                # Strip objectives from the card data
                for x, f in enumerate(embedInfo.fields):
                    if 'Objectives' in f.name:
                        embedInfo.remove_field(x)
                if hideheader:
                    embedInfo.title = ''
                    embedInfo.description = ''
                await ctx.send(embed=embedInfo)
            else:
                await ctx.send('Could not resolve server from provided information.')
        else:
            await ctx.send('Could not resolve server from provided information.')

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def listmodes(self, ctx, group: int = -1):
        """Lists available modes for the current channel"""
        outStr = ['Available modes in this channel, are:']
        for k in MODE_CONFIG:
            if (self.pugInfo.modeLimit > 0 and MODE_CONFIG[k].modeGroup in [0, self.pugInfo.modeLimit]) or (group > 0 and MODE_CONFIG[k].modeGroup in [0, group]) or self.pugInfo.modeLimit == 0:
                outStr.append(PLASEP + '**' + k + '**')
        outStr.append(PLASEP)
        await ctx.send(' '.join(outStr))

    @commands.hybrid_command(aliases=['activepugs'])
    @commands.guild_only()
    @commands.cooldown(1, 5, commands.BucketType.channel)
    @commands.check(isActiveChannel_Check)
    async def listpugs(self, ctx):
        """Lists all active pugs in this channel with player counts"""
        channelId = ctx.message.channel.id
        
        # Get all active pugs in this channel
        activePugs = []
        inProgressPugs = []
        if channelId in self.pugInstances:
            default_mode, _ = self.getDefaultPugByActivity(channelId)
            default_mode = default_mode if default_mode else MODE_DEFAULT

            for mode, pug in self.pugInstances[channelId].items():
                status = ' :mega:' if mode == default_mode else ''
                rankedStatus = ' :scales:' if pug.ranked else ''
                server = pug.gameServer.format_current_serveralias if pug.gameServer else 'No server assigned'
                if pug.pugTempLocked > 1:
                    serverAddr = '\n**Status**: Pug is currently on hold while another match is in progress. '
                elif pug.gameServer and pug.matchReady and pug.pugTempLocked < 2 and pug.pugLocked:
                    serverAddr = f'\n@ `{pug.gameServer.format_gameServerURL}` - spec pass: `{pug.gameServer.spectatorPassword}`'
                elif pug.gameServer and pug.matchReady and pug.pugTempLocked < 2 and not pug.pugLocked:
                    serverAddr = f'\n** Status**: Setting up server for the next match...'
                    await self.processPugStatus(ctx, pug=pug)
                else:
                    serverAddr = ''

                maps = f'\nMaps: {pug.maps.format_current_maplist}' if pug.mapsReady else ''
                playerList = ''
                if len(pug.red) or len(pug.blue):
                    playerList = f'\n:red_circle: {", ".join([f"<@{player.id}>" for player in pug.red])}\n:blue_circle: {", ".join([f"<@{player.id}>" for player in pug.blue])}'
                if len(pug.players) and not pug.captainsReady:
                    if len(pug.red) or len(pug.blue):
                        playerList = f'{playerList}, unpicked players:'
                    playerList = f'\n:crossed_swords: {playerList}{", ".join([f"<@{player.id}>" for player in pug.players])}' if len(pug.players) else '\nNo players'
                if pug.matchReady:
                    inProgressPugs.append((mode, pug.maps.maxMaps, pug.numPlayers, pug.maxPlayers, status, rankedStatus, server, playerList, maps, serverAddr))
                else:
                    activePugs.append((mode, pug.maps.maxMaps, pug.numPlayers, pug.maxPlayers, status, rankedStatus, server, playerList, maps, serverAddr))
        
        if not activePugs and not inProgressPugs:
            await ctx.send('No active PUG queues in this channel.')
            return
        linebreak = '\n\n~~---------------------~~' if len(activePugs) > 1 else ''
        embedInfo = discord.Embed(color=discord.Color.greyple(),title=f'Active PUG queues in #{ctx.message.channel.name}')
        if len(activePugs) > 0:
            for mode, matchlength, playerCount, maxPlayers, status, ranked, server, playerList, maps, serverAddr in activePugs:
                players = f'{playerCount}/{maxPlayers}'
                mode_name = MODE_CONFIG[mode].name if mode in MODE_CONFIG else mode
                if (1/maxPlayers)*playerCount >= 0.5:
                    embedInfo.color = discord.Color.orange()
                if (1/maxPlayers)*playerCount >= 0.75:
                    embedInfo.color = discord.Color.red()
                if len(maps) > 0:
                    embedInfo.color = discord.Color.green()
                embedInfo.add_field(name=f'{mode} ({mode_name}) on {server}',value=f'Best of `{matchlength}` maps. `{players}` players signed{ranked}{status}{":" if playerList else "."}{playerList}{maps}{serverAddr}{linebreak}',inline=False)
            embedInfo.set_footer(text='Use `!join <mode>` or `!leave <mode>` to join/leave a specific mode.')
            await ctx.send(embed=embedInfo)
        if len(inProgressPugs) > 0:
            embedInfo = discord.Embed(color=discord.Color.green(),title=f'Games in progress in #{ctx.message.channel.name}')
            for mode, matchlength, playerCount, maxPlayers, status, ranked, server, playerList, maps, serverAddr in inProgressPugs:
                players = f'{playerCount}/{maxPlayers}'
                mode_name = MODE_CONFIG[mode].name if mode in MODE_CONFIG else mode
                embedInfo.add_field(name=f'{mode} ({mode_name}) on {server}',value=f'Best of `{matchlength}` maps. `{players}` players signed{ranked}{status}{":" if playerList else "."}{playerList}{maps}{serverAddr}{linebreak}',inline=False)
            embedInfo.set_footer(text='')
            await ctx.send(embed=embedInfo)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def setmode(self, ctx, mode: str):
        """Sets mode of the pug (deprecated)"""
        await ctx.send('Use !join <mode> to join or start a specific mode, or !leave <mode> to leave a specific mode.')
        return True

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def setplayers(self, ctx, mode, limit: str = '12'):
        """Sets number of players"""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        elif limit != '' and limit.upper() in map(str.upper, MODE_CONFIG):
                targetMode = limit
                limit = str(mode)
                targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=targetMode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if targetPug.pugLocked:
            await ctx.send('Player limit cannot be changed while pug is in progress.')
            return
        if targetPug.ranked != True and targetPug.captainsReady:
            await ctx.send('Pug already in picking mode. Reset if you wish to change player limit.')
        elif (int(limit) % 2 == 0 and int(limit) >= MODE_CONFIG[targetPug.mode].minPlayers and int(limit) <= MODE_CONFIG[targetPug.mode].maxPlayers):
            targetPug.setMaxPlayers(int(limit))
            await ctx.send('Player limit set to ' + str(targetPug.maxPlayers))
            await self.processPugStatus(ctx, targetPug)
        elif (int(limit) % 2 == 0 and int(limit) < len(targetPug.players) + len(targetPug.red) + len(targetPug.blue)):
            await ctx.send(f'Player limit cannot be set to {limit} as there are already {len(targetPug.players) + len(targetPug.red) + len(targetPug.blue)} players signed up.')
        else:
            await ctx.send(f'Player limit unchanged. Players must be a multiple of 2 + between {MODE_CONFIG[targetPug.mode].minPlayers} and {MODE_CONFIG[targetPug.mode].maxPlayers}')

    @commands.hybrid_command(aliases = ['adminsp','asp'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(admin.hasManagerRole_Check)
    async def adminsetplayers(self, ctx, mode: str = '', limit: int = 12):
        """Force sets number of players"""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if targetPug.ranked != True and targetPug.captainsReady:
            await ctx.send('Pug already in picking mode. Reset if you wish to change player limit.')
        elif (limit % 2 == 0):
            targetPug.setMaxPlayers(limit)
            await ctx.send(f'Player limit forcefully set to {targetPug.maxPlayers}. ({targetPug.mode} min: {MODE_CONFIG[targetPug.mode].minPlayers}, max: {MODE_CONFIG[targetPug.mode].maxPlayers})')
            await self.processPugStatus(ctx, targetPug)
        else:
            await ctx.send('Player limit unchanged. Players must be a multiple of 2')


    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def setmaps(self, ctx, mode: str = '', limit: int=5):
        """Sets number of maps"""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
            try:
                limit = int(mode)
                mode = ''
            except ValueError:
                pass

        if (targetPug.ranked and targetPug.ratings is not None and 'maps' in targetPug.ratings and 'fixedpicklimit' in targetPug.ratings['maps'] and targetPug.ratings['maps']['fixedpicklimit'] > 0):
            await ctx.send(f'Map limit is fixed to {targetPug.maps.maxMaps} maps within this ranked mode.')
            return
        if (targetPug.pugLocked != True and targetPug.maps.setMaxMaps(limit)):
            await ctx.send(f'Map limit set to {targetPug.maps.maxMaps}')
            if targetPug.teamsReady:
                # Only need to do this if maps already being picked, as it could mean the pug needs to be setup.
                await self.processPugStatus(ctx, targetPug)
        else:
            modemsg = ' Specify a mode to set a limit, e.g.: !setmaps <mode> <limit>' if mode == '' else ''
            await ctx.send(f'Map limit unchanged. Map limit is {targetPug.maps.maxMapsLimit}.{modemsg}')

    @commands.hybrid_command(aliases = ['adminendmatch','endmatch'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(admin.hasManagerRole_Check)
    async def rkendmatch(self, ctx, mode: str = '', matchCode: str = ''):
        """Forcefully ends the current ranked match, with a match code for post-match processing."""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if (matchCode not in [None, ''] and targetPug is not None and targetPug.ranked and (targetPug.pugLocked or targetPug.gameServer.matchInProgress) or matchCode.lower() == 'force'):
            if matchCode.lower() == 'force':
                await ctx.send('Force ending match without match code. RP may not be updated correctly.')
                if targetPug.gameServer.matchCode in [None,'','N/A'] or (targetPug.gameServer.matchCode not in [None,'','N/A'] and len(targetPug.gameServer.matchCode) < 6):
                    targetPug.gameServer.matchCode = f'temp-{datetime.now().strftime("%Y%m%d%H%M%S")}'
                await self.endMatch(False)
                self.ratingsLock = False
                return
            endpoint = f'{targetPug.ratingsSyncAPI["matchDataURL"]}?&matchcode={matchCode}'
            log.debug(f'rksync() - Fetching provided match from API: {endpoint}')
            syData = self.ratingsSync(endpoint, body='', restrict=True, delay=5)
            if syData not in [{},None,''] and 'match_summary' in syData:
                await ctx.send(f'Ending match with valid match code `{matchCode}`' if matchCode else '')
                targetPug.gameServer.matchCode = matchCode
                await targetPug.gameServer.endMatch(False)
                self.ratingsLock = False
            else:
                await ctx.send('Invalid match code provided. Please verify with Pug Stats and try again.')
        else:
            await ctx.send('No ranked match is currently in progress.')

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def reset(self, ctx, mode: str = ''):
        """Resets the pug. Players must rejoin and server is reset even if a match is running. Use with care."""
        reset = False
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.pugInfo

        if (admin.hasManagerRole_Check(ctx) or not(targetPug.pugLocked or (targetPug.gameServer and targetPug.gameServer.matchInProgress))):
            if mode in [None, '']:
                await ctx.send(f'Specify a mode to perform an administrative reset, e.g.: !reset {targetPug.mode}')
            else:
                reset = True
        else:
            requester = ctx.message.author
            if requester in targetPug.red:
                if self.resetRequestRed:
                    await ctx.send('Red team have already requested reset. Blue team must also request.')
                else:
                    self.resetRequestRed = True
                    await ctx.send('Red team have requested reset. Blue team must also request.')
            elif requester in targetPug.blue:
                if self.resetRequestBlue:
                    await ctx.send('Blue team have already requested reset. Red team must also request.')
                else:
                    self.resetRequestBlue = True
                    await ctx.send('Blue team have requested reset. Red team must also request.')
            else:
                if mode in [None, '']:
                    await ctx.send(f'Specify a mode to request a reset, e.g.: !reset {targetPug.mode}')
                else:
                    await ctx.send('Pug is in progress, only players involved the pug or admins can reset.')
            if self.resetRequestRed and self.resetRequestBlue:
                self.resetRequestRed = False
                self.resetRequestBlue = False
                reset = True
        if reset:
            await ctx.send(f'[**{targetPug.mode}**] Removing all signed players: {targetPug.format_all_players(number=False, mention=True)}')
            if len(targetPug.queuedPlayers):
                await ctx.send(f'[**{targetPug.mode}**] Removing all queued players: {targetPug.format_queued_players(number=False, mention=True)}')
            if targetPug.resetPug(True):
                await ctx.send(f'[**{targetPug.mode}**] Pug Reset.')
                await self.listpugs(ctx)
            else:
                await ctx.send(f'[**{targetPug.mode}**]  Reset failed. Please, try again or inform an admin.')

    @commands.hybrid_command(aliases=['replay'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def retry(self, ctx, mode: str = ''):
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
        if targetPug == None:
            await ctx.send('Could not find a valid, failed PUG to retry. Use `!retry <mode>` to specify a game to re-attempt setup for.')
            return
        if targetPug.gameServer.matchInProgress is False or targetPug.gameServer.gameServerOnDemand:
            retryAllowed = True
        else:
            retryAllowed = False

        if targetPug.matchReady and retryAllowed:
            await self.processPugStatus(ctx, targetPug)
        else:
            # TODO: Recall saved data from last match and play it back into the bot
            await ctx.send('Retry can only be utilised after a failed setup.')

    @commands.hybrid_command(aliases=['resetcaps','resetcap','xc'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def resetcaptains(self, ctx, mode: str = ''):
        """Resets back to captain mode. Any players or maps picked will be reset."""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)

        if targetPug == None:
            for mode, pug in self.getAllPugsInChannel(channelId=ctx.message.channel.id).items():
                if not pug.ranked and pug.playersReady and pug.captainsReady:
                    targetPug = pug
                    break
        if targetPug == None:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if targetPug.ranked or targetPug.numCaptains < 1 or targetPug.pugLocked:
            return

        targetPug.maps.resetMaps()
        targetPug.softPugTeamReset()
        await ctx.send('Captains have been reset.')
        await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command(aliases=['jq','JQ'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def queue(self, ctx, mode: str = ''):
        """Joins a queue of players for the next pug"""
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        await self.join(ctx, mode=targetPug.mode, notes='queue')
        return
    
    @commands.hybrid_command(aliases=['j','J','JOIN'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def join(self, ctx, mode: str = '', notes: str = ''):
        """Joins the pug. Optionally specify a mode (e.g., !join proAS) and flags (nomic, q/next)."""
        player = ctx.message.author
        flags = ''
        notesmsg = ''
        targetMode = None
        targetPug = None
        
        if len(mode) == 0:
            if str(ctx.author.id) in self.playerPreferences:
                mode = self.playerPreferences[str(ctx.author.id)]['mode']
                log.debug(f'Player preference mode used: {mode}')

        if mode.upper() in map(str.upper, MODE_CONFIG):
            targetMode = next((key for key, value in MODE_CONFIG.items() if key.upper()==notes.upper()), None)

        if notes in [None, ''] and targetMode == None:
            notes = mode

        if notes not in [None, '']:
            if notes.upper() in map(str.upper, MODE_CONFIG):
                targetMode = next((key for key, _ in MODE_CONFIG.items() if key.upper()==notes.upper()), None)
            elif notes.lower() == "nomic":
                flags = notes.lower()
                notesmsg = ' and tagged as "no mic"'
            elif notes.lower()[:1] == "q" or notes.lower()[:4] == "next":
                targetPug = self.getPugForChannel(ctx.message.channel.id)
                if targetPug is None:
                    await ctx.send('No active pug found in this channel.')
                    return
                if player in targetPug.queuedPlayers:
                    await ctx.send(f'{display_name(player)} is already queued for the next pug.')
                    return True
                if targetPug.pugLocked or targetPug.playersReady:
                    flags = 'queue'
                    notesmsg = ' to the queue for the next pug'
                else:
                    notesmsg = ' immediately, as a pug is not yet running'
        if targetMode is None:
            prefs = self.getPlayerPreferences(player.id)
            log.debug('join() - returned player preferences')
            if prefs:
                targetMode = prefs['mode']

        if targetMode is not None:
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=targetMode)
            if targetPug is None:
                await ctx.send(f'Could not access pug for mode `{targetMode}`.')
                return
            notesmsg = f' to {MODE_CONFIG[targetMode].name}' if notesmsg == '' else notesmsg
        else:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
            if targetPug is None:
                await ctx.send('No active pug found in this channel.')
                return
            targetMode = targetPug.mode

        canJoin, conflictMsg = self.handleMultiInstanceConflicts(player=player, targetChannelId=ctx.message.channel.id, targetMode=targetMode)
        if not canJoin:
            await ctx.send(conflictMsg)
            return

        if targetPug.ranked:
            if not targetPug.addRankedPlayer(player, flags):
                if targetPug.playersReady:
                    await ctx.send('Ranked pug is already full.')
                    return
                elif player in targetPug.players:
                    await ctx.send('Already added.')
                    return
                else:
                    await ctx.send(f'{display_name(player)} could not be added - ineligible to join a ranked pug.')
                    return
        else:
            if not targetPug.addPlayer(player, flags):
                if targetPug.playersReady:
                    await ctx.send('Pug is already full.')
                    return
                else:
                    await ctx.send('Already added.')
                    return

        if flags != 'queue':
            self.trackPlayerJoin(player=player, channelId=ctx.message.channel.id, mode=targetMode, updateConfig=True)

        if flags != 'queue':
            await ctx.send(f'{display_name(player)} was added{notesmsg}.')
            await self.listpugs(ctx)
        else:
            await ctx.send(f'{display_name(player)} was added{notesmsg}.')
        await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command(aliases=['l','lv','lva','lq'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def leave(self, ctx, mode: str = ''):
        """Leaves the pug. Optionally specify a mode (e.g., !leave proAS) to leave a specific mode's pug."""
        player = ctx.message.author
        targetMode = None
        
        if mode not in [None, '']:
            if mode.upper() in map(str.upper, MODE_CONFIG):
                targetMode = next((key for key, _ in MODE_CONFIG.items() if key.upper()==mode.upper()), None)
            else:
                await ctx.send(f'Invalid mode `{mode}`. Use `!listmodes` to see available modes.')
                return
        
        if targetMode is None:
            targetPug = self.getPugForChannel(ctx.message.channel.id)
            if targetPug is None:
                await ctx.send('No active pug found in this channel.')
                return
            targetMode = targetPug.mode
        else:
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=targetMode)
            if targetPug is None:
                await ctx.send(f'Could not access pug for mode `{targetMode}`.')
                return
        
        if targetPug.pugLocked:
            await ctx.send('Match is in progress, players cannot leave the game at this time.')
            return

        if ctx.message.channel.id in self.tempQueuedPlayers:
            tempPlayers = self.tempQueuedPlayers[ctx.message.channel.id]
            for i, (tempPlayer, flags) in enumerate(tempPlayers):
                if tempPlayer.id == player.id and 'mode' in flags and flags['mode'].upper() == mode.upper():
                    self.tempQueuedPlayers[ctx.message.channel.id].pop(i)
                    if not self.tempQueuedPlayers[ctx.message.channel.id]:
                        del self.tempQueuedPlayers[ctx.message.channel.id]
                    self.trackPlayerLeave(player, ctx.message.channel.id, targetMode)
                    #await ctx.send(f'{display_name(player)} has left the temporary queue.')
                    #return True
        
        if player in targetPug.queuedPlayers:
            targetPug.removePlayerFromPug(player)
            self.trackPlayerLeave(player, ctx.message.channel.id, targetMode)
            await ctx.send(f'{display_name(player)} has left the queue.')
            return True
        if not targetPug.pugLocked:
            if targetPug.removePlayerFromPug(player):
                self.trackPlayerLeave(player, ctx.message.channel.id, targetMode)
                await ctx.send(f'{display_name(player)} left.')
                await self.processPugStatus(ctx, targetPug)
        else:
            await self.isPugInProgress(ctx, True)
        return True
    
    @commands.hybrid_command(aliases=['cap','сфзефшт'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def captain(self, ctx, mode: str = ''):
        """Volunteer to be a captain in the pug."""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)

        if targetPug == None:
            for mode, pug in self.getAllPugsInChannel(channelId=ctx.message.channel.id).items():
                if not pug.ranked and pug.playersReady and not pug.captainsReady:
                    targetPug = pug
                    break
        if targetPug == None:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if targetPug.ranked or not targetPug.playersReady or targetPug.captainsReady or targetPug.gameServer.matchInProgress:
            log.debug(f'!captain rejected for {targetPug.mode}: Players Ready = {targetPug.playersReady}, Captains Ready = {targetPug.captainsReady}, Match In Progress = {targetPug.gameServer.matchInProgress}')
            return

        player = ctx.message.author
        if targetPug.setCaptain(player):
            await ctx.send(f'{player.mention} has volunteered as a captain!')
            await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command(aliases=['randcap','rcap','rc'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def randomcaptains(self, ctx, mode: str = ''):
        """Picks a random captain for each team without a captain."""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)

        if targetPug == None:
            for mode, pug in self.getAllPugsInChannel(channelId=ctx.message.channel.id).items():
                if not pug.ranked and pug.playersReady and not pug.captainsReady:
                    targetPug = pug
                    break
        if targetPug == None:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if targetPug == None or targetPug.ranked or not targetPug.playersReady or targetPug.captainsReady:
            log.debug(f'!randomcaptains rejected for {targetPug.mode if targetPug else "unknown"}: Players Ready = {targetPug.playersReady if targetPug else "unknown"}, Captains Ready = {targetPug.captainsReady if targetPug else "unknown"}, Match In Progress = {targetPug.gameServer.matchInProgress if targetPug else "unknown"}')
            return

        while not targetPug.captainsReady:
            pick = None
            while not pick:
                pick = random.choice(targetPug.players)
            targetPug.setCaptain(pick)
        await self.processPugStatus(ctx, targetPug)

    @commands.command(aliases=['p'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def pick(self, ctx, mode: str = '', *players: int): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Picks a player for a team in the pug."""
        captain = ctx.message.author
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
            log.debug(f'pick() - found active pug in channel {ctx.message.channel.id} by mode {mode}')
        else:
            pno = 0
            try:
                pno = int(mode)
            except ValueError:
                pass
            if pno > 0:
                players = (pno,) + players
                log.debug(f'pick() - parsed mode as player number {str(pno)} and adjusted players tuple to {str(players)}')
            elif type(mode) is int:
                players = (mode,) + players
                log.debug(f'pick() - parsed mode as player number {str(mode)} and adjusted players tuple to {str(players)}')
        if targetPug == None:
            # Find active pug
            log.debug(f'pick() - finding active pug in channel {ctx.message.channel.id}')
            for mode, pug in self.getAllPugsInChannel(channelId=ctx.message.channel.id).items():
                if not pug.ranked and pug.captainsFull:
                    targetPug = pug
                    break

        if targetPug == None:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        # TODO: improve this, don't think we should use matchInProgress
        if targetPug == None or targetPug.ranked or targetPug.teamsFull or (not targetPug.captainsFull) or (not captain == targetPug.currentCaptainToPickPlayer) or targetPug.pugLocked:
            log.debug(f'!pick rejected for {targetPug.mode if targetPug else "unknown"}: Ranked = {targetPug.ranked if targetPug else "unknown"}, Teams Full = {targetPug.teamsFull if targetPug else "unknown"}, Captains Full = {targetPug.captainsFull if targetPug else "unknown"}, Picking Captain = {captain}, Current Picking Captain = {targetPug.currentCaptainToPickPlayer if targetPug else "unknown"}, Pug Locked = {targetPug.pugLocked if targetPug else "unknown"}, Match In Progress = {targetPug.gameServer.matchInProgress if targetPug else "unknown"}')
            return

        picks = list(itertools.takewhile(functools.partial(targetPug.pickPlayer, captain), (x - 1 for x in players)))

        if picks:
            if targetPug.teamsFull:
                await ctx.send(f'Teams have been selected:\n{targetPug.format_teams(mention=True)}')
            await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command(aliases=['maplist'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def listmaps(self, ctx, mode: str = '', show: str = ''):
        """Returns the list of maps to pick from"""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
        
        if targetPug == None:
            # Find active pug
            log.debug(f'listmaps() - finding active pug in channel {ctx.message.channel.id}')
            for mode, pug in self.getAllPugsInChannel(channelId=ctx.message.channel.id).items():
                if pug.captainsFull and pug.teamsFull and not pug.matchReady:
                    targetPug = pug
                    break
        if targetPug == None:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)
        
        if targetPug == None:
            targetPug = self.pugInfo

        if (targetPug.ranked):
            if (show == "all" or mode == "all"):
                msg = [f'Ranked mode ({targetPug.mode}) will pick from the following underscored maps: ']
                msg.append(targetPug.maps.format_available_maplist)
            else:
                msg = [f'Ranked mode ({targetPug.mode}) will pick from the following map list: ']
                msg.append(targetPug.maps.format_filtered_maplist)
        else:
            msg = ['Server map list is: ']
            msg.append(targetPug.maps.format_available_maplist)
        await ctx.send('\n'.join(msg))

    @commands.hybrid_command(aliases=['m'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def map(self, ctx, mode: str = '', idx: int = 0):
        """Picks a map in the pug."""
        captain = ctx.message.author
        targetPug = None

        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
                
        if targetPug == None:
            if idx == 0:
                try:
                    idx = int(mode)
                except ValueError:
                    pass
            # Find active pug
            for mode, pug in self.getAllPugsInChannel(channelId=ctx.message.channel.id).items():
                if not pug.ranked and pug.teamsReady:
                    targetPug = pug
                    break
        if targetPug == None:
            targetPug = self.getPugForChannel(channelId=ctx.message.channel.id)

        if (targetPug == None or targetPug.ranked or targetPug.matchReady or not targetPug.teamsReady or captain != targetPug.currentCaptainToPickMap):
            # Skip if in ranked mode or not in captain mode with full teams or if the author is not the next map captain.
            log.debug(f'!map rejected for {targetPug.mode if targetPug else "unknown"}: Ranked = {targetPug.ranked if targetPug else "unknown"}, Match Ready = {targetPug.matchReady if targetPug else "unknown"}, Teams Ready = {targetPug.teamsReady if targetPug else "unknown"}, Current Captain = {targetPug.currentCaptainToPickMap if targetPug else "unknown"}')
            return

        mapIndex = idx - 1 # offset as users see them 1-based index.
        if mapIndex < 0 or mapIndex >= len(targetPug.maps.availableMapsList):
            await ctx.send('Pick a valid map. Use !map <mode> <map_number>. Use !listmaps to see the list of available maps.')
            return

        if not targetPug.pickMap(captain, mapIndex):
            await ctx.send('Map already picked. Please, pick a different map.')
        
        msg = [f'Maps chosen **({len(targetPug.maps)} of {targetPug.maps.maxMaps})**:']
        msg.append(targetPug.maps.format_current_maplist)
        await ctx.send(' '.join(msg))
        await self.processPugStatus(ctx, targetPug)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def last(self, ctx, mode: str = ''):
        """Shows the last pug info."""
        targetPug = None
        activePugs = []
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode, ignoreMissing=True)
            if targetPug is not None:
                activePugs.append(targetPug)
        else:
            for mode, pug in self.getAllPugsInChannel(ctx.message.channel.id).items():
                activePugs.append(pug)

        linebreak = '\n' if len(activePugs) > 1 else ''
        embedInfo = discord.Embed(color=discord.Color.blurple(),title=f':timer: Previous Pugs in #{ctx.message.channel.name}')
        for targetPug in activePugs:
            mode_name = MODE_CONFIG[targetPug.mode].name if targetPug.mode in MODE_CONFIG else targetPug.mode
            pughdr = f'{targetPug.mode} ({mode_name})'
            pugstr = f'{targetPug.format_last_pug_for_embed}{linebreak}'
            embedInfo.add_field(name=pughdr, value=pugstr, inline=False)
        embedInfo.url = DEFAULT_STATS_URL
        await ctx.send(embed=embedInfo)

    @commands.hybrid_command(aliases=['pref','prefs','preference','preferences'])
    @commands.check(isActiveChannel_Check)
    async def prefer(self, ctx, mode: str='', maps: str=''):
        "Stores user preferences towards modes and maps."
        await ctx.send('Preferences updated.' if self.setPlayerPreferences(player=ctx.author.id, mode=mode, maps=maps, save=True) else 'Could not update preferences. Try again later.')
        return True

    @commands.hybrid_command(aliases = ['setrep','repchan'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def setreporter(self, ctx):
        """Configures the UT Server Reporter channel. Admin only"""
        self.utReporterChannel = ctx.message.channel
        await ctx.send('UT Reporter threads will be active in this channel for the next PUG.')
        return

    @commands.hybrid_command(aliases = ['muterep'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def mutereporter(self, ctx, mode: str = ''):
        """Mutes the UT Server Reporter until the next active pug. Admin only"""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
                
        if targetPug == None:
            targetPug = self.pugInfo

        if (targetPug.gameServer.utQueryReporterActive or targetPug.gameServer.utQueryStatsActive) and self.utReporterChannel is not None:
            targetPug.gameServer.utQueryReporterActive = False
            targetPug.gameServer.utQueryStatsActive = False
            await ctx.send('Muted UT Reporter threads in the reporter channel')
        else:
            await ctx.send('UT Reporter channel not defined, or threads not currently running.')
        return

    @commands.hybrid_command(aliases = ['startrep','forcerep'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def startreporter(self, ctx, mode: str = ''):
        """Force-starts the UT Server Reporter, whether an active pug is running or not. Admin only"""
        targetPug = None
        if mode != '' and mode.upper() in map(str.upper, MODE_CONFIG):
            targetPug = self.getPugForModeInChannel(channelId=ctx.message.channel.id, mode=mode)
                
        if targetPug == None:
            targetPug = self.pugInfo

        if targetPug.gameServer.utQueryStatsActive or targetPug.gameServer.utQueryReporterActive:
            if self.utReporterChannel is None:
                await ctx.send('UT Reporter channel has not yet been configured, use **!setreporter** to configure the target channel.')
            elif self.utReporterChannel != ctx.message.channel:
                await ctx.send('UT Reporter is already active in another channel.')
            else:
                await ctx.send('UT Reporter is already active in this channel.')
        else:
            if targetPug.gameServer.utQueryServer('info'):
                self.utReporterChannel = ctx.message.channel
                if 'code' in targetPug.gameServer.utQueryData and targetPug.gameServer.utQueryData['code'] == 200:
                    targetPug.gameServer.utQueryStatsActive = True
                    targetPug.gameServer.utQueryReporterActive = True
                    await ctx.send('Force-started UT Reporter threads in this channel')
        return

async def setup(bot):
    await bot.add_cog(PUG(bot, DEFAULT_CONFIG_FILE))
