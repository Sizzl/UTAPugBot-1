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
DEFAULT_MAPS = 7
DEFAULT_PICKMODETEAMS = 1 # Fairer for even numbers (players should be even, 1st pick gets 1, 2nd pick gets 2)
DEFAULT_PICKMODEMAPS = 3 # Fairer for odd numbers (maps are usually odd, so 2nd pick should get more picks)

DEFAULT_GAME_SERVER_REF = 'pugs1'
DEFAULT_GAME_SERVER_IP = '0.0.0.0'
DEFAULT_GAME_SERVER_PORT = '7777'
DEFAULT_GAME_SERVER_NAME = 'Unknown Server'
DEFAULT_POST_SERVER = 'https://utassault.net'
DEFAULT_POST_TOKEN = 'NoToken'
DEFAULT_THUMBNAIL_SERVER = '{0}/pugstats/images/maps/'.format(DEFAULT_POST_SERVER)
DEFAULT_CONFIG_FILE = 'servers/config.json'
DEFAULT_RATING_FILE = 'players/ratings.json'

# Valid modes with default config
Mode = collections.namedtuple('Mode', 'isRanked minPlayers maxPlayers friendlyFireScale gameType mutators')
MODE_CONFIG = {
    'stdAS': Mode(False, 2, 20, 0, 'LeagueAS140.LeagueAssault', None),
    'proAS': Mode(False, 2, 20, 100, 'LeagueAS140.LeagueAssault', None),
    'ASplus': Mode(False, 2, 20, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlus'),
    'rASplus': Mode(True, 8, 14, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlus,rAS140.RankedAS'),
    'proASplus': Mode(False, 2, 20, 100, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.ASPlus'),
    'iAS': Mode(False, 2, 20, 0, 'LeagueAS140.LeagueAssault', 'LeagueAS-SP.iAS'),
    'ZPiAS': Mode(False, 2, 20, 0, 'LeagueAS140.LeagueAssault', 'ZeroPingPlus103.ColorAccuGib')
}

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
UP = '\U0001F53A'
DN = '\U0001F53B'

DISCORD_MD_CHARS = '*~_`'
DISCORD_MD_ESCAPE_RE = re.compile('[{}]'.format(DISCORD_MD_CHARS))
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
            msg.append('{} years'.format(int(y[0])))
        if d[0] > 0:
            msg.append('{} days'.format(int(d[0])))
        if h[0] > 0:
            msg.append('{} hours'.format(int(h[0])))
        if m[0] > 0:
            msg.append('{} minutes'.format(int(m[0])))
        msg.append('{} seconds'.format(int(s[0])))
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
        self.playerFlags = []
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
        return '[{}/{}]'.format(self.numPlayers, self.maxPlayers)

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
                log.debug('addPlayer() - Adding player {0} to pug queue.'.format(player.display_name))
                self.queuedPlayers.append(player)
                return True
        else:
            if player not in self and not self.playersFull:
                self.players.append(player)
                if len(flags):
                    self.playerFlags.append({player.id: flags})
                return True
        return False

    def addRankedPlayer(self, player, flags: str = ''):
        # Determine eligibility and ratings data present, perform any other checks here
        log.debug('addRankedPlayer({0}) started'.format(player.display_name))
        if self.checkRankedPlayersEligibility([player]):
            if self.addPlayer(player, flags):
                log.debug('addRankedPlayer({0}) succeeded.'.format(player.display_name))
                return True
        return False

    def removePlayer(self, player):
        if player in self:
            self.players.remove(player)
            if player.id in self.playerFlags:
                self.playerFlags.remove(player.id)
            return True
        return False

    def resetPlayers(self, includeQueuedPlayers: bool = False):
        self.players = []
        self.playerFlags = []
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
                log.debug('checkRankedPlayersEligibility({0}) - registrations list not present in ratingsData.'.format(players))
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
                    listedMaps.append('**{0})** __{1}__'.format(idx, x))
                else:
                    listedMaps.append('**{0})** ~~{1}~~'.format(idx, x))
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
                return PLASEP.join('{1}'.format(*x) for x in uniqMaps)
            else:
                return PLASEP.join('**{0})** {1}'.format(*x) for x in indexedMaps)

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
                    log.debug('autoPickRankedMaps() - Reverted to full maplist for pick {0} of {1} [order preference {2}] - {3}'.format(str(len(simulatedMaps)+1),str(self.maxMaps),str((i+1)),pick))
                if pick not in [None,'']:
                    mapTotals = 0
                    for value in mapRatios.values():
                        mapTotals += value
                    mapDiv = gcd(mapRatios[pick],mapTotals)
                    mapRatio = '{0}:{1}'.format(str(mapRatios[pick]//mapDiv),str(mapTotals//mapDiv))
                    if simulate:
                        log.debug('autoPickRankedMaps() - Simulating map pick {0} of {1} [order preference {2}] - {3}; slot chances - {4}'.format(str(len(simulatedMaps)+1),str(self.maxMaps),str((i+1)),pick,mapRatio))
                        simulatedMaps.append(pick)
                        simulatedMapsStr.append(pick)
                        # simulatedMapsStr.append('{0} *({1} chance)*'.format(pick,mapRatio))
                    else:
                        log.debug('autoPickRankedMaps() - Adding map {0} of {1} [order preference {2}] - {3}; slot chances - {4}'.format(str(len(self.maps)+1),str(self.maxMaps),str((i+1)),pick,mapRatio))
                        self.maps.append(pick)
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
            for m in self.mapListWeighting:
                log.debug('adjustRankedMapDesirability() - resetting {0} to default desirability'.format(m['map']))
                m['desirability'] = m['weight']*self.desirabilityMultiplier
            return True
        elif action.lower() == 'mapincrease' or action.lower() == 'mapdecrease':
            if len(map):
                for m in self.mapListWeighting:
                    if str(m['map']).lower() == map.lower():
                        if action.lower() == 'mapdecrease':
                            newDesirability = min(m['desirability']/max(1,adjustment),m['weight']*self.desirabilityMultiplier)
                        else:
                            newDesirability = min(m['desirability']*max(1,adjustment),m['weight']*self.desirabilityMultiplier)
                        log.debug('adjustRankedMapDesirability() - adjusting {0} desirability; from: {1}, to: {2}'.format(m['map'],m['desirability'],newDesirability))
                        m['desirability'] = newDesirability
            if newDesirability > 0:
                return True
            return False
        else:
            log.debug('adjustRankedMapDesirability() called for maplist length - {0}'.format(str(len(self.maps))))
            for pick in self.maps:
                for m in self.mapListWeighting:
                    if m['map'] == pick:
                        if 'desirability' not in m:
                            log.debug('adjustRankedMapDesirability() - resetting {0} to default desirability'.format(pick))
                            m['desirability'] = m['weight']*self.desirabilityMultiplier
                        else:
                            log.debug('adjustRankedMapDesirability() - reverting {0} desirability to {1}'.format(pick,str(int(round(m['desirability']*self.desirabilityReduction,0)))))
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
        super().__init__(maxPlayers, ranked, roleRequired)
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
    def __init__(self, configFile=DEFAULT_CONFIG_FILE, parent=None):
        # Initialise the class with hardcoded defaults, then parse in JSON config
        self.parent = parent
        self.configFile = configFile
        self.configMaps = []

        # All servers
        self.allServers = DEFAULT_SERVER_LIST
        
        # POST server, game server and map thumbnails / info:
        self.postServer = DEFAULT_POST_SERVER
        self.authtoken = DEFAULT_POST_TOKEN
        self.thumbnailServer = DEFAULT_THUMBNAIL_SERVER

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

        self.loadConfig(configFile)
        self.validateServers()
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
    def loadConfig(self, configFile):
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
                    log.info('Loaded {0} maps from config.json'.format(len(info['maplist'])))
                    self.configMaps = info['maplist']
                else:
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
                log.error('GameServer: Config file could not be loaded: {0}'.format(configFile))
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
            self.udpSock.sendto(str.encode('\\{0}\\'.format(queryType)),(self.utQueryData['ip'], self.utQueryData['query_port']))
            udpData = []
            while True:
                if queryType == 'consolelog': # Larger buffer required for consolelog
                    udpRcv, _ = self.udpSock.recvfrom(65536)
                else:
                    udpRcv, _ = self.udpSock.recvfrom(4096)
                try:
                    udpData.extend(udpRcv.decode('utf-8','ignore').split('\\')[1:-2]) 
                except UnicodeDecodeError as e:
                    log.error('UDP decode error: {0}'.format(e.reason))
                    log.debug('Attempted sending UDP query {0} to {1}:{2}.'.format(queryType, self.utQueryData['ip'], self.utQueryData['query_port']))
                    return
                if udpRcv.split(b'\\')[-2] == b'final':
                    break
            parts = zip(udpData[::2], udpData[1::2])
            for part in parts:
                self.utQueryData[part[0]] = part[1]
            self.utQueryData['code'] = 200
            self.utQueryData['lastquery'] = int(time.time())
        except socket.timeout:
            log.error('UDP socket timeout when connecting to {0}:{1} to perform a query: {2}'.format(self.utQueryData['ip'], self.utQueryData['query_port'],queryType))
            self.utQueryData['status'] = 'Timeout connecting to server.'
            self.utQueryData['code'] = 408
            self.utQueryData['lastquery'] = 0

        return True

    #########################################################################################
    # Formatted JSON
    #########################################################################################
    @property
    def format_post_header_auth(self):
        fmt = {
                'Content-Type': 'application/json; charset=UTF-8',
                'PugAuth': '{}'.format(self.authtoken),
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
        fmt.update({'Mode': 'remote{0}'.format(state)})
        return fmt

    def format_post_body_serverref(self, serverref: str = ''):
        if len(serverref) == 0:
            serverref = self.gameServerRef
        fmt = {
            'server': serverref
        }
        return fmt

    def format_post_body_setup(self, numPlayers: int, maps, mode: str):
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
        return '{0}'.format(serverName)
    
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
            'CN':':flag_cn:',
            'XX':':pirate_flag:',
            'GP':':rainbow_flag:',
            'US':':flag_us:'
        }
        msg = []
        i = 0
        for s in self.allServers:
            i += 1
            servername = '{0}'.format(s[1])
            for flag in flags:
                servername  = re.compile(flag).sub(flags[flag], servername)
            msg.append('{0}. {1} - {2}'.format(i, servername, s[2]))
        return '\n'.join(msg)

    @property
    def format_gameServerURL(self):
        return 'unreal://{0}:{1}'.format(self.gameServerIP, self.gameServerPort)

    @property
    def format_gameServerURL_red(self):
        return '{0}{1}{2}'.format(self.format_gameServerURL, '?password=', self.redPassword)

    @property
    def format_gameServerURL_blue(self):
        return '{0}{1}{2}'.format(self.format_gameServerURL, '?password=', self.bluePassword)

    @property
    def format_gameServerURL_spectator(self):
        return '{0}{1}{2}'.format(self.format_gameServerURL, '?password=', self.spectatorPassword)

    @property
    def format_gameServerState(self):
        return '{0}'.format(self.gameServerState)

    @property
    def format_server_info(self):
        fmt = '{0} | {1}'.format(self.gameServerName, self.format_gameServerURL)
        return fmt

    @property
    def format_red_password(self):
        fmt = 'Red team password: **{}**'.format(self.redPassword)
        return fmt

    @property
    def format_blue_password(self):
        fmt = 'Blue team password: **{}**'.format(self.bluePassword)
        return fmt

    @property
    def format_spectator_password(self):
        fmt = 'Spectator password: **{}**'.format(self.spectatorPassword)
        return fmt

    @property
    def format_game_server(self):
        fmt = 'Pug Server: **{}**'.format(self.format_gameServerURL)
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
                log.error('Invalid JSON returned from server, URL: {0} HTTP response: {1}; content:{2}'.format(r.url,r.status_code,r.content))
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
                            self.updateServerReference(sv['serverRef'], sv['serverName'],'unreal://{0}:{1}'.format(sv['serverAddr'], sv['serverPort']), sv['cloudManaged'], sv['serverStatus']['Summary'])

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
        log.debug('Posting "Check" to API {0} - {1}'.format(self.postServer,body))
        r = self.makePostRequest(self.postServer, self.format_post_header_check, body)
        log.debug('Received data from API - Status: {0}; Content-Length: {1}'.format(r.status_code,r.headers['content-length']))
        self.lastUpdateTime = datetime.now()
        if(r):
            return r.json()
        else:
            return None

    def updateServerStatus(self, ignorematchStarted: bool = False):
        log.debug('Running updateServerStatus')
        info = self.getServerStatus()
        log.debug('updateServerStatus - info fetched')
        log.debug('serverStatus: {0}'.format(info['serverStatus']))
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
        log.debug('Running controlOnDemandServer-{0} for {1}...'.format(state,serverref))
        if state not in [None, 'stop','halt','shutdown']:
            if not self.updateServerStatus(True): # or self.matchInProgress:
                return None

        headers = self.format_post_header_control(state)
        body = self.format_post_body_serverref(serverref)
        log.debug('Posting "Remote{0}" to API {1} - {2}'.format(state,self.postServer,body))
        r = self.makePostRequest(self.postServer, headers, body)
        log.debug('Received data from API - Status: {0}; Content-Length: {1}'.format(r.status_code,r.headers['content-length']))
        if(r):
            log.debug('controlOnDemandServer-{0} returned JSON info...'.format(state))
            info = r.json()
            return info
        else:
            log.error('controlOnDemandServer-{0} failed.'.format(state))
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

    def setupMatch(self, numPlayers, maps, mode):
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
        body = self.format_post_body_setup(numPlayers, maps, mode)

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
        log.debug('endMatch (viaReset={0}): Ended = {1}. matchCode = {2}, redScore = {3} - blueScore = {4}'.format(viaReset, self.endMatchPerformed, self.matchCode, self.redScore, self.blueScore))
        if self.endMatchPerformed is True:
            if self.parent.storeLastPug('**Score:** Red {0} - {1} Blue'.format(self.redScore, self.blueScore), self.redScore, self.blueScore, self.lastMatchCode, viaReset):
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
                self.lastMatchCode = '{0}'.format(self.matchCode)
                self.matchCode = ''
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
            newServer = int(self.gameServerRotation[int('{:0}{:0>2}'.format(datetime.now().year,datetime.now().isocalendar()[1]))%len(self.gameServerRotation)])-1
            if self.gameServerRef != self.allServers[newServer][0]:
                log.debug('checkServerRotation - Updating current server to: {0}'.format(self.allServers[newServer][1]))
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
            await ctx.send('{0} is ready for action.'.format(self.parent.gameServer.gameServerName))
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
    def __init__(self, numPlayers, numMaps, pickModeTeams, pickModeMaps, configFile=DEFAULT_CONFIG_FILE, ratingsFile=DEFAULT_RATING_FILE):
        super().__init__(numPlayers, pickModeTeams)
        self.name = 'Assault'
        self.mode = 'stdAS'
        self.lastPlayedMode = 'stdAS'
        self.matchReportPending = False
        self.desc = self.name + ': ' + self.mode + ' PUG'
        self.servers = [GameServer(configFile,self)]
        self.serverIndex = 0
        
        self.ranked = False
        self.redPower = 0
        self.bluePower = 0
        self.ratings = None
        self.ratingsFile = ratingsFile
        self.ratingsSyncAPI = {'matchDataURL':'','ratingsDataURL':'','playerDataURL':'','apiKey':''}

        self.maps = PugMaps(numMaps, pickModeMaps, self.ranked, self.servers[self.serverIndex].configMaps)
        self.roleRequired = None
        self.lastPugStr = 'No last pug info available.'
        self.lastPugTimeStarted = None
        self.pugLocked = False
        self.pugTempLocked = False

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
        return PLASEP.join(fmt.format(*x) for x in numberedPlayers)

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

    def format_teams(self, number: bool = False, mention: bool = False):
        teamsStr = '**Red Team:** {}\n**Blue Team:** {}'
        red = self.format_red_players(number=number, mention=mention)
        blue = self.format_blue_players(number=number, mention=mention)
        return teamsStr.format(red, blue)

    @property
    def format_pug_short(self):
        fmt = '**__{0.desc} [{1}/{0.maxPlayers}] \|\| {2} \|\| {3} maps__**'
        return fmt.format(self, len(self), self.gameServer.gameServerName, self.maps.maxMaps)

    def format_pug(self, number=True, mention=False):
        fmt = '**__{0.desc} [{1}/{0.maxPlayers}] \|\| {2} \|\| {3} maps:__**\n{4}'
        return fmt.format(self, len(self), self.gameServer.gameServerName, self.maps.maxMaps, self.format_all_players(number=number, mention=mention))

    @property
    def format_match_is_ready(self):
        fmt = ['Match is ready:']
        fmt.append(self.format_teams(mention=True))
        fmt.append('Maps ({}):\n{}'.format(self.maps.maxMaps, self.maps.format_current_maplist))
        fmt.append(self.gameServer.format_game_server)
        fmt.append(self.gameServer.format_spectator_password)
        return '\n'.join(fmt)

    @property
    def format_match_in_progress(self):
        if self.pugLocked:
            if not self.matchReady:
                # Handles the case when the bot has been restarted so doesn't have previous info.
                # Could improve this in future by caching the state to disk when shutting down and loading back in on restart.
                return 'Match is in progress, but do not have previous pug info. Please use **!serverstatus** to monitor this match'

            fmt = ['Match in progress ({} ago):'.format(getDuration(self.lastPugTimeStarted, datetime.now()))]
            fmt.append(self.format_teams(mention=False))
            if self.ranked:
                fmt.append('Red RP: {0}; Blue RP: {1}'.format(str(self.redPower),str(self.bluePower)))
            fmt.append('Maps ({}): {}'.format(self.maps.maxMaps, self.maps.format_current_maplist))
            fmt.append('Mode: ' + self.mode+' @ '+self.gameServer.format_game_server)
            fmt.append(self.gameServer.format_spectator_password)
            if len(self.queuedPlayers):
                fmt.append('Queued players for next pug: {}'.format(self.format_queued_players(mention=False)))
            return '\n'.join(fmt)
        return None

    @property
    def format_last_pug(self):
        if self.lastPugTimeStarted and '{}' in self.lastPugStr:
            return self.lastPugStr.format(getDuration(self.lastPugTimeStarted, datetime.now()))
        else:
            return 'No last pug info available.'

    @property
    def format_list_servers(self):
        indexedServers = ((i,s) for i,s in enumerate(self.servers, 1) if s)
        fmt = []
        for x in indexedServers:
            fmt.append('**{0})** {1}'.format(str(x[0]), x[1].format_server_info))

        return '\n'.join(fmt)

    #########################################################################################
    # Functions:
    #########################################################################################
    def addServer(self, serverfile: str):
        try:
            self.servers.add(GameServer(serverfile))
            return True
        except:
            return False

    def removeServer(self, index: int):
        if index >= 0 and index < len(self.servers):
            self.servers.pop(index)
            if self.serverIndex == index and len(self.servers) > 0:
                self.serverIndex = 0

    def removePlayerFromPug(self, player):
        if player in self.queuedPlayers:
            if player.id in self.playerFlags:
                self.playerFlags.remove(player.id)
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
            # Try to set up 5 times with a 5s delay between attempts.
            result = False
            self.pugTempLocked = True
            for x in range(0, 5):
                result = self.gameServer.setupMatch(self.maxPlayers, self.maps.maps, self.mode)
                log.debug('Setup attempt {0}/5: Result returned: {1}'.format(x+1,result))
                if not result:
                    time.sleep(5)
                else:
                    self.pugLocked = True
                    if self.gameServer.matchCode in [None,'']:
                        # Generate a temporary match code which can be updated later
                        self.gameServer.matchCode = 'temp-{0}'.format(datetime.now().strftime('%Y%m%d%H%M%S'))
                    self.storeLastPug(matchCode=self.gameServer.matchCode)
                    return True
            self.pugTempLocked = False
        return False

    def storeLastPug(self, appendstr: str = '', redScore: int = 0, blueScore: int = 0, matchCode: str = '', viaReset: bool = False):
        if self.matchReady:
            fmt = []
            if matchCode in [None,'']:
                if self.gameServer.matchCode in [None,'']:
                    self.gameServer.matchCode = 'temp-{0}'.format(datetime.now().strftime('%Y%m%d%H%M%S'))
                matchCode = self.gameServer.matchCode
            fmt.append('Last **{}** ({} ago)'.format(self.desc, '{}'))
            fmt.append(self.format_teams())
            if self.ranked:
                fmt.append('Red RP: {0}; Blue RP: {1}'.format(str(self.redPower),str(self.bluePower)))
            fmt.append('Maps ({}):\n{}'.format(self.maps.maxMaps, self.maps.format_current_maplist))
            self.lastPugStr = '\n'.join(fmt)
            self.lastPugTimeStarted = datetime.now()
            self.lastPlayedMode = self.mode
            if self.ranked:
                log.debug('storeLastPug(viaReset={5}) - Calling via matchReady - storeRankedPug({0},{1},{2},{3},{4})'.format(self.mode, matchCode, str(redScore), str(blueScore), self.lastPugTimeStarted, str(False), str(viaReset)))
                if self.storeRankedPug(self.mode, matchCode, redScore, blueScore, self.lastPugTimeStarted.isoformat(), False):
                    log.debug('storeRankedPug() - Stored game successfully via storeLastPug matchReady')
                else:
                    log.debug('storeRankedPug() - Failed to store game successfully via storeLastPug matchReady')
            return True
        elif len(appendstr):
            fmt = []
            fmt.append(self.lastPugStr)
            fmt.append(appendstr)
            self.lastPugStr = '\n'.join(fmt)
            if self.ranked:
                log.debug('storeLastPug(viaReset={5}) - Calling storeRankedPug({0},{1},{2},{3},{4})'.format(self.mode, matchCode, str(redScore), str(blueScore), self.lastPugTimeStarted, str(False), str(viaReset)))
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
        self.pugTempLocked = True
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
        self.pugTempLocked = False
        self.pugLocked = False
        if self.ranked:
            self.setRankedMode(self.ranked, True)
        return True
    
    def setRankedMode(self, rankedMode: bool, skipResets: bool = False):
        # Perform any checks needed when switching between ranked and non-ranked modes
        self.maps.rankedMode = self.ranked = False
        self.maps.filteredMapsList = self.maps.availableMapsList
        self.ratings = None
        self.roleRequired = None
        if rankedMode == False and self.ratingsFile != '':
            log.debug('setRankedMode({0}) - Calling savePugRatings({1})'.format(rankedMode,self.ratingsFile))
            self.savePugRatings(self.ratingsFile)
        if skipResets != True:
            log.debug('setRankedMode({0}) - Calling softPugTeamReset()'.format(rankedMode))
            self.softPugTeamReset() # clear any caps / picks
            log.debug('setRankedMode({0}) - Calling configurePlayersRankedMode({1},{2})'.format(rankedMode,self.ranked,self.roleRequired))
            self.configurePlayersRankedMode(self.ranked, self.roleRequired) # reconfigure teams and players
            log.debug('setRankedMode({0}) - Calling maps.resetMaps()'.format(rankedMode))
            self.maps.resetMaps() # reset map selection
        if rankedMode and self.ratingsFile != '':
            log.debug('setRankedMode({0}) - Calling loadPugRatings({1})'.format(rankedMode,self.ratingsFile))
            if self.loadPugRatings(self.ratingsFile):
                log.debug('setRankedMode({0}) - Checking self.ratings...'.format(rankedMode))
                if self.ratings is not None:
                    log.debug('setRankedMode({0}) - self.ratings present.'.format(rankedMode))
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
                    log.debug('setRankedMode({0}) - Calling checkRankedPlayersEligibility({1})'.format(rankedMode,self.players))
                    if self.checkRankedPlayersEligibility(self.players):
                        self.maps.rankedMode = self.ranked = True
                        log.debug('setRankedMode({0}) - Ranked mode setup, calling configurePlayersRankedMode({1},{2},(JSON))'.format(rankedMode,self.ranked,self.roleRequired))
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
                            if 'maplist' in self.ratings['maps']:
                                # build a filtered map list array from this data
                                self.maps.filteredMapsList = []
                                for x in self.ratings['maps']['maplist']:
                                    self.maps.filteredMapsList.append(x['map'])
                                self.maps.filteredMapsList = list(set(self.maps.filteredMapsList))
                                self.maps.mapListWeighting = self.ratings['maps']['maplist']
        return self.ranked
    
    def setMode(self, requestedMode: str):
        # Dictionaries are case sensitive, so we'll do a map first to test case-insensitive input, then find the actual key after
        if requestedMode.upper() in map(str.upper, MODE_CONFIG):
            ## Iterate through the keys to find the actual case-insensitive mode
            requestedMode = next((key for key, value in MODE_CONFIG.items() if key.upper()==requestedMode.upper()), None)
            lastMode = self.mode
            ## ProAS and iAS are played with a different maximum number of players.
            ## Can't change mode from std to pro/ias if more than the maximum number of players allowed for these modes are signed.
            if len(self.players) > MODE_CONFIG[requestedMode].maxPlayers:
                return False, str(MODE_CONFIG[requestedMode].maxPlayers) + ' or fewer players must be signed for a switch to ' + requestedMode
            else:
                ## If max players is more than mode max and there aren't more than mode max players signed, automatically reduce max players to mode max.
                if self.maxPlayers > MODE_CONFIG[requestedMode].maxPlayers:
                    self.setMaxPlayers(MODE_CONFIG[requestedMode].maxPlayers)
                self.mode = requestedMode
                additionalInfo = ''
                if MODE_CONFIG[requestedMode].isRanked:
                    log.debug('Setting up ranked mode - {0}'.format(requestedMode))
                    if self.setRankedMode(MODE_CONFIG[requestedMode].isRanked, False):
                        self.desc = 'Ranked Assault (' + self.mode + ') PUG'
                        additionalInfo = ' (ranked, best of '+str(self.maps.maxMaps)+' maps)'
                    else:
                        log.debug('setRankedMode({0}) failed'.format(MODE_CONFIG[requestedMode].isRanked))
                        self.mode = lastMode # Revert if ratings failed to load for this game mode    
                else:
                    self.setRankedMode(False, False)
                if self.ranked != True:
                    self.desc = 'Assault (' + self.mode + ') PUG'
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
        self.ratings = None # save before load?
        log.debug('loadPugRatings({0}) started'.format(ratingsFile))
        with open(ratingsFile) as f:
            try:
                ratingsData = json.load(f)
            except:
                ratingsData = None
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
                            log.debug('loadPugRatings({0}) stored ratings data for {1}'.format(ratingsFile,self.mode))
                            return True
                else:
                    # Generate an empty ranked schema with the default mode
                    rkData = {
                        'syncapi': self.ratingsSyncAPI,
                        'rankedgames': [
                            {
                                'mode':'rASPlus',
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
                log.debug('loadPugRatings({0}) ratingsData is not a valid object'.format(ratingsFile))
        return False
        
    def savePugRatings(self, ratingsFile, ratingsUpdates = None):
        """Saves the ranked game ratings data to the JSON configuration file"""
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
                        log.debug('savePugRatings({0}) updating ratingsData directly from provided ratingsUpdates.'.format(ratingsFile))
                        ratingsData['rankedgames']  = ratingsUpdates['rankedgames'] # pass valid data straight into the file
                else:
                    if self.ratings not in [None,'']:
                        if ratingsData['rankedgames'] is not None:
                            for gamedata in ratingsData['rankedgames']:
                                if gamedata['mode'] == self.ratings['mode']:
                                    self.ratings['lastupdated'] = datetime.now().isoformat()
                                    for key in ['maps','eligibility','registrations','ratings','lastsync','fixedpicklimit','capMode','capWindow','capRole','games','scoring','lastupdated']:
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
                        log.warning('savePugRatings({0}) failed to generate ratingsData. Cached ratings not present and updates not provided.'.format(ratingsFile))
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
        log.debug('makeRatedTeams() - playerIDs = {0}; playerRatings = {1}'.format(playerIDs,playerRatings))
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
        log.debug('makeRatedTeams() masked values: red={0}, blue={1}'.format(rankedRed,rankedBlue))
        if len(simulatedRatings) == 0:
            self.redPower = sum(rankedRed)
            self.bluePower = sum(rankedBlue)
            msg = 'Red RP: {0}; Blue RP: {1}'.format(str(self.redPower),str(self.bluePower))
        else:
            msg = 'Red RP: {0}; Blue RP: {1}'.format(str(sum(rankedRed)),str(sum(rankedBlue)))
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
                    msg = msg+'\nRed captain: {0}\nBlue captain: {1}'.format(redCap.mention,blueCap.mention)
        if len(simulatedRatings):
            msg = msg+'\nSimulated Red team: {0}'.format(PLASEP.join(sp['name'] for sp in simRed))
            msg = msg+'\nSimulated Blue team: {0}'.format(PLASEP.join(sp['name'] for sp in simBlue))
        log.debug('makeRatedTeams() completed: {0}'.format(msg.replace('\n','; ')))
        return msg

    def storeRankedPug(self, mode: str = '', matchCode: str = '', redScore: int = 0, blueScore: int = 0, timeStarted: str = '', hasEnded: bool = False, redPlayers: list = [], bluePlayers: list = [], maps: list = [], redPower = 0, bluePower = 0, timeEnded = ''):
        """Stores ranked pug match data and handles end-game scenarios"""
        if mode in [None, ''] and self.ranked:
            mode = self.mode
        if matchCode in [None,'']:
            if self.gameServer.matchCode in [None,'']:
                self.gameServer.matchCode = 'temp-{0}'.format(datetime.now().strftime('%Y%m%d%H%M%S'))
            matchCode = self.gameServer.matchCode
            log.debug('storeRankedPug() - grabbed mode from gameServer.matchCode - {0}'.format(matchCode))
        if matchCode in [None,'']:
            return False
        if hasEnded:
            timeEnded = datetime.now().isoformat()
        self.savePugRatings(self.ratingsFile)
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
                    self.setRankedMode(self.ranked, True)
        return True

    def applyRankedScoring(self, rkData: object, mode: str, match: object, void: bool = False, player: int = 0):
        """Searches for the given match code and applies given scoring logic to players"""
        # TO-DO Add voluntary captain scoring when capmode 3 is supported
        winners = []
        losers = []
        modeData = {}
        winCap = 0
        loseCap = 0
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
                winCap = match['capred']['id']
                loseCap = match['capblue']['id']
            elif match['scoreblue'] > match['scorered']:
                winners = match['teamblue']
                losers = match['teamred']
                winScore = match['scoreblue']
                loseScore = match['scorered']
                winCap = match['capblue']['id']
                loseCap = match['capred']['id']
            if void:
                match['completed'] = False
                log.debug('applyRankedScoring() - Voided match {0}'.format(match['gameref']))
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
                                if p['did'] == winCap:
                                    p['ratingvalue'] = p['ratingvalue']+capWinRP
                            elif p['did'] in losers:
                                p['ratingvalue'] = p['ratingvalue']+loseRP
                                if p['did'] == loseCap:
                                    p['ratingvalue'] = p['ratingvalue']+capLoseRP
                            if 'ratinghistory' in p:
                                p['ratinghistory'] = sorted(p['ratinghistory'], key=lambda g: datetime.fromisoformat(g['matchdate'])) 
                                if len(p['ratinghistory']) > 150:
                                    p['ratinghistory'][:] = p['ratinghistory'][-150:]
                            p['lastgamedate'] = match['startdate']
                            p['lastgameref'] = match['gameref']
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
        log.debug('convertQueuedPlayers() - Queue length: {0}; Pug Locked: {1}'.format(str(len(self.queuedPlayers)), str(self.pugLocked)))
        if len(self.queuedPlayers):
            self.players = self.queuedPlayers
        self.queuedPlayers = []
        return True

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
        self.pugInfo = AssaultPug(DEFAULT_PLAYERS, DEFAULT_MAPS, DEFAULT_PICKMODETEAMS, DEFAULT_PICKMODEMAPS, configFile)
        self.configFile = configFile

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

    def cog_unload(self):
        self.updateGameServer.cancel()
        self.sendMatchReport.cancel()
        self.updateUTQueryReporter.cancel()
        self.updateUTQueryStats.cancel()
        self.updateGuildEmojis.cancel()
        self.updateServerRotation.cancel()

#########################################################################################
# Loops.
#########################################################################################
    @tasks.loop(seconds=60.0)
    async def updateGameServer(self):
        queueCheck = False
        if self.pugInfo.pugLocked:
            log.info('Updating game server [pugLocked=True]..')
            if not self.pugInfo.gameServer.updateServerStatus():
                log.warning('Cannot contact game server.')
            if len(self.pugInfo.queuedPlayers):
                queueCheck = True
            if self.pugInfo.gameServer.processMatchFinished():
                self.savePugConfig(self.configFile)
                msg = 'Match finished. Resetting pug'
                if (self.pugInfo.ranked):
                    msg = msg+' and updating player RP.'
                    await self.activeChannel.send(msg)
                else:
                    msg = msg+'...'
                    await self.activeChannel.send(msg)
                if self.pugInfo.resetPug():
                    await self.activeChannel.send(self.pugInfo.format_pug())
                    log.info('Match over.')
                    if queueCheck and self.pugInfo.playersFull:
                        await self.activeChannel.send('Queued players have been added and the pug is full. When ready, start the next pug by sending !pug')
                    return
                await self.activeChannel.send('Reset failed.')
                log.error('Reset failed')

    @updateGameServer.before_loop
    async def before_updateGameServer(self):
        log.info('Waiting before updating game server...')
        await self.bot.wait_until_ready()
        log.info('Ready.')

    @tasks.loop(seconds=4.0)
    async def updateUTQueryReporter(self):
        if self.pugInfo.gameServer.utQueryReporterActive and self.utReporterChannel is not None:
            await self.queryServerConsole()
        return

    @updateUTQueryReporter.before_loop
    async def before_updateUTQueryReporter(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=60.0)
    async def updateUTQueryStats(self):
        if self.utReporterChannel is not None:
            if self.pugInfo.gameServer.utQueryStatsActive:
                if ('laststats' not in self.pugInfo.gameServer.utQueryData) or ('laststats' in self.pugInfo.gameServer.utQueryData and int(time.time())-int(self.pugInfo.gameServer.utQueryData['laststats']) > 55):
                    await self.queryServerStats()
            elif self.pugInfo.gameServer.utQueryReporterActive and self.pugInfo.pugLocked:
                # Skip one cycle, then re-enable stats
                self.pugInfo.gameServer.utQueryStatsActive = True
        return

    @updateUTQueryStats.before_loop
    async def before_updateUTQueryStats(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=10.0)
    async def sendMatchReport(self):
        if self.pugInfo.matchReportPending and self.pugInfo.pugLocked != True:
            matchref = 'last'
            if len(self.pugInfo.gameServer.lastMatchCode):
                matchref = self.pugInfo.gameServer.lastMatchCode
            elif len(self.pugInfo.gameServer.matchCode):
                matchref = self.pugInfo.gameServer.matchCode
            log.debug('Sending match report to {0}; last mode={1}; last matchref={2};'.format(self.activeChannel,self.pugInfo.lastPlayedMode,matchref))
            self.pugInfo.matchReportPending = False
            await self.rkrp(self.activeChannel, mode=self.pugInfo.lastPlayedMode, matchref=matchref)
        return
    
    @sendMatchReport.after_loop
    async def on_sendMatchReport_cancel(self):
        self.pugInfo.matchReportPending = False

    @tasks.loop(minutes=5)
    async def updateGuildEmojis(self):
        self.cacheGuildEmojis()
        return
    
    @tasks.loop(hours=1)
    async def updateServerRotation(self):
        # Only auto-rotate between 6:00 and 9:59 am on a Monday
        if datetime.now().weekday() == 0 and datetime.now().hour >= 6 and datetime.now().hour <= 9 and not self.pugInfo.pugLocked:
            log.debug('updateServerRotation loop - calling checkServerRotation()')
            self.pugInfo.gameServer.checkServerRotation()
        return
#########################################################################################
# Utilities.
#########################################################################################
    def loadPugConfig(self, configFile):
        with open(configFile) as f:
            info = json.load(f)
            if info:
                if 'pug' in info and 'activechannelid' in info['pug']:
                    channelID = info['pug']['activechannelid']
                    channel = discord.Client.get_channel(self.bot, channelID)
                    log.info('Loaded active channel id: {0} => channel: {1}'.format(channelID, channel))
                    if channel:
                        self.activeChannel = channel
                        # Only load current info if the channel is valid, otherwise the rest is useless.
                        if 'current' in info['pug']:
                            if 'mode' in info['pug']['current']:
                                self.pugInfo.setMode(info['pug']['current']['mode'])
                            if 'playerlimit' in info['pug']['current']:
                                self.pugInfo.setMaxPlayers(info['pug']['current']['playerlimit'])
                            if 'maxmaps' in info['pug']['current']:
                                self.pugInfo.maps.setMaxMaps(info['pug']['current']['maxmaps'])
                            if 'timesaved' in info['pug']['current']:
                                time_saved = datetime.fromisoformat(info['pug']['current']['timesaved'])
                                # Only load signed players if timesaved is present and it is within 60 seconds of when the file was last saved.
                                # This is to avoid people thinking they were unsigned and causing a no-show.
                                if (datetime.now() - time_saved).total_seconds() < 60 and 'signed' in info['pug']['current']:
                                    players = info['pug']['current']['signed']
                                    if players:
                                        for player_id in players:
                                            player = self.activeChannel.guild.get_member(player_id)
                                            if player:
                                                self.pugInfo.addPlayer(player)
                        if 'lastpug' in info['pug']:
                            if 'pugstr' in info['pug']['lastpug']:
                                self.pugInfo.lastPugStr = info['pug']['lastpug']['pugstr']
                                if 'timestarted' in info['pug']['lastpug']:
                                    try:
                                        self.pugInfo.lastPugTimeStarted = datetime.fromisoformat(info['pug']['lastpug']['timestarted'])
                                    except:
                                        self.pugInfo.lastPugTimeStarted = None
                    else:
                        log.warning('No active channel id found in config file.')
                if 'pug' in info and 'reporterchannelid' in info['pug']:
                    channelID = info['pug']['reporterchannelid']
                    channel = discord.Client.get_channel(self.bot,channelID)
                    if channel:
                        self.utReporterChannel = channel
                if 'pug' in info and 'reporterconsolewatermark' in info['pug']:
                    self.pugInfo.gameServer.utQueryConsoleWatermark = info['pug']['reporterconsolewatermark']
            else:
                log.error('PUG: Config file could not be loaded: {0}'.format(configFile))
            f.close()
        return True

    def savePugConfig(self, configFile):
        with open(configFile) as f:
            info = json.load(f)
            if 'pug' in info and 'activechannelid' in info['pug']:
                last_active_channel_id = info['pug']['activechannelid']
            if 'pug' not in info:
                info['pug'] = {}
            if self.activeChannel:
                info['pug']['activechannelid'] = self.activeChannel.id
            else:
                info['pug']['activechannelid'] = 0
            if self.utReporterChannel:
                info['pug']['reporterchannelid'] = self.utReporterChannel.id
            else:
                info['pug']['reporterchannelid'] = 0
            if self.pugInfo.gameServer.utQueryConsoleWatermark > 0:
               info['pug']['reporterconsolewatermark'] = self.pugInfo.gameServer.utQueryConsoleWatermark
            else:
               info['pug']['reporterconsolewatermark'] = 0
            # Only save info about the current/last pugs if the channel id is valid and unchanged in this save.
            if self.activeChannel and self.activeChannel.id == last_active_channel_id:
                # current pug info:
                info['pug']['current'] = {}
                info['pug']['current']['timesaved'] = datetime.now().isoformat()
                info['pug']['current']['mode'] = self.pugInfo.mode
                info['pug']['current']['playerlimit'] = self.pugInfo.maxPlayers
                info['pug']['current']['maxmaps'] = self.pugInfo.maps.maxMaps
                if len(self.pugInfo.players) > 0:
                    info['pug']['current']['signed'] = []
                    for p in self.pugInfo.all:
                        if (p not in [None]):
                            info['pug']['current']['signed'].append(p.id)

                # last pug info:
                info['pug']['lastpug'] = {}
                if self.pugInfo.lastPugTimeStarted:
                    info['pug']['lastpug']['timestarted'] = self.pugInfo.lastPugTimeStarted.isoformat()
                if self.pugInfo.lastPugStr:
                    info['pug']['lastpug']['pugstr'] = self.pugInfo.lastPugStr
        with open(configFile,'w') as f:
            json.dump(info, f, indent=4)
        return True
   
    #########################################################################################
    # Formatted strings:
    #########################################################################################

    def format_pick_next_player(self, mention: bool = False):
        player = self.pugInfo.currentCaptainToPickPlayer
        return '{} to pick next player (**!pick <number>**)'.format(player.mention if mention else display_name(player))

    def format_pick_next_map(self, mention: bool = False):
        player = self.pugInfo.currentCaptainToPickMap
        return '{} to pick next map (use **!map <number>** to pick and **!listmaps** to view available maps)'.format(player.mention if mention else display_name(player))

    #########################################################################################
    # Functions:
    #########################################################################################

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        if type(error) is PugIsInProgress:
            # To handle messages returned when disabled commands are used when pug is already in progress.
            msg = ['Match is currently in progress.']
            if ctx.message.author in self.pugInfo:
                msg.append('{},  please, join the match or find a sub.'.format(ctx.message.author.mention))
                msg.append('If the match has just ended, please, wait at least 60 seconds for the pug to reset.')
            else:
                msg.append('Pug will reset when it is finished.')
            await ctx.send('\n'.join(msg))

    def isActiveChannel(self, ctx):
        return self.activeChannel is not None and self.activeChannel == ctx.message.channel
    
    async def checkOnDemandServer(self, ctx):
        if self.pugInfo.gameServer.gameServerState in ('N/A','N/AN/A') and self.pugInfo.gameServer.gameServerOnDemand is True:
            await ctx.send('Starting on-demand server: {0}...'.format(self.pugInfo.gameServer.gameServerName))
            info = self.pugInfo.gameServer.controlOnDemandServer('start')
            if (info):
                log.info('On-demand server start {0} returned: {1}'.format(self.pugInfo.gameServer.gameServerName,info['cloudManagementResponse']))
                return True
            else:
                log.error('Failed to start on-demand server: {0}'.format(self.pugInfo.gameServer.gameServerName))
                await ctx.send('Failed to start on-demand server: {0}. Select another server before completing map selection.'.format(self.pugInfo.gameServer.gameServerName))
                return False
        return True

    async def processPugStatus(self, ctx):
        # Big function to test which stage of setup we're at:
        if not self.pugInfo.playersFull:
            # Not filled, nothing to do.
            return

        # Work backwards from match ready.
        # Note match is ready once players are full, captains picked, players picked and maps picked.
        if self.pugInfo.mapsReady and self.pugInfo.matchReady:
            if self.pugInfo.pugTempLocked:
                # Avoid repeating a setup when multiple conditions are true
                return
            if self.pugInfo.ranked:
                msg = '\n'.join(['Ranked mode map selection complete. Setting up match now.',self.pugInfo.maps.format_current_maplist])
                self.pugInfo.pugTempLocked = True
                await ctx.send(msg)
            if self.pugInfo.gameServer.gameServerOnDemand and not self.pugInfo.gameServer.gameServerOnDemandReady:
                await ctx.send('Waiting for {0} to be ready for action...'.format(self.parent.gameServer.gameServerName))
            if self.pugInfo.setupPug():
                await self.sendPasswordsToTeams()
                await ctx.send(self.pugInfo.format_match_is_ready)
                self.pugInfo.gameServer.utQueryConsoleWatermark = self.pugInfo.gameServer.format_new_watermark
                self.pugInfo.gameServer.utQueryData = {}
                self.pugInfo.gameServer.utQueryReporterActive = True
                self.pugInfo.gameServer.utQueryStatsActive = True
                self.resetRequestRed = False # only need to reset this here because we only care about this when a match is in progress.
                self.resetRequestBlue = False # only need to reset this here because we only care about this when a match is in progress.
            else:
                await ctx.send('**PUG Setup Failed**. Use **!retry** to attempt setting up again with current configuration, or **!reset** to start again from the beginning.')
            return

        if self.pugInfo.teamsReady:
            # Need to pick maps.
            if (self.pugInfo.ranked):
                self.pugInfo.maps.autoPickRankedMaps()
                self.pugInfo.ratings['maps']['maplist'] = self.pugInfo.maps.mapListWeighting
                await self.processPugStatus(ctx) # loop back around
            else:
                await ctx.send(self.format_pick_next_map(mention=True))
            return
        
        if self.pugInfo.captainsReady:
            # Special case to display captains on the first pick.
            if len(self.pugInfo.red) == 1 and len(self.pugInfo.blue) == 1:
                await ctx.send(self.pugInfo.red[0].mention + ' is captain for the **Red Team**')
                await ctx.send(self.pugInfo.blue[0].mention + ' is captain for the **Blue Team**')
            # Need to pick players.
            msg = '\n'.join([
                self.pugInfo.format_remaining_players(number=True),
                self.pugInfo.format_teams(),
                self.format_pick_next_player(mention=True)])
            await ctx.send(msg)
            # Check server state and fire a start-up command if needed
            await self.checkOnDemandServer(ctx)
            return
        
        if self.pugInfo.numCaptains == 1:
            # Need second captain.
            await ctx.send('Waiting for 2nd captain. Type **!captain** to become a captain. To choose a random captain type **!randomcaptains**')
            return

        if self.pugInfo.playersReady:
            if self.pugInfo.ranked:
                # Logic is reversed, fill teams and then nominate random captains
                costmsg = self.pugInfo.makeRatedTeams()
                msg = '\n'.join(['Ranked teams have been established:',self.pugInfo.format_teams(),costmsg])
                await ctx.send(msg)
                await self.checkOnDemandServer(ctx)
                await self.processPugStatus(ctx) # loop back around
                return
            else:
                # Need captains.
                msg = ['**{}** has filled.'.format(self.pugInfo.desc)]
                if len(self.pugInfo) == 2 and self.pugInfo.playersFull:
                    # Special case, 1v1: assign captains instantly, so jump straight to map picks.
                    self.pugInfo.setCaptain(self.pugInfo.players[0])
                    self.pugInfo.setCaptain(self.pugInfo.players[1])
                    await ctx.send('Teams have been automatically filled.\n{}'.format(self.pugInfo.format_teams(mention=True)))
                    await ctx.send(self.format_pick_next_map(mention=False))
                    # Check server state and fire a start-up command if needed
                    await self.checkOnDemandServer(ctx)
                    return
                # Standard case, moving to captain selection.
                msg.append(self.pugInfo.format_pug(mention=True))
                # Need first captain
                msg.append('Waiting for captains. Type **!captain** to become a captain. To choose random captains type **!randomcaptains**')
            await ctx.send('\n'.join(msg))
            return

    async def sendPasswordsToTeams(self):
        if self.pugInfo.matchReady:
            msg_redPassword = self.pugInfo.gameServer.format_red_password
            msg_redServer = self.pugInfo.gameServer.format_gameServerURL_red
            msg_bluePassword = self.pugInfo.gameServer.format_blue_password
            msg_blueServer = self.pugInfo.gameServer.format_gameServerURL_blue
            for player in self.pugInfo.red:
                try:
                    await player.send('{0}\nJoin the server @ **{1}**'.format(msg_redPassword, msg_redServer))
                except:
                    await self.activeChannel.send('Unable to send password to {} - are DMs enabled? Please ask your teammates for the red team password.'.format(player.mention))
            for player in self.pugInfo.blue:
                try:
                    await player.send('{0}\nJoin the server @ **{1}**'.format(msg_bluePassword, msg_blueServer))
                except:
                    await self.activeChannel.send('Unable to send password to {} - are DMs enabled? Please ask your teammates for the blue team password.'.format(player.mention))
        if self.activeChannel:
            await self.activeChannel.send('Check private messages for server passwords.')
        return True

    async def isPugInProgress(self, ctx, warn: bool=False):
        if not self.isActiveChannel(ctx):
            return False
        if warn and self.pugInfo.pugLocked:
            log.warning('Raising PugIsInProgress')
            raise PugIsInProgress('Pug In Progress')
        return not self.pugInfo.pugLocked
    
    async def queryServerConsole(self):
        # Fetch watermark from previous messages
        consoleWatermark = self.pugInfo.gameServer.utQueryConsoleWatermark
        reportToChannel = self.utReporterChannel
        # Fetch console log
        if self.pugInfo.gameServer.utQueryServer('consolelog') and reportToChannel is not None:
            if 'code' in self.pugInfo.gameServer.utQueryData and self.pugInfo.gameServer.utQueryData['code'] == 200:
                if 'consolelog' in self.pugInfo.gameServer.utQueryData:
                    bReportScoreLine = False
                    # Attempt to serialize to JSON, otherwise if server doesn't support this, use simple string manipulation
                    try:
                        utconsole = json.loads(self.pugInfo.gameServer.utQueryData['consolelog'])
                    except:
                        utconsole = {}
                        utconsole['messages'] = str(self.pugInfo.gameServer.utQueryData['consolelog']).split('|')

                    for m in utconsole['messages']:
                        try:
                            # Message format: {"stamp":"20220101133700666", "type":"Say", "gametime":"120", "displaytime":"02:00", "message": ":robot::guitar:", "teamindex":"0", "team":"Red", "player":"Sizzl"}
                            if 'message' in m and 'stamp' in m and int(m['stamp']) > self.pugInfo.gameServer.utQueryConsoleWatermark:
                                if 'type' in m and m['type'] == 'Say':
                                    for em in self.customStaticEmojis:
                                        m['message']  = re.compile(em).sub('<{0}{1}>'.format(em,self.customStaticEmojis[em]), m['message'])
                                    for em in self.customAnimatedEmojis:
                                        m['message']  = re.compile(em).sub('<a{0}{1}>'.format(em,self.customAnimatedEmojis[em]), m['message'])
                                    if 'team' in m:
                                        if m['team'] == 'Spectator':
                                            await reportToChannel.send('[{0}] {1} (*{2}*): {3}'.format(m['displaytime'],m['player'].strip(),m['team'],m['message'].strip()))
                                        else:
                                            await reportToChannel.send('[{0}] {1} (**{2}**): {3}'.format(m['displaytime'],m['player'].strip(),m['team'],m['message'].strip()))
                                    else:
                                        await reportToChannel.send('[{0}] {1}: {2}'.format(m['displaytime'],m['player'].strip(),m['message'].strip()))
                                else:
                                    if re.search('1\sminutes\suntil\sgame\sstart|conquered\sthe\sbase|defended\sthe\sbase',m['message'],re.IGNORECASE) is not None:
                                        bReportScoreLine = True
                                    if len(m['message'].strip()) > 0:
                                        await reportToChannel.send('[{0}] {1}'.format(m['displaytime'],m['message'].strip()))
                                consoleWatermark = int(m['stamp'])
                        except:
                            try:
                                # Message format: 20220101133700666 [13:37] Player: Message
                                # We won't do any fancy replacements here, just drop the message verbatim.
                                stamp = int(m[:17])
                            except:
                                stamp = 0
                            if stamp > self.pugInfo.gameServer.utQueryConsoleWatermark:
                                await reportToChannel.send('{0}'.format(m[-(len(m)-18):]))
                                if re.search('1\sminutes\suntil\sgame\sstart|conquered\sthe\sbase|defended\sthe\sbase',m,re.IGNORECASE) is not None:
                                    bReportScoreLine = True
                            if stamp > 0:
                                consoleWatermark = stamp
                            else:
                                consoleWatermark = self.pugInfo.gameServer.format_new_watermark
                    self.pugInfo.gameServer.utQueryConsoleWatermark = consoleWatermark

                    if self.pugInfo.gameServer.utQueryStatsActive is False:
                        # Picking up a deferred stats request (from bReportScoreLine)
                        await self.queryServerStats()
                        # Reset the requirement for scoreline and re-enable the infrequent stats embed
                        bReportScoreLine = False
                        self.pugInfo.gameServer.utQueryStatsActive = True

                    if bReportScoreLine:
                        # Defer a scoreline report to the next cycle of this function by disabling the infrequent stats embed
                        self.pugInfo.gameServer.utQueryStatsActive = False
            elif 'code' in self.pugInfo.gameServer.utQueryData and self.pugInfo.gameServer.utQueryData['code'] == 408 and self.pugInfo.pugLocked == False:
                self.pugInfo.gameServer.utQueryStatsActive = False
                self.pugInfo.gameServer.utQueryReporterActive = False
        return True

    async def queryServerStats(self, cacheonly: bool=False):
        embedInfo = discord.Embed(color=discord.Color.greyple(),title=self.pugInfo.gameServer.format_current_serveralias,description='Waiting for server info...')
        # Send "info" to get basic server details and confirm online
        if self.pugInfo.gameServer.utQueryServer('info'):
            if 'code' in self.pugInfo.gameServer.utQueryData and self.pugInfo.gameServer.utQueryData['code'] == 200:
                if cacheonly is False:
                    # Rate-limit reporter-channel stats cards to one a minute, even after an on-demand stats call
                    self.pugInfo.gameServer.utQueryData['laststats'] = int(time.time())

                # Send multi-query request for lots of info
                if self.pugInfo.gameServer.utQueryServer('status\\\\level_property\\timedilation\\\\game_property\\teamscore\\\\game_property\\teamnamered\\\\game_property\\teamnameblue\\\\player_property\\Health\\\\game_property\\elapsedtime\\\\game_property\\remainingtime\\\\game_property\\bmatchmode\\\\game_property\\friendlyfirescale\\\\game_property\\currentdefender\\\\game_property\\bdefenseset\\\\game_property\\matchcode\\\\game_property\\fraglimit\\\\game_property\\timelimit\\\\rules'):
                    queryData = self.pugInfo.gameServer.utQueryData
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
                        summary['PlayerList{0}'.format(x)] = '*(No players)*'
                        summary['PlayerList{0}_data'.format(x)] = ''
                    summary['PlayerList255'] = '*(No Spectators)*'
                    summary['PlayerList255_data'] = ''
                    # Pick out generic UT info
                    if 'hostname' in queryData:
                        if 'mutators' in queryData and re.search('Lag\sCompensator',str(queryData['mutators']),re.IGNORECASE) is not None:
                            summary['Title'] = summary['Hostname'] = queryData['hostname'].replace('| StdAS |','| lcAS |')
                        else:
                            summary['Title'] = summary['Hostname'] = queryData['hostname'].replace('| iAS | zp|','| zp-iAS |')
                    if 'mapname' in queryData:
                        embedInfo.set_thumbnail(url='{0}{1}.jpg'.format(self.pugInfo.gameServer.thumbnailServer,str(queryData['mapname']).lower()))
                        summary['Map'] = queryData['mapname']
                    if 'remainingtime' in queryData:
                        summary['RemainingTime'] = '{0}'.format(str(time.strftime('%M:%S',time.gmtime(int(queryData['remainingtime'])))))
                    elif 'elapsedtime' in queryData:
                        summary['ElapsedTime'] = '{0}'.format(str(time.strftime('%M:%S',time.gmtime(int(queryData['elapsedtime'])))))
                    elif 'timelimit' in queryData and int(queryData['timelimit']) > 0:
                        summary['TimeLimit'] = '{0}:00'.format(int(queryData['timelimit']))
                    if 'maptitle' in queryData:
                        summary['Map'] = queryData['maptitle']
                    if 'numplayers' in queryData and 'maxplayers' in queryData:
                        summary['PlayerCount'] = '{0}/{1}'.format(queryData['numplayers'],queryData['maxplayers'])
                        if 'maxteams' in queryData and int(queryData['numplayers']) > 0:
                            for x in range(int(queryData['numplayers'])):
                                if 'player_{0}'.format(x) in queryData:
                                    player = {}
                                    player['Name'] = queryData['player_{0}'.format(x)].replace('`','').strip()
                                    if len(player['Name']) > 14:
                                        player['Name'] = '{0}...'.format(player['Name'][:12]).strip()
                                    player['Frags'] = '0'
                                    if 'frags_{0}'.format(x) in queryData:
                                        player['Frags'] = queryData['frags_{0}'.format(x)].strip()                                                                                        
                                    player['Ping'] = '0'
                                    if 'ping_{0}'.format(x) in queryData:
                                        player['Ping'] = queryData['ping_{0}'.format(x)].strip()
                                        if len(str(player['Ping'])) > 3:
                                            player['Ping'] = '---'
                                    if 'team_{0}'.format(x) in queryData:
                                        player['TeamId'] = queryData['team_{0}'.format(x)]
                                    if player['TeamId'] == '255':
                                        summary['PlayerList{0}_data'.format(player['TeamId'])] = '{0}\n{1}\t {2} {3}'.format(summary['PlayerList{0}_data'.format(player['TeamId'])],player['Name'].ljust(15),''.rjust(5),player['Ping'].rjust(4))
                                    else:
                                        summary['PlayerList{0}_data'.format(player['TeamId'])] = '{0}\n{1}\t {2} {3}'.format(summary['PlayerList{0}_data'.format(player['TeamId'])],player['Name'].ljust(15),player['Frags'].rjust(5),player['Ping'].rjust(4))

                            for x in range(int(queryData['maxteams'])):
                                if summary['PlayerList{0}_data'.format(x)] not in ['',None]:
                                    summary['PlayerList{0}'.format(x)] = '```Player Name{0}\t Score Ping'.format('\u2800'*3)
                                    summary['PlayerList{0}'.format(x)] = '{0}{1}\n```'.format(summary['PlayerList{0}'.format(x)],summary['PlayerList{0}_data'.format(x)])
                            
                            if summary['PlayerList255_data'] not in ['',None]:
                                summary['PlayerList255'] = '```Name       {0}\t       Ping'.format('\u2800'*3)
                                summary['PlayerList255'] = '{0}{1}\n```'.format(summary['PlayerList255'],summary['PlayerList255_data'])

                    # Set basic embed info
                    embedInfo.color = summary['Colour']
                    embedInfo.title = summary['Title']
                    embedInfo.description = '```unreal://{0}:{1}```'.format(queryData['ip'],queryData['game_port'])

                    if 'password' in queryData and queryData['password'] == 'True' and self.pugInfo.gameServer.format_gameServerURL=='unreal://{0}:{1}'.format(queryData['ip'],queryData['game_port']):
                        embedInfo.set_footer(text='Spectate @ {0}/?password={1}'.format(self.pugInfo.gameServer.format_gameServerURL,self.pugInfo.gameServer.spectatorPassword))

                    # Pick out info for UTA-only games
                    if 'bmatchmode' in queryData and 'gametype' in queryData and queryData['gametype'] == 'Assault':
                        # Send individual requests for objectives and UTA-enhanced team info, refresh local variable
                        self.pugInfo.gameServer.utQueryServer('objectives')
                        self.pugInfo.gameServer.utQueryServer('teams')
                        queryData = self.pugInfo.gameServer.utQueryData
    
                        if 'AdminName' in queryData and queryData['AdminName'] not in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                            # Match mode is active
                            if 'score_0' in queryData and 'score_1' in queryData:
                                if queryData['score_0'] > queryData['score_1']:
                                    summary['Colour'] = discord.Color.red()
                                elif queryData['score_0'] < queryData['score_1']:
                                    summary['Colour'] = discord.Color.blurple()
                                if 'teamnamered' in queryData and 'teamnameblue' in queryData:
                                    summary['Title'] = '{0} | {1} {2} - {3} {4}'.format(self.pugInfo.desc,queryData['teamnamered'],queryData['score_0'],queryData['score_1'],queryData['teamnameblue'])
                                else:
                                    summary['Title'] = '{0} | RED {1} - {2} BLUE'.format(self.pugInfo.desc,queryData['score_0'],queryData['score_1'])
                            summary['Hostname'] = '```unreal://{0}:{1}```'.format(queryData['ip'],queryData['game_port'])
                        elif 'AdminName' in queryData and queryData['AdminName'] in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                            summary['Hostname'] = '```unreal://{0}:{1}```'.format(queryData['ip'],queryData['game_port'])
                        # Build out round info
                        if 'bdefenseset' in queryData and 'currentdefender' in queryData:
                            if queryData['bdefenseset'] in ['true','True','1']:
                                summary['RoundStatus'] = '2/2'
                            else:
                                summary['RoundStatus'] = '1/2'
                            if queryData['currentdefender'] == '1':
                                if 'teamnamered' in queryData and queryData['AdminName'] not in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                                    summary['RoundStatus'] = '{0}\tRound {1}; {2} attacking'.format(summary['Hostname'],summary['RoundStatus'],queryData['teamnamered'])
                                else:
                                    summary['RoundStatus'] = '{0}\tRound {1}; {2} attacking'.format(summary['Hostname'],summary['RoundStatus'],'Red Team')
                            else:
                                if 'teamnameblue' in queryData and queryData['AdminName'] not in ['OPEN - PUBLIC','LOCKED - PRIVATE']:
                                    summary['RoundStatus'] = '{0}\tRound {1}; {2} attacking'.format(summary['Hostname'],summary['RoundStatus'],queryData['teamnameblue'])
                                else:
                                    summary['RoundStatus'] = '{0}\tRound {1}; {2} attacking'.format(summary['Hostname'],summary['RoundStatus'],'Blue Team')
                        if 'fortcount' in queryData:
                            summary['Objectives'] = ''
                            for x in range(int(queryData['fortcount'])):
                                if x == 0:
                                    summary['Objectives'] = ' \t {0} - {1}'.format(str(queryData['fort_{0}'.format(x)]),str(queryData['fortstatus_{0}'.format(x)]))
                                else:
                                    summary['Objectives'] = '{0}\n \t {1} - {2}'.format(summary['Objectives'],str(queryData['fort_{0}'.format(x)]),str(queryData['fortstatus_{0}'.format(x)]))
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
                        queryData = self.pugInfo.gameServer.utQueryData
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
                self.pugInfo.gameServer.utQueryEmbedCache = embedInfo.to_dict()

        if ('code' not in self.pugInfo.gameServer.utQueryData) or ('code' in self.pugInfo.gameServer.utQueryData and self.pugInfo.gameServer.utQueryData['code'] > 400):
            # Server offline
            embedInfo.color = discord.Color.darker_gray()
            if self.pugInfo.gameServer.gameServerOnDemand is True:
                embedInfo.description = '```{0}```\nOn-demand server is currently offline. Start a !pug to use this server.'.format(self.pugInfo.gameServer.format_gameServerURL)
                self.pugInfo.gameServer.utQueryEmbedCache = embedInfo.to_dict()
            else:
                self.pugInfo.gameServer.utQueryEmbedCache = {} # fall back to old method
        return True

    def cacheGuildEmojis(self):
        if self.activeChannel is not None:
            for x in self.activeChannel.guild.emojis:
                if x.animated:
                    self.customAnimatedEmojis[':{0}:'.format(x.name)] = x.id
                else:
                    self.customStaticEmojis[':{0}:'.format(x.name)] = x.id

    def ratingsMatchInfo(self, mode, matchCode: str = ''):
        matchInfo = {}
        if self.pugInfo.ranked and self.pugInfo.mode.upper() == mode.upper() and self.pugInfo.ratings not in [None,'']:
            rkData = {'rankedgames':[]}
            rkData['rankedgames'].append(self.pugInfo.ratings)
        else:
            if self.pugInfo.pugLocked != True:
                self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
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
        cards = []
        embedInfo = discord.Embed(color=discord.Color.greyple(),title='Ranked mode {0} match report'.format(mode),description='')
        if len(matchref) == 0 and playerid == 0:
            if len(self.pugInfo.gameServer.matchCode):
                matchref = self.pugInfo.gameServer.matchCode
            elif len(self.pugInfo.gameServer.lastMatchCode):
                matchref = self.pugInfo.gameServer.lastMatchCode
        if len(matchref) > 0:
            embedInfo.description = 'Match reference: `{0}`'.format(matchref)
            matchInfo = self.ratingsMatchInfo(mode,matchref)
            if matchInfo == {}:
                embedInfo.title = 'Match not found'
                return embedInfo
            else:
                matchref = matchInfo['gameref']
                embedInfo.description = 'Match reference: `{0}` ([stats]({1}{2}{3}))'.format(matchref,DEFAULT_POST_SERVER,'/pugstats/index.php?p=uta_match&matchcode=',matchref)
                started = datetime.fromisoformat(matchInfo['startdate'])
                embedInfo.add_field(name='Match started',value=started.strftime('%d/%b/%Y @ %H:%M'),inline=True)
                if matchInfo['completed']:
                    ended = datetime.fromisoformat(matchInfo['enddate'])
                    embedInfo.add_field(name='Match ended',value=ended.strftime('%d/%b/%Y @ %H:%M'),inline=True)
                    embedInfo.add_field(name='Duration',value='{0} mins'.format(getDuration(started,ended,'minutes')),inline=True)
                    embedInfo.color = discord.Color.brand_green()
                else:
                    embedInfo.add_field(name='Status',value='Incomplete / Void',inline=True)
                embedInfo.add_field(name="Map list",value=PLASEP.join(matchInfo['maplist']),inline=False)
                embedInfo.add_field(name="Team Power",value='Red {0} - {1} Blue'.format(matchInfo['rpred'],matchInfo['rpblue']))
                embedInfo.add_field(name="Score",value='Red {0} - {1} Blue'.format(matchInfo['scorered'],matchInfo['scoreblue']))
            cards.append(embedInfo)
            if 'teamred' in matchInfo:
                teamRed = matchInfo['teamred']
            if 'teamblue' in matchInfo:
                teamBlue = matchInfo['teamblue']
        if len(teamRed) > 0:
            embedInfo = discord.Embed(color=discord.Color.red(),title='Team Red player ratings '.format(GRAPHUP),description='')
            if matchInfo['scorered'] < matchInfo['scoreblue']:
                embedInfo.title = embedInfo.title+GRAPHDN
            else:
                embedInfo.title = embedInfo.title+GRAPHUP
            report = self.ratingsPlayerReport(mode=mode,players=teamRed,matchref=matchref)
            if 'cap_name' in report and report['cap_name'] not in [None,'']:
                embedInfo.add_field(name='Captain',value='{0}'.format(report['cap_name']),inline=True)
                embedInfo.add_field(name='Power',value='{0}'.format(report['cap_rp']),inline=True)
                embedInfo.description = ''
            if 'players' in report and report['players'] not in [None,'']:
                #embedInfo.add_field(name="\u200B", value="\u200B")
                #embedInfo.add_field(name='Players',value='{0}'.format(report['players']),inline=True)
                #embedInfo.add_field(name='Power',value='{0}'.format(report['players_rp']),inline=True)
                embedInfo.add_field(name='Player ratings',value='{0}'.format(report['players_sum']),inline=False)
                embedInfo.description = ''
            if embedInfo.description != 'Data not found':
                cards.append(embedInfo)
        if len(teamBlue) > 0:
            embedInfo = discord.Embed(color=discord.Color.blurple(),title='Team Blue player ratings ',description='Data not found')
            if matchInfo['scorered'] > matchInfo['scoreblue']:
                embedInfo.title = embedInfo.title+GRAPHDN
            else:
                embedInfo.title = embedInfo.title+GRAPHUP
            report = self.ratingsPlayerReport(mode=mode,players=teamBlue,matchref=matchref)
            if 'cap_name' in report and report['cap_name'] not in [None,'']:
                embedInfo.add_field(name='Captain',value='{0}'.format(report['cap_name']),inline=True)
                embedInfo.add_field(name='Power',value='{0}'.format(report['cap_rp']),inline=True)
                embedInfo.description = ''
            if 'players' in report and report['players'] not in [None,'']:
                #embedInfo.add_field(name="\u200B", value="\u200B")
                #embedInfo.add_field(name='Players',value='{0}'.format(report['players']),inline=True)
                #embedInfo.add_field(name='Power',value='{0}'.format(report['players_rp']),inline=True)
                embedInfo.add_field(name='Player ratings',value='{0}'.format(report['players_sum']),inline=False)
                embedInfo.description = ''
            if embedInfo.description != 'Data not found':
                cards.append(embedInfo)
        if playerid > 0:
            embedInfo = discord.Embed(color=discord.Color.greyple(),title='Player rating history',description='Data not found')
            report = self.ratingsPlayerReport(mode=mode,playerid=playerid)
            if 'player_name' in report and report['player_name'] not in [None,'']:
                embedInfo.description = 'Ratings history for {0}'.format(report['player_name'])
                embedInfo.add_field(name='Last game',value='{0}'.format(report['player_last']),inline=False)
                if len(report['player_hist']):
                    embedInfo.add_field(name='Previous {0} game(s)'.format(report['player_hist_count']),value='{0}'.format(report['player_hist']),inline=False)
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
                capSummary = 'RP: {0} {2} Previous RP: {1}'.format(cap['ratingvalue'],cap['ratingprevious'],updn(cap['ratingvalue'],cap['ratingprevious']))
            else:
                for h in cap['ratinghistory']:
                    if h['matchref'].upper() == matchref.upper() or len(matchref) == 0:
                        capSummary = 'RP before: {0} {2} RP after: {1}; Current RP: {3}'.format(h['ratingbefore'],h['ratingafter'],updn(h['ratingafter'],h['ratingbefore']),cap['ratingvalue'])
            report['cap_name'] = cap['dlastnick']
            report['cap_rp'] = capSummary
        elif playerid > 0:
            player = self.ratingsPlayerDataHandler('rkget', mode, playerid)
            report['player_name'] = player['dlastnick']
            if len(player['lastgamedate']) > 0:
                g_startdate = datetime.fromisoformat(player['lastgamedate']).strftime('%d/%b/%Y @ %H:%M')
                if player['lastgameref'] == 'admin-set':
                    if player['ratingprevious'] == 0:
                        pSummary = 'Admin seeded rating of **{0}** on {1}\n'.format(player['ratingvalue'],g_startdate)
                    else:
                        pSummary = 'Admin set rating on {0}: RP before: **{1}** {2} RP after: **{3}**\n'.format(g_startdate,player['ratingprevious'],updn(player['ratingvalue'],player['ratingprevious']),player['ratingvalue'])
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
                            pSummary = 'Match: `{0}` @ {1}\n> Team: {2}\n> Score: Red {3} - {4} Blue;\n> RP before: **{5}** {6} RP after: **{7}**\n'.format(player['lastgameref'],g_startdate,pteam,matchInfo['scorered'],matchInfo['scoreblue'],player['ratingprevious'],updn(player['ratingvalue'],player['ratingprevious']),player['ratingvalue'])
                        else:
                            pSummary = 'Match: `{0}` @ {1}\n> Team: {2}\n> Status: Incomplete / voided match\n'.format(player['lastgameref'],g_startdate,pteam)
                    else:
                        pSummary = 'Match: `{0}` @ {1}\n> RP before: **{2}** {3} RP after: **{4}**\n'.format(player['lastgameref'],g_startdate,player['ratingprevious'],updn(player['ratingvalue'],player['ratingprevious']),player['ratingvalue'])
            else:
                g_startdate = datetime.fromisoformat(player['ratingdate']).strftime('%d/%b/%Y')
                pSummary = 'Seed rating, set on {0}: **{1}**\n'.format(g_startdate,player['ratingvalue'])
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
                                pSummary = pSummary+'Admin seeded rating of **{0}** on {1}\n'.format(h['ratingafter'],g_startdate)
                            else:
                                pSummary = pSummary+'Admin set rating on {0}\n> RP before: **{1}** {2} RP after: **{3}**\n'.format(g_startdate,h['ratingbefore'],updn(h['ratingafter'],h['ratingbefore']),h['ratingafter'])
                        else:
                            matchInfo = self.ratingsMatchInfo(mode, h['matchref'])
                            if matchInfo != {}:
                                if playerid in matchInfo['teamred']:
                                    pteam = 'Red'
                                else:
                                    pteam = 'Blue'
                                if (matchInfo['completed']):
                                    pSummary = pSummary+'Match: `{0}` @ {1}\n> Team: {2}\n> Score: Red {3} - {4} Blue\n> RP before: **{5}** {6} RP after: **{7}**\n'.format(h['matchref'],g_startdate,pteam,matchInfo['scorered'],matchInfo['scoreblue'],h['ratingbefore'],updn(h['ratingafter'],h['ratingbefore']),h['ratingafter'])
                                else:
                                    pSummary = pSummary+'Match: `{0}` @ {1}\n> Team: {2}\n> Status: Incomplete / voided match\n'.format(h['matchref'],g_startdate,pteam)
                            else:
                                pSummary = pSummary+'Match: `{0}` @ {1}\n> RP before: **{2}** {3} RP after: **{4}**\n'.format(h['matchref'],g_startdate,h['ratingbefore'],updn(h['ratingafter'],h['ratingbefore']),h['ratingafter'])
                        i += 1
            report['player_hist'] = pSummary
            report['player_hist_count'] = i
        if len(players) > 1:
            for p in players[1:]:
                player = self.ratingsPlayerDataHandler('rkget',mode,p)
                report['players'] = report['players']+player['dlastnick']+'\n'
                report['players_sum'] = report['players_sum']+'**'+player['dlastnick']+'**\n> '
                if player['lastgameref'].upper() == matchref.upper() or len(matchref) == 0:
                    pSummary = 'RP: {0} {2} Previous RP: {1}\n'.format(player['ratingvalue'],player['ratingprevious'],updn(player['ratingvalue'],player['ratingprevious']))
                    report['players_rp'] = report['players_rp']+pSummary
                    report['players_sum'] = report['players_sum']+pSummary
                else:
                    for h in player['ratinghistory']:
                        if h['matchref'].upper() == matchref.upper() or len(matchref) == 0:
                            pSummary = 'RP before: {0} {2} RP after: {1}; Current RP: {3}\n'.format(h['ratingbefore'],h['ratingafter'],updn(h['ratingafter'],h['ratingbefore']),player['ratingvalue'])
                            report['players_rp'] = report['players_rp']+pSummary
                            report['players_sum'] = report['players_sum']+pSummary
        return report
    
    def ratingsPlayerDataHandler(self, action, mode, player, rating: int = 0, toggle: bool = False, additionalid: int = 0):
        if self.pugInfo.ranked and self.pugInfo.mode.upper() == mode.upper() and self.pugInfo.ratings not in [None,'']:
            log.debug('ratingsPlayerDataHandler() using cached ratings')
            rkData = {'rankedgames':[]}
            rkData['rankedgames'].append(self.pugInfo.ratings)
        else:
            log.debug('ratingsPlayerDataHandler() reloading data from JSON file')
            if self.pugInfo.pugLocked != True:
                self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
        rkReload = False
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' not in x:
                    rkReload = True
        if rkReload:
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
        if type(player) is int:
            pid = player
            pdn = ''
        elif type(player) is str:
            pid = -1
            pdn = player
        else:
            pid = player.id
            pdn = player.display_name
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode'] # update formatting
                    if action == 'rkget' or action == 'rkrecalc':
                        msg = 'Error - Player not registered for ranked games in {0}'.format(mode)
                        if pid in x['registrations'] or pid < 0:
                            for r in x['ratings']:
                                if r['did'] == pid or (pid < 0 and 'dlastnick' in r and str(r['dlastnick']).lower() == pdn.lower()):
                                    if action == 'rkget':
                                        return r
                                    elif action == 'rkrecalc':
                                        if pid < 0: 
                                            pid = r['did']
                                        admsets = []
                                        log.debug('ratingsPlayerDataHandler(rkrecalc) - Recalculating rank for {0}'.format(r['dlastnick']))
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
                                            return 'Initial seed rating could not be found for {0}, please provide a seed rating.'.format(r['dlastnick'])
                                        log.debug('ratingsPlayerDataHandler({0}) - Seed rating for {1} = {2}'.format(mode,r['dlastnick'],rating))
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
                                                    msg = 'RP recalculated for {0}:\n> Seed rating on {1}: **{2}**\n'.format(r['dlastnick'],g_date,rating)
                                                if m['completed']:
                                                    # Determine whether rating has been adjusted before sending to recalc
                                                    for aset in admsets:
                                                        if len(r['lastgamedate']) == 0:
                                                            r['lastgamedate'] = r['ratingdate']
                                                        if datetime.fromisoformat(aset['matchdate']) > datetime.fromisoformat(r['lastgamedate']) and datetime.fromisoformat(m['startdate']) > datetime.fromisoformat(aset['matchdate']):
                                                            log.debug('ratingsPlayerDataHandler({0}) - admin adjusted rating present between matches (last game: {1}; update date: {2}; next match started: {3}). Adjusting seed for {4} to {5} without adjusting history'.format(mode,r['lastgamedate'],aset['matchdate'],m['startdate'],r['dlastnick'],aset['ratingafter']))
                                                            r['ratingdate'] = aset['matchdate']
                                                            r['ratingprevious'] = r['ratingvalue']
                                                            r['ratingvalue'] = aset['ratingafter']
                                                            g_date = datetime.fromisoformat(aset['matchdate']).strftime('%d/%b/%Y %H:%M')
                                                            msg = msg+'> Admin updated @ {0}: RP before: **{1}**; RP after: **{2}**\n'.format(g_date,r['ratingprevious'],r['ratingvalue'])
                                                    if pid in m['teamred']:
                                                        pteam = 'Red'
                                                    else:
                                                        pteam = 'Blue'
                                                    if m['capred']['id'] == pid or m['capblue']['id'] == pid:
                                                        pteam = pteam+', captain'
                                                    g_last = datetime.fromisoformat(m['startdate'])
                                                    g_date = g_last.strftime('%d/%b/%Y %H:%M')
                                                    log.debug('ratingsPlayerDataHandler({0}) - {1} present in match {2} - calculating RP'.format(mode,r['dlastnick'],m['gameref']))
                                                    rk = self.pugInfo.applyRankedScoring(x, mode=mode, match=m, player=pid)
                                                    log.debug('ratingsPlayerDataHandler({0}) - Updated player data from applyRankedScoring() for match: {1} = {2}; RP before: {3}, RP after: {4}'.format(mode,m['gameref'],rk['lastgameref'],rk['ratingprevious'],rk['ratingvalue']))
                                                    if 'did' in rk and rk['did'] == pid:
                                                        r = rk
                                                    msg = msg+'> Match: `{0}` @ {1} (team {2}); Score: Red {3} - {4} Blue. RP before: **{5}**; RP after: **{6}**\n'.format(r['lastgameref'],g_date,pteam,m['scorered'],m['scoreblue'],r['ratingprevious'],r['ratingvalue'])
                                                else:
                                                    log.debug('ratingsPlayerDataHandler({0}) - {1} present in voided/incomplete match {2} - ignoring RP. Last completed game: {3} on {4}'.format(mode,r['dlastnick'],m['gameref'],r['lastgameref'],r['lastgamedate']))
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
                                                    msg = msg+'> Admin updated @ {0}: RP before: **{1}**; RP after: **{2}**\n'.format(g_date,r['ratingprevious'],r['ratingvalue'])
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
                            log.debug('{0}({1},{2},{3},{4}) - registering new pid for {5}.'.format(action,pid,mode,rating,toggle,pdn))
                        rkUpdate = False
                        for r in x['ratings']:
                            if r['did'] == pid:
                                log.debug('{0}({1},{2},{3},{4}) - updating existing rank data for {5}.'.format(action,pid,mode,rating,toggle,pdn))
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
                                if len(r['ratinghistory']) > 150:
                                     r['ratinghistory'][:] = r['ratinghistory'][-150:]
                                r['ratingprevious'] = r['ratingvalue']
                                r['ratingvalue'] = rating
                                r['lastgamedate'] = r['ratingdate']
                                r['lastgameref'] = 'admin-set'
                                rkUpdate = True
                        if rkUpdate == False and pid > -1: # new entry required
                            log.debug('{0}({1},{2},{3},{4}) - adding new rank data for {5}.'.format(action,pid,mode,rating,toggle,pdn))
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
                        if pid > -1:
                            msg = 'Rank configured with a rating of {0} for {1} (id:{2}) in game mode {3}'.format(rating,pdn,pid,mode)
                        else:
                            msg = 'Player ID could not be established for {0}'.format(pdn)
                    elif action == 'rkdel':
                        if pid in x['registrations']:
                            x['registrations'].remove(pid)
                        for r in x['ratings']:
                            if r['did'] == pid:
                                x['ratings'].remove(r)
                        if pid > -1:
                            msg = 'Ranked player rating removed for {0} (id:{1}) in game mode {2}'.format(pdn,pid,mode)
                        else:
                            msg = 'Player ID could not be established for {0}'.format(pdn)
                    else:
                        msg = 'Unsupported action called.'
        if self.pugInfo.savePugRatings(self.pugInfo.ratingsFile, rkData):
            log.debug('ratingsPlayerDataHandler({0}) - saved updated ratings'.format(mode))
            if (self.pugInfo.ranked): # reload data for current ranked mode
                self.pugInfo.setRankedMode(self.pugInfo.ranked, True)
                log.debug('ratingsPlayerDataHandler({0}) - loaded ratings back into memory'.format(mode))
        else:
                msg = 'Error - rank data could not be saved; check bot logs.'
        if action == 'rkget':
            msg = None
        return msg

    def ratingsSync(self, endpoint: str = '', body: str = '', authkey: str = '', restrict: bool = False, delay: int = 0):
        if restrict and (datetime.now() - self.lastAPISyncTime).total_seconds() < delay: # 5 second delay between requests when restricted.
            log.debug('API request throttled. Last API sync: {0}'.format(self.lastAPISyncTime.strftime('%d/%m/%Y %H:%M:%S')))
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
                log.error('Invalid JSON returned from Sync API, URL: {0} HTTP response: {1}; content:{2}'.format(r.url,r.status_code,r.content))
                return {}
        else:
            return {}

    #########################################################################################
    # Bot Admin ONLY commands.
    #########################################################################################
    @commands.hybrid_command(aliases=['enable'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def pugenable(self, ctx):
        """Enables PUG commands in the channel. Note only one channel can be active at a time. Admin only"""
        if self.activeChannel:
            if self.activeChannel == ctx.message.channel:
                await ctx.send('PUG commands are already enabled in {}'.format(ctx.message.channel.mention))
                return
            await self.activeChannel.send('PUG commands have been disabled in {0}. They are now enabled in {1}'.format(self.activeChannel.mention, ctx.message.channel.mention))
            await ctx.send('PUG commands have been disabled in {}'.format(self.activeChannel.mention))
        self.activeChannel = ctx.message.channel
        self.savePugConfig(self.configFile)
        await ctx.send('PUG commands are enabled in {}'.format(self.activeChannel.mention))

    @commands.command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminadd(self, ctx, *players: discord.Member): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Adds a player to the pug. Admin only"""
        failed = False
        for player in players:
            if self.pugInfo.ranked:
                if self.pugInfo.checkRankedPlayersEligibility([player]):
                    if not self.pugInfo.addRankedPlayer(player):
                        failed = True
                        if self.pugInfo.playersReady:
                            await ctx.send('Cannot add {0}: Ranked pug is already full.'.format(display_name(player)))
                        else:
                            await ctx.send('Cannot add {0}: They are already signed.'.format(display_name(player)))
                    else:
                        await ctx.send('{0} is elgible for ranked play and was added by an admin. {1}\n'.format(display_name(player), self.pugInfo.format_pug()))
                else:
                    failed = True
                    await ctx.send('Cannot add {0}: They are inelgible to join a ranked pug.'.format(display_name(player)))
            else:
                if not self.pugInfo.addPlayer(player):
                    failed = True
                    if self.pugInfo.playersReady:
                        await ctx.send('Cannot add {0}: Pug is already full.'.format(display_name(player)))
                    else:
                        await ctx.send('Cannot add {0}: They are already signed.'.format(display_name(player)))
                else:
                    await ctx.send('{0} was added by an admin. {1}\n'.format(display_name(player), self.pugInfo.format_pug()))
        if not failed:
            await self.processPugStatus(ctx)

    @commands.command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminremove(self, ctx, *players: discord.Member): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Removes a player from the pug. Admin only"""
        for player in players:
            if self.pugInfo.removePlayerFromPug(player):
                await ctx.send('**{0}** was removed by an admin.'.format(display_name(player)))
            else:
                await ctx.send('{0} is not in the pug.'.format(display_name(player)))
        await self.processPugStatus(ctx)

    @commands.hybrid_command(aliases=['setserver','setactiveserver'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminsetserver(self, ctx, idx: int):
        """Sets the active server to the index chosen from the pool of available servers. Admin only"""
        svindex = idx - 1 # offset as users see them 1-based index.
        if self.pugInfo.gameServer.useServer(svindex,self.pugInfo.captainsReady): # auto start eligible servers when caps are ready
            await ctx.send('Server was activated by an admin - {0}.'.format(self.pugInfo.gameServer.format_current_serveralias))
            self.pugInfo.gameServer.utQueryConsoleWatermark = self.pugInfo.gameServer.format_new_watermark
            if self.pugInfo.gameServer.gameServerState in ('N/A','N/AN/A'):
                # Check whether server is being changed when captains are already ready
                if not self.pugInfo.captainsReady:
                    await ctx.send('Server is currently offline, but will be fired up upon Captains being selected.')

            # Bit of a hack to get around the problem of a match being in progress when this is initialised. - TODO consider off state too
            # Will improve this later.
            if self.pugInfo.gameServer.lastSetupResult == 'Match In Progress':
                self.pugLocked = True
        else:
            await ctx.send('Selected server **{0}** could not be activated.'.format(idx))
    
    @commands.hybrid_command(aliases=['startserver'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminstartserver(self, ctx, idx: int):
        """Starts up an on-demand server. Admin only"""
        previousRef = self.pugInfo.gameServer.gameServerRef
        svindex = idx - 1 # offset as users see them 1-based index.
        if self.pugInfo.gameServer.useServer(svindex, True):
            await ctx.send('**{0}** is starting up (allow up to 60s).'.format(self.pugInfo.gameServer.gameServerName))
        else:
            await ctx.send('Selected server **{0}** could not be activated.'.format(idx))
        self.pugInfo.gameServer.useServer(-1, True,previousRef) # return to active server
        return True

    @commands.hybrid_command(aliases=['stopserver'])
    @commands.check(admin.hasManagerRole_Check)
    @commands.check(isPugInProgress_Warn)
    async def adminstopserver(self, ctx, idx: int):
        """Queues up an on-demand server to shut down. Admin only"""
        svindex = idx - 1 # offset as users see them 1-based index.
        if self.pugInfo.gameServer.stopOnDemandServer(svindex):
            if len(self.pugInfo.gameServer.allServers[svindex][1]) > 0:
                await ctx.send('**{0}** is queued for shut-down.'.format(self.pugInfo.gameServer.allServers[svindex][1]))
        else:
            await ctx.send('Selected server **{0}** could not be activated.'.format(idx))

    @commands.hybrid_command(aliases=['refreshservers'])
    @commands.check(admin.hasManagerRole_Check)
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
    async def adminremoveserver(self, ctx, svref: str):
        # Removed add server in favour of pulling from API; left remove server in here in case one needs temporarily removing until restart
        """Removes a server from available pool. Admin only"""
        if self.pugInfo.gameServer.removeServerReference(svref):
            await ctx.send('Server was removed from the available pool by an admin.')
        else:
            await ctx.send('Server could not be removed. Is it even in the list?')
    
    @commands.command(aliases=['setrotation','rotate'])
    @commands.check(admin.hasManagerRole_Check)
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
            await ctx.send('Server rotation set to: {0}'.format(', '.join(map(str,self.pugInfo.gameServer.gameServerRotation))))

    @commands.hybrid_command(aliases=['checkrotation','checkrotate'])
    @commands.check(isPugInProgress_Warn)
    async def checkserverrotation(self, ctx):
        """Checks current server and rotates accordingly."""
        tempRotation = self.pugInfo.gameServer.gameServerRef
        if len(self.pugInfo.gameServer.gameServerRotation) > 0:
            self.pugInfo.gameServer.checkServerRotation()
            if self.pugInfo.gameServer.gameServerRef != tempRotation:
                await ctx.send('Server rotation changed server to: {0}.'.format(self.pugInfo.gameServer.format_current_serveralias))
            else:
                await ctx.send('Server is already correctly set.')
        else:
            await ctx.send('Server rotation is not configured.')

    @commands.hybrid_command(aliases=['getrotation'])
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
                        await ctx.send(' - {0}{1}'.format(self.pugInfo.gameServer.allServers[svindex][1],' :arrow_forward: This week'))
                        thisWeek = -1
                    elif (nextWeek == int(x)):
                        await ctx.send(' - {0}{1}'.format(self.pugInfo.gameServer.allServers[svindex][1],' :fast_forward: Next week'))
                        nextWeek = -1
                    else:
                        await ctx.send(' - {0}'.format(self.pugInfo.gameServer.allServers[svindex][1]))
        else:
            await ctx.send('Server rotation is not configured.')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    async def adminaddmap(self, ctx, map: str):
        """Adds a map to the available map list. Admin only"""
        if self.pugInfo.maps.addMapToAvailableList(map):
            self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile, self.pugInfo.maps.availableMapsList)
            await ctx.send('**{0}** was added to the available maps by an admin. The available maps are now:\n{1}'.format(map, self.pugInfo.maps.format_available_maplist))
        else:
            await ctx.send('**{0}** could not be added. Is it already in the list?'.format(map))

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    async def admininsertmap(self, ctx, index: int, map: str):
        """Insert a map into the available map list at the given index. Admin only"""
        if index > 0 and index <= self.pugInfo.maps.maxMapsLimit + 1:
            offset_index = index - 1 # offset as users see them 1-based index
            if self.pugInfo.maps.insertMapIntoAvailableList(offset_index, map):
                self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile, self.pugInfo.maps.availableMapsList)
                await ctx.send('**{0}** was inserted into the available maps by an admin. The available maps are now:\n{1}'.format(map, self.pugInfo.maps.format_available_maplist))
            else:
                await ctx.send('**{0}** could not be inserted. Is it already in the list?'.format(map))
        else:
            await ctx.send('The valid format of this command is, for example: !admininsertmap # AS-MapName, where # is in the range (1, NumMaps + 1).')

    @commands.command()
    @commands.check(admin.hasManagerRole_Check)
    async def adminreplacemap(self, ctx, *mapref: str): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Replaces a map within the available map list. Admin only"""
        if len(mapref) == 2 and mapref[0].isdigit() and (int(mapref[0]) > 0 and int(mapref[0]) <= len(self.pugInfo.maps.availableMapsList)):
            index = int(mapref[0]) - 1 # offset as users see in a 1-based index; the range check is performed before it gets here
            map = mapref[1]
            oldmap = self.pugInfo.maps.availableMapsList[index]
            if self.pugInfo.maps.substituteMapInAvailableList(index, map):
                self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile, self.pugInfo.maps.availableMapsList)
                await ctx.send('**{1}** was added to the available maps by an admin in position #{0}, replacing {2}. The available maps are now:\n{3}'.format(mapref[0],map,oldmap,self.pugInfo.maps.format_available_maplist))
            else:
                await ctx.send('**{1}** could not be added in slot {0}. Is it already in the list? Is the position valid?'.format(mapref[0],map))
        else:
            await ctx.send('The valid format of this command is, for example: !adminreplacemap # AS-MapName, where # is in the range (1, NumMaps).')

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    async def adminremovemap(self, ctx, map: str):
        """Removes a map to from available map list. Admin only"""
        if map.isdigit():
            index = int(map) - 1 # offset as users see in a 1-based index
            mapNameToRemove = self.pugInfo.maps.getMapFromAvailableList(index)
        else:
            mapNameToRemove = map
        if self.pugInfo.maps.removeMapFromAvailableList(mapNameToRemove):
            self.pugInfo.gameServer.saveMapConfig(self.pugInfo.gameServer.configFile,self.pugInfo.maps.availableMapsList)
            await ctx.send('**{0}** was removed from the available maps by an admin.\n{1}'.format(mapNameToRemove, self.pugInfo.maps.format_available_maplist))
        else:
            await ctx.send('**{0}** could not be removed. Is it in the list?'.format(map))

    @commands.hybrid_command()
    @commands.check(admin.hasManagerRole_Check)
    async def passwords(self, ctx):
        """Provides current game passwords to the requesting administrator. Admin only"""
        if self.isPugInProgress:
            await ctx.message.author.send('For the game currently running at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.message.author.send('{0} - {1}'.format(self.pugInfo.gameServer.format_red_password, self.pugInfo.gameServer.format_blue_password))
            await ctx.send('Check your private messages!')
        else:
            await ctx.send('There is no game in progress.')
    
    # Ranked mode commands
    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rksave(self, ctx):
        """Calls for a configuration save"""
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Please try again after the match has concluded.')
        else:
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            await ctx.send('Rank configuration saved.')
        return True

    @commands.hybrid_command(aliases=['setrk','rankset','addrk','rankadd','rkadd'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkset(self, ctx, player: discord.Member, mode: str = 'rASPlus', rating: int = 500, externalpid: int = 0):
        """Adds or sets a player rating within a game mode: PlayerNick GameMode(e.g. rASPlus) Weight(e.g., 500)"""
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Please try again after the match has concluded.')
        else:
            msg = self.ratingsPlayerDataHandler('rkset',mode,player,rating,False,externalpid)
            await ctx.send(msg)
        return True
    
    @commands.hybrid_command(aliases=['rmrk','rkrm','rkdelete','rankdel','rankremove'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkdel(self, ctx, player: discord.Member, mode: str = 'rASPlus'):
        """Removes a player rating within a game mode: PlayerNick GameMode(e.g. rASPlus)"""
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Please try again after the match has concluded.')
        else:
            msg = self.ratingsPlayerDataHandler('rkdel',mode,player,0)
            await ctx.send(msg)
        return True
    
    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rksync(self, ctx, mode: str = 'rASPlus', item: str = '', direction: str = 'outbound'):
        if (self.pugInfo.pugLocked):
            await ctx.send('Ranked data cannot be synchronised while a game is in progress. Please try again later.')
            return True
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if (item in [None,'']):
            await ctx.send('A valid match reference or object type must be specified.')
            return True
        if self.pugInfo.ranked and self.pugInfo.mode.upper() == mode.upper():
            mode = self.pugInfo.mode
        matchInfo = self.ratingsMatchInfo(mode,item)
        if matchInfo == {} and direction == 'outbound':
            await ctx.send('The provided valid match reference could not be found for outbound sync.')
        elif matchInfo != {} and direction == 'outbound':
            started = datetime.fromisoformat(matchInfo['startdate'])
            await ctx.send('{0} match `{1}` played on {2} at {3}...'.format('Synchronising',matchInfo['gameref'],started.strftime('%d/%m/%Y'),started.strftime('%H:%M:%S')))
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rk = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
            if 'rankedgames' in rk:
                for x in rk['rankedgames']:
                    if 'games' in x and 'mode' in x and x['mode'].upper() == mode.upper():
                        for g in x['games']:
                            if 'gameref' in g and g['gameref'].upper() == item.upper():
                                log.debug('rksync() - Found match data in rk; completed={0}'.format(g['completed']))
            # self.pugInfo.savePugRatings(self.pugInfo.ratingsFile,rk)
            # self.pugInfo.setRankedMode(MODE_CONFIG[self.pugInfo.mode].isRanked, False)
        elif direction == 'inbound':
            if item in ['all','matches','player','players']:
                log.debug('rksync() - Batch fetching data from API: {0}'.format(item))
                # await ctx.send('{0} {1} data from {2}...'.format('Fetching',item,'Sync API'))
                await ctx.send('Synchronisation of {0} data for `{1}` has not yet been implemented.'.format(item, mode))
            else:
                endpoint = '{0}?&matchcode={1}'.format(self.pugInfo.ratingsSyncAPI['matchDataURL'],item)
                log.debug('rksync() - Fetching provided match from API: {0}'.format(endpoint))
                await ctx.send('{0} match `{1}` from {2}...'.format('Fetching',item,'Sync API'))
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
                                    log.debug('rksync() - Player lookup failed for ID: {0}'.format(s['did']))
                            elif 'team' in s and s['team'] == 'Blue':
                                g_blue.append(s['did'])
                                p = self.ratingsPlayerDataHandler('rkget', mode, s['did'])
                                if p not in [None,'',{}] and 'ratingvalue' in p:
                                    g_blue_rp += int(p['ratingvalue'])
                                else:
                                    log.debug('rksync() - Player lookup failed for ID: {0}'.format(s['did']))
                        else:
                            invalid_players.append(s['playername'])
                    for m in syData['maps']:
                        if 'map' in m and m['map'] not in g_maps:
                            g_maps.append(m['map'])
                    if len(invalid_players):
                        log.debug('rksync() - Abandonded sync of match data for {0} due to invalid players found.'.format(item))
                        await ctx.send('Could not sync match `{0}` for mode {1}. One or more invalid or unregistered players found: {2}'.format(item, mode,', '.join(invalid_players)))
                        return True
                    else:
                        log.debug('rksync() - Preparing to store match data for {0}.'.format(item))
                        if self.pugInfo.storeRankedPug(mode=mode, matchCode=item, redScore=syData['score_red'], blueScore=syData['score_blue'], timeStarted=g_start, hasEnded=False, redPlayers=g_red, bluePlayers=g_blue, maps=g_maps, redPower=g_red_rp, bluePower=g_blue_rp, timeEnded=g_end):
                            log.debug('rksync() - Stored match successfully.')
                            self.pugInfo.setRankedMode(MODE_CONFIG[self.pugInfo.mode].isRanked, False)
                            if len(g_red) == len(g_blue):
                                log.debug('rksync() - Match marked as completed successfully.')
                                await self.rkvoidmatch(ctx,mode,item)
                        return True
                if syData == None:
                    await ctx.send('Could not sync match `{0}` for mode {1}. API requests throttled, please try again in 10s.'.format(item, mode))
                else:
                    await ctx.send('Could not sync match `{0}` for mode {1}. Please check the reference is valid.'.format(item, mode))
        return True

    @commands.hybrid_command(aliases=['rankrecalc','rankcalc','rkrpcalc'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkrecalc(self, ctx, player: discord.Member, mode: str = 'rASPlus', seed: int = 0, pid: int = 0):
        """Recalculates RP of a player: PlayerNick GameMode(e.g. rASPlus) <optional seed value>"""
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('RP cannot be reclculated while a ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
        else:
            if pid > 0:
                await ctx.send('Recalculating RP (override ID={0})...'.format(pid))
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
    async def rkgamesimulation(self, ctx, player1=None, player2=None, player3=None, player4=None, player5=None, player6=None, player7=None, player8=None, player9=None, player10=None, player11=None, player12=None, player13=None, player14=None):
        """Simulates player picks for the active ranked mode, using rules for that mode. Use player:(+-)100 modifiers to test changes."""
        if not self.pugInfo.ranked:
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
                    log.debug('rkgamesimulation() - simulation pre-processed player: {0}'.format(player))
                else:
                    if player == None:
                        player = modifier[1]
                    if len(modifier[4]):
                        adjust = int(modifier[4])
                    if len(modifier[3]) == 0 and len(modifier[2]) == 0:
                        override = True
                    elif len(modifier[3]):
                        adjust = 0-adjust
                pstats = self.ratingsPlayerDataHandler('rkget', self.pugInfo.mode, player)
                if pstats not in ['', None]:
                    if adjust > 0:
                        if override:
                            ratingValue = adjust
                        else:
                            ratingValue = pstats['ratingvalue']+adjust
                        log.debug('rkgamesimulation() - adjusted player rating for {0} from {1} to {2}'.format(player,str(pstats['ratingvalue']),str(ratingValue)))
                    else:
                        ratingValue = pstats['ratingvalue']
                        log.debug('rkgamesimulation() - adding player {0} at RP {1} to simulation'.format(player,str(pstats['ratingvalue'])))
                    players.append({
                        'id': pstats['did'],
                        'did': pstats['did'],
                        'name': pstats['dlastnick'],
                        'ratingvalue': ratingValue
                    })
                else:
                    invalid.append(player)
        if len(players) > 1 and len(players) % 2 == 0:
            msg = self.pugInfo.makeRatedTeams(simulatedRatings=players)
            await ctx.send('Simulated player pick for {0} provided and registered players:'.format(len(players)))
            await ctx.send(msg)
        else:
            if len(invalid):
                await ctx.send('Invalid player(s): {0}'.format(PLASEP.join(invalid)))
            await ctx.send('Provide two or more valid players for simulation.')
        return True

    @commands.command(aliases=['clearmaplist'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkclearmaps(self, ctx, mode: str = ''):
        """Clears all available maps from a ranked mode maplist. Parameters: GameMode"""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified to clear maps.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('Maps could not be cleared - a ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Please try again after the match has concluded.')
        else:
            # Save then load the current ratings data before manipulating and saving again
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
            rkUpdate = False
            if 'rankedgames' in rkData:
                log.debug('rkclearmaps({0}) - ranked games present.'.format(mode))
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        log.debug('rkclearmaps({0}) - updating mode {1}.'.format(mode,x['mode']))
                        mode = x['mode'] # update formatting
                        if 'maps' in x:
                            if 'maplist' in x['maps']:
                                x['maps']['maplist'] = []
                            x['fixedpicklimit'] = 0
                        rkUpdate = True
            if rkUpdate and self.pugInfo.savePugRatings(self.pugInfo.ratingsFile, rkData):
                await ctx.send('Map list and pick limit cleared for ranked game mode {0}'.format(mode))
                if (self.pugInfo.ranked): # reload data for current ranked mode
                    self.pugInfo.setRankedMode(self.pugInfo.ranked, True)
            else:
                await ctx.send('Error - ranked map data could not be saved; check bot logs.')
        return True

    @commands.command(aliases=['rkaddmap'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkaddmaps(self, ctx, mode: str='', *maps: str):
        """Adds maps to a ranked mode maplist. Parameters: GameMode Map:Order:Weight"""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified to add maps.')
            return True
        if len(maps) == 0:
            await ctx.send('Provide one or more maps in the format: MapName1 MapName2:Order:Weight\nOptional parameters - **Order** represents the pick order (0 is any order) and **Weight** will apply a multiplier on chances of being picked - higher weight = higher chance of being picked, default weight is 1.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Maps cannot be added while a match is in progress.')
        else:
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
            rkUpdate = False
            if 'rankedgames' in rkData:
                log.debug('rkaddmaps({0},{1}) - ranked games present.'.format(mode,maps))
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        log.debug('rkaddmaps({0},{1}) - updating mode {2}.'.format(mode,maps,x['mode']))
                        mode = x['mode'] # update formatting
                        maplist = []
                        if 'maps' in x:
                            if 'maplist' in x['maps']:
                                maplist = x['maps']['maplist']
                        else:
                            x['maps'] = {
                                'maplist': [],
                                'fixedpicklimit': 0
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
                        rkUpdate = True
            if rkUpdate and self.pugInfo.savePugRatings(self.pugInfo.ratingsFile, rkData):
                await ctx.send('Map list updated for ranked game mode {0}'.format(mode))
                if (self.pugInfo.ranked): # reload data for current ranked mode
                    self.pugInfo.setRankedMode(self.pugInfo.ranked, True)
            elif rkUpdate==False:
                await ctx.send('Error - a ranked map limit could not be saved - game mode not found.')
            else:
                await ctx.send('Error - ranked map data could not be added; check bot logs.')
        return True

    @commands.hybrid_command(aliases=['rklimit','rksetlimit', 'rksetmaps','rksetmaplimit'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkmaplimit(self, ctx, mode: str='', limit: int = 5, shuffle: str = ''):
        """Sets the pick limit and shuffle mode for a ranked mode maplist."""
        if (mode in [None,''] or limit in [None, '']):
            await ctx.send('A valid ranked mode and map limit must be specified.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0} [{1},{2},{3}]'.format(self.pugInfo.gameServer.format_gameServerURL,self.pugInfo.pugLocked,self.pugInfo.ranked))
            await ctx.send('Map limits cannot be modified while a match is in progress.')
        else:
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
            rkUpdate = False
            if 'rankedgames' in rkData:
                log.debug('rkmaplimit({0},{1}) - ranked games present.'.format(mode,limit))
                for x in rkData['rankedgames']:
                    if 'mode' in x and str(x['mode']).upper() == mode.upper():
                        log.debug('rkmaplimit({0},{1}) - updating mode {2}.'.format(mode,limit,x['mode']))
                        mode = x['mode'] # update formatting
                        if 'maps' in x:
                            x['maps']['fixedpicklimit'] = limit
                        else:
                            x['maps'] = {
                                'maplist': [],
                                'fixedpicklimit': limit,
                                'randomorder': False
                            }
                        if len(shuffle) > 0:
                            if shuffle[:1].lower() == 'o':
                                x['maps']['randomorder'] = False
                            else:
                                x['maps']['randomorder'] = True
                        rkUpdate = True
            if rkUpdate and self.pugInfo.savePugRatings(self.pugInfo.ratingsFile, rkData):
                await ctx.send('Map limit updated for ranked game mode {0}'.format(mode))
                if (self.pugInfo.ranked): # reload data for current ranked mode
                    self.pugInfo.setRankedMode(self.pugInfo.ranked, True)
            elif rkUpdate==False:
                await ctx.send('Error - a ranked map limit could not be saved - game mode not found.')
            else:
                await ctx.send('Error - a ranked map limit could not be saved; check bot logs.')
        return True
    
    @commands.hybrid_command(aliases=['rkmapsim','rksimmap'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkmapsimulation(self, ctx, count=5):
        """Simulates a given number of auto-picks for the active ranked mode, using rules for that mode"""
        if not self.pugInfo.ranked:
            await ctx.send('Ranked mode must be active for map pick simulations to occur.')
            return True
        cachedMLR = self.pugInfo.maps.mapListWeighting
        count = max(1, min(count, 30))
        await ctx.send('Simulating map picks for {0} matches:'.format(count))
        for x in range(count):
            await ctx.send('**Simulation {0}**: {1}'.format(x+1,PLASEP.join(self.pugInfo.maps.autoPickRankedMaps(simulate=True))))
        self.pugInfo.maps.mapListWeighting = cachedMLR # return back to current state
        return True

    @commands.hybrid_command(aliases=['rkresetmaps','rkresetmappri','rkmapresetpri','rkmapresetdesirability'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkresetmapdesirability(self, ctx):
        """Resets map desirability to defaults within an active ranked mode."""
        if not self.pugInfo.ranked:
            await ctx.send('Ranked mode must be active for map desirability factors to be reset.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        self.pugInfo.maps.adjustRankedMapDesirability(action='resetAll')
        self.pugInfo.ratings['maps']['maplist'] = self.pugInfo.maps.mapListWeighting
        if self.pugInfo.savePugRatings(self.pugInfo.ratingsFile):
            await ctx.send('Map desirability values reset to pool defaults.')
            return True

    @commands.hybrid_command(aliases=['rkmapboost','rkboost'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkincreasemapdesirability(self, ctx, map: str='', factor: int = 2):
        """Inceases map desirability within an active ranked mode. Parameters: Map Factor, e.g. AS-Ballistic 2"""
        if not self.pugInfo.ranked:
            await ctx.send('Ranked mode must be active for map desirability factors to be adjusted.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        if self.pugInfo.maps.adjustRankedMapDesirability(action='mapincrease',map=map, adjustment=factor):
            self.pugInfo.ratings['maps']['maplist'] = self.pugInfo.maps.mapListWeighting
            if self.pugInfo.savePugRatings(self.pugInfo.ratingsFile):
                await ctx.send('Map desirability value adjusted.')
                return True
        else:
            await ctx.send('Map desirability value was not adjusted. Please check map name and increase factor values are correct.')
            return True

    @commands.hybrid_command(aliases=['rkmapnerf','rknerf'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkdecreasemapdesirability(self, ctx, map: str='', divisor: int = 2):
        """Decreases map desirability within an active ranked mode. Parameters: Map Divisor, e.g. AS-Bridge 2"""
        if not self.pugInfo.ranked:
            await ctx.send('Ranked mode must be active for map desirability factors to be adjusted.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        if self.pugInfo.maps.adjustRankedMapDesirability(action='mapdecrease',map=map, adjustment=divisor):
            self.pugInfo.ratings['maps']['maplist'] = self.pugInfo.maps.mapListWeighting
            if self.pugInfo.savePugRatings(self.pugInfo.ratingsFile):
                await ctx.send('Map desirability value adjusted.')
                return True
        else:
            await ctx.send('Map desirability value was not adjusted. Please check map name and increase factor values are correct.')
            return True

    @commands.hybrid_command(aliases=['rkmodeconf','rkmodeconfig'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkconf(self, ctx, mode: str = '', capmode: int = 0, role: discord.Role=None, window: int = 0):
        """Configures ranked mode core settings."""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
        rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode']
                    previousSettings = ''
                    if 'capMode' in x:
                        previousSettings = previousSettings+'Captain mode: {0} ({1}); '.format(RATED_CAP_MODE[x['capMode']],x['capMode'])
                        if 'capWindow' in x and int(x['capMode']) == 3:
                            previousSettings = previousSettings+'Time window for captain selection: {0}s; '.format(str(x['capWindow']))
                    if 'capRole' in x and len(x['capRole']) > 0:
                        previousSettings = previousSettings+'Discord role for captain selection: {0}; '.format(x['capRole'])
                    x['capMode'] = max(0, min(capmode, 2)) # clamp to 0-2 - future support for mode 3 will be needed
                    newSettings = 'Captain mode: {0} ({1}); '.format(RATED_CAP_MODE[x['capMode']],x['capMode'])
                    if capmode == 3:
                        x['capWindow'] = max(30, min(window, 240)) # clamp to 30-240
                        newSettings = newSettings+'Time window for captain selection: {0}s; '.format(str(x['capWindow']))
                    else:
                        x['capWindow'] = 0
                    if capmode == 2 and role is not None:
                        x['capRole'] = role.name
                        newSettings = 'Discord role for captain selection: {0}; '.format(x['capRole'])
            if self.pugInfo.savePugRatings(self.pugInfo.ratingsFile, rkData):
                await ctx.send('Ranked game mode {0} configuration updated.\nPrevious settings - {1}\nNew settings - {2}'.format(mode,previousSettings,newSettings))
                if (self.pugInfo.ranked): # reload data for current ranked mode
                    self.pugInfo.setRankedMode(self.pugInfo.ranked, True)
            else:
                await ctx.send('Error - ranked game config could not be updated; check bot logs.')
        return True

    @commands.hybrid_command(aliases=['rkscoreconfig','rkscoreconf'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def rkscoring(self, ctx, mode: str = '', scoremode: str = 'permap', teamwin: int = 0, teamlose: int = 0, capwin: int = 0, caplose: int = 0, volcapwin: int = 0, volcaplose: int = 0):
        """Configures ranked mode scoring settings."""
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if (scoremode.lower() not in ['permap','pergame']):
            await ctx.send('Settings not saved: Score Mode must be either: "permap" or "pergame".')
            return True
        if self.pugInfo.pugLocked and self.pugInfo.ranked:
            await ctx.send('A ranked match is already underway at {0}'.format(self.pugInfo.gameServer.format_gameServerURL))
            await ctx.send('Configuration cannot be modified while a match is in progress.')
            return True
        self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
        rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
        rkUpdate = False
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode']
                    previousSettings = ''
                    if 'scoring' in x:
                        if 'mode' in x['scoring'] and 'teamWin' in x['scoring'] and 'teamLose' in x['scoring'] and 'capWin' in x['scoring'] and 'capLose' in x['scoring']:
                            previousSettings = 'Scoring mode: {0}; Points - Winning team: {1}, Losing Team: {2}, Winning Cap: {3}, Losing Cap: {4}'.format(x['scoring']['mode'],x['scoring']['teamWin'],x['scoring']['teamLose'],x['scoring']['capWin'],x['scoring']['capLose'])
                        if 'capMode' in x and x['capMode'] == 3:
                            if 'volCapWin' in x['scoring'] and (x['scoring']['volCapWin']) > 0:
                                previousSettings = previousSettings+', Winning Voluntary Captain: {0}'.format(x['scoring']['volCapWin'])
                            if 'volCapLose' in x['scoring'] and (x['scoring']['volCapLose']) != 0:
                                previousSettings = previousSettings+', Losing Voluntary Captain: {0}'.format(x['scoring']['volCapLose'])
                    x['scoring'] = {
                        'mode': scoremode,
                        'teamWin': max(0, teamwin),
                        'teamLose': teamlose,
                        'capWin': max(0, capwin),
                        'capLose': caplose,
                        'volCapWin': max(0, volcapwin),
                        'volCapLose': volcaplose
                    }
                    newSettings = 'Scoring mode: {0}; Points - Winning team: {1}, Losing Team: {2}, Winning Cap: {3}, Losing Cap: {4}'.format(x['scoring']['mode'],x['scoring']['teamWin'],x['scoring']['teamLose'],x['scoring']['capWin'],x['scoring']['capLose'])
                    if 'capMode' in x and x['capMode'] == 3:
                        newSettings = newSettings+', Winning Voluntary Captain: {0}, Losing Voluntary Captain: {1}'.format(x['scoring']['volCapWin'],x['scoring']['volCapLose'])
                    rkUpdate = True
        if rkUpdate == True:
            if self.pugInfo.savePugRatings(self.pugInfo.ratingsFile, rkData):
                await ctx.send('Ranked game mode {0} configuration updated.\nPrevious settings - {1}\nNew settings - {2}'.format(mode,previousSettings,newSettings))
                if (self.pugInfo.ranked): # reload data for current ranked mode
                        self.pugInfo.setRankedMode(self.pugInfo.ranked, True)
                else:
                    await ctx.send('Error - ranked game config could not be updated; check bot logs.')
        else:
            await ctx.send('Error - ranked mode not found, or no ranked configuration exists.')
        return True

    @commands.hybrid_command(aliases=['rklist'])
    @commands.guild_only()
    async def rkrecent(self, ctx, mode: str = '', last: int = 5, completed: str = ''):
        """Returns recent ranked matches"""
        if (mode in [None,'']):
            if self.pugInfo.ranked:
                mode = self.pugInfo.mode
            else:
                await ctx.send('A valid ranked mode must be specified.')
                return True
        games = []
        msg = []
        if self.pugInfo.pugLocked != True and self.pugInfo.ranked:
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
        rkData = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
        if 'rankedgames' in rkData:
            for x in rkData['rankedgames']:
                if 'mode' in x and str(x['mode']).upper() == mode.upper():
                    mode = x['mode']
                    games = sorted(x['games'], key=lambda g: datetime.fromisoformat(g['startdate']), reverse=True)
                    players = {}
                    if 'ratings' in x:
                        for p in x['ratings']:
                            players[p['did']] = p['dlastnick']
        if len(games):
            msg = 'Recent {0} ranked games:\n'.format(mode)
        else:
            msg = 'No games were found for mode: {0}, please specify a valid mode.'.format(mode)
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
                        teamred.append(players[x]+'({0})'.format(CAPSIGN))
                    else:
                        teamred.append(players[x])
                for x in g['teamblue']:
                    if g['capblue']['id'] == x:
                        teamblue.append(players[x]+'({0})'.format(CAPSIGN))
                    else:
                        teamblue.append(players[x])
                msg = msg+'{0}) Match Ref: `{1}`; Started {2}, {3}\n'.format(i, g['gameref'],g_startdate,g_enddate)
                msg = msg+'> Red team (RP: {0}): {1}\n> Blue team (RP: {2}): {3}\n'.format(g['rpred'],PLASEP.join(teamred),g['rpblue'],PLASEP.join(teamblue))
                msg = msg+'> Score :red_square: {0} - {1} :blue_square:\n\n'.format(g['scorered'],g['scoreblue'])
        if len(msg) > 4000:
            for m in msg.split('\n'):
                if len(m):
                    await ctx.send(m)
        else:
            await ctx.send(msg)
        return True

    @commands.hybrid_command(aliases=['rkreport'])
    @commands.guild_only()
    async def rkrp(self, ctx, mode: str = '', matchref: str = '', player: discord.Member = None):
        """Returns match and player RP reports"""
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
            if self.pugInfo.ranked:
                mode = self.pugInfo.mode
            else:
                await ctx.send('A valid ranked mode must be specified.')
                return True
        else:
            if self.pugInfo.ranked and self.pugInfo.mode.upper() == mode.upper():
                mode = self.pugInfo.mode # fix case
        teamRed = []
        teamBlue = []
        reports = []
        if self.isPugInProgress and self.pugInfo.ranked and len(matchref) == 0 and player in ['',None]:
            teamBlue = self.pugInfo.blue
            teamRed = self.pugInfo.red
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
    async def rkvoidmatch(self, ctx, mode: str = '', matchref: str = ''):
        if (self.pugInfo.pugLocked):
            await ctx.send('Matches cannot be voided while a game is in progress. Please try again later.')
            return True
        if (mode in [None,'']):
            await ctx.send('A valid ranked mode must be specified.')
            return True
        if (matchref in [None,'']):
            await ctx.send('A valid match reference must be specified.')
            return True
        if self.pugInfo.ranked and self.pugInfo.mode.upper() == mode.upper():
            mode = self.pugInfo.mode
        matchInfo = self.ratingsMatchInfo(mode,matchref)
        if matchInfo == {}:
            await ctx.send('The provided valid match reference could not be found.')
        else:
            started = datetime.fromisoformat(matchInfo['startdate'])
            await ctx.send('{0} match `{1}` played on {2} at {3}...'.format(('Voiding' if matchInfo['completed'] else 'Re-establishing'),matchInfo['gameref'],started.strftime('%d/%m/%Y'),started.strftime('%H:%M:%S')))
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile)
            rk = self.pugInfo.loadPugRatings(self.pugInfo.ratingsFile, True)
            if 'rankedgames' in rk:
                for x in rk['rankedgames']:
                    if 'games' in x and 'mode' in x and x['mode'].upper() == mode.upper():
                        for g in x['games']:
                            if 'gameref' in g and g['gameref'].upper() == matchref.upper():
                                g['completed'] = not g['completed']
                                log.debug('rkvoidmatch() - Found match data in rk; completed={0}'.format(g['completed']))
            self.pugInfo.savePugRatings(self.pugInfo.ratingsFile,rk)
            self.pugInfo.setRankedMode(MODE_CONFIG[self.pugInfo.mode].isRanked, False)
            players = []
            players.extend(matchInfo['teamred'])
            players.extend(matchInfo['teamblue'])
            for p in players:
                player = self.ratingsPlayerDataHandler('rkget', mode, p)
                if player not in [None,{},'']:
                    msg = self.ratingsPlayerDataHandler('rkrecalc',mode,p,0)
                    await ctx.send('> Recalculated RP for {0}...\n> - Updated to last event: {1}'.format(player['dlastnick'],msg.split('\n')[-2].replace('> ','')))
                else:
                    await ctx.send('> Could not recalculate RP for ID `{0}`; player not found.'.format(p))
        return True

    #########################################################################################
    # Bot commands.
    #########################################################################################
    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def disable(self, ctx):
        """Disables PUG commands in the channel. Note only one channel can be active at a time. Admin only"""
        if self.activeChannel:
            await self.activeChannel.send('PUG commands now disabled.')
            if ctx.message.channel != self.activeChannel:
                await ctx.send('PUG commands are disabled in ' + self.activeChannel.mention)
            self.activeChannel = None
            self.savePugConfig(self.configFile)
            return
        await ctx.send('PUG commands were not active in any channels.')
    
    @commands.hybrid_command(aliases = ['pug'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def list(self, ctx):
        """Displays pug status"""
        if self.pugInfo.pugLocked:
            # Pug in progress, show the teams/maps.
            await ctx.send(self.pugInfo.format_match_in_progress)
        elif self.pugInfo.teamsReady and self.pugInfo.ranked:
            # Ranked mode, just display teams and maps
            msg = '\n'.join([
                self.pugInfo.format_pug_short,
                self.pugInfo.format_teams(),
                self.pugInfo.maps.format_current_maplist])
            await ctx.send(msg)
        elif self.pugInfo.teamsReady:
            # Picking maps, just display teams.
            msg = '\n'.join([
                self.pugInfo.format_pug_short,
                self.pugInfo.format_teams(),
                self.pugInfo.maps.format_current_maplist,
                self.format_pick_next_map(mention=False)])
            await ctx.send(msg)
        elif self.pugInfo.captainsReady:
            # Picking players, show remaining players to pick, but don't
            # highlight the captain to avoid annoyance.
            msg = '\n'.join([
                self.pugInfo.format_pug_short,
                self.pugInfo.format_remaining_players(number=True),
                self.pugInfo.format_teams(),
                self.format_pick_next_player(mention=False)])
            await ctx.send(msg)
        else:
            # Default, show sign ups.
            msg = []
            msg.append(self.pugInfo.format_pug())
            if self.pugInfo.playersReady and not self.pugInfo.ranked:
                # Copy of what's in processPugStatus, not ideal, but avoids the extra logic it does.
                if self.pugInfo.numCaptains == 1:
                    # Need second captain.
                    msg.append('Waiting for 2nd captain. Type **!captain** to become a captain. To choose a random captain type **!randomcaptains**')
                else:
                    msg.append('Waiting for captains. Type **!captain** to become a captain. To choose random captains type **!randomcaptains**')
            await ctx.send('\n'.join(msg))
            await self.processPugStatus(ctx)

    @commands.hybrid_command(aliases = ['pugtime'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Ignore)
    async def promote(self, ctx):
        """Promotes the pug. Limited to once per minute alongside poke."""
        # TODO: Switch the use of these times of limits to use the "cooldown" decorator. see https://stackoverflow.com/questions/46087253/cooldown-for-command-on-discord-bot-python
        delay = 60
        # reusing lastpoketime, so both are limited to one of the two per 60s
        if (datetime.now() - self.lastPokeTime).total_seconds() < delay:
            return
        self.lastPokeTime = datetime.now()
        await ctx.send('Hey @here it\'s PUG TIME!!!\n**{0}** needed for **{1}**!'.format(self.pugInfo.playersNeeded, self.pugInfo.desc))

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Ignore)
    async def poke(self, ctx):
        """Highlights those signed to pug. Limited to once per minute alongside promote."""
        # TODO: Switch the use of these times of limits to use the "cooldown" decorator. see https://stackoverflow.com/questions/46087253/cooldown-for-command-on-discord-bot-python
        minPlayers = 2
        delay = 60
        if self.pugInfo.numPlayers < minPlayers or (datetime.now() - self.lastPokeTime).total_seconds() < delay:
            return
        self.lastPokeTime = datetime.now()
        await ctx.send('Poking those signed (you will be unable to poke for {0} seconds): {1}'.format(delay, self.pugInfo.format_all_players(number=False, mention=True)))

    @commands.hybrid_command(aliases = ['serverlist'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def listservers(self, ctx):
        await ctx.send(self.pugInfo.gameServer.format_showall_servers)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def server(self, ctx):
        """Displays Pug server info"""
        await ctx.send(self.pugInfo.gameServer.format_game_server)

    @commands.hybrid_command(aliases = ['serverinfo'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def serverstatus(self, ctx):
        """Displays Pug server current status"""
        await self.queryServerStats(True)
        if self.pugInfo.gameServer.utQueryEmbedCache != {}:
            embedInfo = discord.Embed().from_dict(self.pugInfo.gameServer.utQueryEmbedCache)
            # Strip objectives from the card data
            for x, f in enumerate(embedInfo.fields):
                if 'Objectives' in f.name:
                    embedInfo.remove_field(x)
            await ctx.send(embed=embedInfo)
        else:
            await ctx.send(self.pugInfo.gameServer.format_game_server_status)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def serverquery(self, ctx, serveraddr: str, hideheader: bool = True):
        """Displays status of a given server"""
        serverinfo = {}
        if (self.pugInfo.gameServer.utQueryReporterActive or self.pugInfo.gameServer.utQueryStatsActive):
            await ctx.send('Server query cannot be run while pug reporting is in progress.')
        else:
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
                                log.warning('DNS lookup failure for {0}'.format(serveraddr))
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
                self.pugInfo.gameServer.utQueryData = serverinfo
                await self.queryServerStats(True)
                if self.pugInfo.gameServer.utQueryEmbedCache != {}:
                    embedInfo = discord.Embed().from_dict(self.pugInfo.gameServer.utQueryEmbedCache)
                    # Reset caches
                    self.pugInfo.gameServer.utQueryData = {}
                    self.pugInfo.gameServer.utQueryEmbedCache = {}
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
    async def listmodes(self, ctx):
        """Lists available modes for the pug"""
        outStr = ['Available modes are:']
        for k in MODE_CONFIG:
            outStr.append(PLASEP + '**' + k + '**')
        outStr.append(PLASEP)
        await ctx.send(' '.join(outStr))

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def setmode(self, ctx, mode):
        """Sets mode of the pug"""
        if self.pugInfo.captainsReady:
            await ctx.send('Pug already in picking mode. Reset if you wish to change mode.')
        else:
            result = self.pugInfo.setMode(mode)
            # Send result message to channel regardless of success/failure
            await ctx.send(result[1])
            # If mode successfully changed, process pug status
            if result[0]:
                await self.processPugStatus(ctx)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def setplayers(self, ctx, limit: int):
        """Sets number of players"""
        if self.pugInfo.ranked != True and self.pugInfo.captainsReady:
            await ctx.send('Pug already in picking mode. Reset if you wish to change player limit.')
        elif (limit % 2 == 0 and limit >= MODE_CONFIG[self.pugInfo.mode].minPlayers and limit <= MODE_CONFIG[self.pugInfo.mode].maxPlayers):
            self.pugInfo.setMaxPlayers(limit)
            await ctx.send('Player limit set to ' + str(self.pugInfo.maxPlayers))
            await self.processPugStatus(ctx)
        else:
            await ctx.send('Player limit unchanged. Players must be a multiple of 2 + between {0} and {1}'.format(str(MODE_CONFIG[self.pugInfo.mode].minPlayers),str(MODE_CONFIG[self.pugInfo.mode].maxPlayers)))

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    @commands.check(admin.hasManagerRole_Check)
    async def adminsetplayers(self, ctx, limit: int):
        """Force sets number of players"""
        if self.pugInfo.ranked != True and self.pugInfo.captainsReady:
            await ctx.send('Pug already in picking mode. Reset if you wish to change player limit.')
        elif (limit % 2 == 0):
            self.pugInfo.setMaxPlayers(limit)
            await ctx.send('Player limit forcefully set to {0}. ({1} min: {2}, max: {3})'.format(str(self.pugInfo.maxPlayers),self.pugInfo.mode,str(MODE_CONFIG[self.pugInfo.mode].minPlayers),str(MODE_CONFIG[self.pugInfo.mode].maxPlayers)))
            await self.processPugStatus(ctx)
        else:
            await ctx.send('Player limit unchanged. Players must be a multiple of 2')


    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def setmaps(self, ctx, limit: int):
        """Sets number of maps"""
        if (self.pugInfo.ranked and self.pugInfo.ratings is not None and 'maps' in self.pugInfo.ratings and 'fixedpicklimit' in self.pugInfo.ratings['maps'] and self.pugInfo.ratings['maps']['fixedpicklimit'] > 0):
            await ctx.send('Map limit is fixed to {0} maps within this ranked mode.'.format(str(self.pugInfo.maps.maxMaps)))
            return
        if (self.pugInfo.maps.setMaxMaps(limit)):
            await ctx.send('Map limit set to ' + str(self.pugInfo.maps.maxMaps))
            if self.pugInfo.teamsReady:
                # Only need to do this if maps already being picked, as it could mean the pug needs to be setup.
                await self.processPugStatus(ctx)
        else:
            await ctx.send('Map limit unchanged. Map limit is {}'.format(self.pugInfo.maps.maxMapsLimit))

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def reset(self, ctx):
        """Resets the pug. Players must rejoin and server is reset even if a match is running. Use with care."""
        reset = False
        if (admin.hasManagerRole_Check(ctx) or not(self.pugInfo.pugLocked or (self.pugInfo.gameServer and self.pugInfo.gameServer.matchInProgress))):
            reset = True
        else:
            requester = ctx.message.author
            if requester in self.pugInfo.red:
                if self.resetRequestRed:
                    await ctx.send('Red team have already requested reset. Blue team must also request.')
                else:
                    self.resetRequestRed = True
                    await ctx.send('Red team have requested reset. Blue team must also request.')
            elif requester in self.pugInfo.blue:
                if self.resetRequestBlue:
                    await ctx.send('Blue team have already requested reset. Red team must also request.')
                else:
                    self.resetRequestBlue = True
                    await ctx.send('Blue team have requested reset. Red team must also request.')
            else:
                await ctx.send('Pug is in progress, only players involved the pug or admins can reset.')
            if self.resetRequestRed and self.resetRequestBlue:
                self.resetRequestRed = False
                self.resetRequestBlue = False
                reset = True
        if reset:
            await ctx.send('Removing all signed players: {}'.format(self.pugInfo.format_all_players(number=False, mention=True)))
            if len(self.pugInfo.queuedPlayers):
                await ctx.send('Removing all queued players: {}'.format(self.pugInfo.format_queued_players(number=False, mention=True)))
            if self.pugInfo.resetPug(True):
                await ctx.send('Pug Reset: {}'.format(self.pugInfo.format_pug_short))
            else:
                await ctx.send('Reset failed. Please, try again or inform an admin.')

    @commands.hybrid_command(aliases=['replay'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def retry(self, ctx):
        if self.pugInfo.gameServer.matchInProgress is False or self.pugInfo.gameServer.gameServerOnDemand:
            retryAllowed = True
        else:
            retryAllowed = False

        if self.pugInfo.matchReady and retryAllowed:
            await self.processPugStatus(ctx)
        else:
            # TODO: Recall saved data from last match and play it back into the bot
            await ctx.send('Retry can only be utilised after a failed setup.')

    @commands.hybrid_command(aliases=['resetcaps','resetcap'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def resetcaptains(self, ctx):
        """Resets back to captain mode. Any players or maps picked will be reset."""
        if self.pugInfo.ranked or self.pugInfo.numCaptains < 1 or self.pugInfo.pugLocked:
            return

        self.pugInfo.maps.resetMaps()
        self.pugInfo.softPugTeamReset()
        await ctx.send('Captains have been reset.')
        await self.processPugStatus(ctx)

    @commands.hybrid_command(aliases=['jq'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def queue(self, ctx):
        """Joins a queue of players for the next pug"""
        await self.join(ctx, 'queue')
        return
    
    @commands.hybrid_command(aliases=['j'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Warn)
    async def join(self, ctx, notes: str = ''):
        """Joins the pug"""
        player = ctx.message.author
        flags = ''
        notesmsg = ''
        if notes not in [None,'']:
            if notes.lower() == "nomic":
                flags = notes.lower()
                notesmsg = ' and tagged as "no mic"'
            if (notes.lower()[:1] == "q" or notes.lower()[:4] == "next"):
                if player in self.pugInfo.queuedPlayers:
                    await ctx.send('{0} is already queued for the next pug.'.format(display_name(player)))
                    return True
                if self.pugInfo.pugLocked or self.pugInfo.playersReady:
                    flags = 'queue'
                    notesmsg = ' to the queue for the next pug'
                else:
                    notesmsg = ' immediately, as a pug is not yet running'
        if self.pugInfo.ranked:
            if not self.pugInfo.addRankedPlayer(player, flags):
                if self.pugInfo.playersReady:
                    await ctx.send('Ranked pug is already full.')
                    return
                elif player in self.pugInfo.players:
                    await ctx.send('Already added.')
                    return
                else:
                    await ctx.send('{0} could not be added - ineligible to join a ranked pug.'.format(display_name(player)))
                    return
        else:
            if not self.pugInfo.addPlayer(player, flags):
                if self.pugInfo.playersReady:
                    await ctx.send('Pug is already full.')
                    return
                else:
                    await ctx.send('Already added.')
                    return
        if flags != 'queue':
            await ctx.send('{0} was added{1}.\n{2}'.format(display_name(player), notesmsg, self.pugInfo.format_pug()))
        else:
            await ctx.send('{0} was added{1}.'.format(display_name(player), notesmsg))
        await self.processPugStatus(ctx)

    @commands.hybrid_command(aliases=['l', 'lva', 'lq'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def leave(self, ctx):
        """Leaves the pug"""
        player = ctx.message.author
        if player in self.pugInfo.queuedPlayers:
            self.pugInfo.removePlayerFromPug(player)
            await ctx.send('{0} has left the queue.'.format(display_name(player)))
            return True
        if self.pugInfo.pugLocked == False:
            if self.pugInfo.removePlayerFromPug(player):
                await ctx.send('{0} left.'.format(display_name(player)))
                await self.processPugStatus(ctx)
        else:
            await self.isPugInProgress(ctx, True)
        return True
    
    @commands.hybrid_command(aliases=['cap','сфзефшт'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Ignore)
    async def captain(self, ctx):
        """Volunteer to be a captain in the pug"""
        if self.pugInfo.ranked or not self.pugInfo.playersReady or self.pugInfo.captainsReady or self.pugInfo.gameServer.matchInProgress:
            log.debug('!captain rejected: Players Ready = {0}, Captains Ready = {1}, Match In Progress {2}'.format(self.pugInfo.playersReady,self.pugInfo.captainsReady,self.pugInfo.gameServer.matchInProgress))
            return

        player = ctx.message.author
        if self.pugInfo.setCaptain(player):
            await ctx.send(player.mention + ' has volunteered as a captain!')
            await self.processPugStatus(ctx)

    @commands.hybrid_command(aliases=['randcap'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    @commands.check(isPugInProgress_Ignore)
    async def randomcaptains(self, ctx):
        """Picks a random captain for each team without a captain."""
        if self.pugInfo.ranked or not self.pugInfo.playersReady or self.pugInfo.captainsReady:
            return

        while not self.pugInfo.captainsReady:
            pick = None
            while not pick:
                pick = random.choice(self.pugInfo.players)
            self.pugInfo.setCaptain(pick)
        await self.processPugStatus(ctx)

    @commands.command(aliases=['p'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def pick(self, ctx, *players: int): # hybrid_command doesn't support an undefined number of params - may need adjusting
        """Picks a player for a team in the pug"""
        captain = ctx.message.author
        # TODO: improve this, don't think we should use matchInProgress
        if self.pugInfo.ranked or self.pugInfo.teamsFull or not self.pugInfo.captainsFull or not captain == self.pugInfo.currentCaptainToPickPlayer or self.pugInfo.pugLocked or self.pugInfo.gameServer.matchInProgress:
            return

        picks = list(itertools.takewhile(functools.partial(self.pugInfo.pickPlayer, captain), (x - 1 for x in players)))

        if picks:
            if self.pugInfo.teamsFull:
                await ctx.send('Teams have been selected:\n{}'.format(self.pugInfo.format_teams(mention=True)))
            await self.processPugStatus(ctx)

    @commands.hybrid_command(aliases=['maplist'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def listmaps(self, ctx, str: str=""):
        """Returns the list of maps to pick from"""
        if (self.pugInfo.ranked):
            if (str == "all"):
                msg = ['Ranked mode ({0}) will pick from the following underscored maps: '.format(self.pugInfo.mode)]
                msg.append(self.pugInfo.maps.format_available_maplist)
            else:
                msg = ['Ranked mode ({0}) will pick from the following map list: '.format(self.pugInfo.mode)]
                msg.append(self.pugInfo.maps.format_filtered_maplist)
        else:
            msg = ['Server map list is: ']
            msg.append(self.pugInfo.maps.format_available_maplist)
        await ctx.send('\n'.join(msg))

    @commands.hybrid_command(aliases=['m'])
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def map(self, ctx, idx: int):
        """Picks a map in the pug"""

        captain = ctx.message.author
        if (self.pugInfo.ranked or self.pugInfo.matchReady or not self.pugInfo.teamsReady or captain != self.pugInfo.currentCaptainToPickMap):
            # Skip if in ranked mode or not in captain mode with full teams or if the author is not the next map captain.
            return

        mapIndex = idx - 1 # offset as users see them 1-based index.
        if mapIndex < 0 or mapIndex >= len(self.pugInfo.maps.availableMapsList):
            await ctx.send('Pick a valid map. Use !map <map_number>. Use !listmaps to see the list of available maps.')
            return

        if not self.pugInfo.pickMap(captain, mapIndex):
            await ctx.send('Map already picked. Please, pick a different map.')
        
        msg = ['Maps chosen **({0} of {1})**:'.format(len(self.pugInfo.maps), self.pugInfo.maps.maxMaps)]
        msg.append(self.pugInfo.maps.format_current_maplist)
        await ctx.send(' '.join(msg))
        await self.processPugStatus(ctx)

    @commands.hybrid_command()
    @commands.guild_only()
    @commands.check(isActiveChannel_Check)
    async def last(self, ctx):
        """Shows the last pug info"""
        if self.pugInfo.gameServer.matchInProgress:
            msg = ['Last match not complete...']
            msg.append(self.pugInfo.format_match_in_progress)
            await ctx.send('\n'.join(msg))
        else:
            await ctx.send(self.pugInfo.format_last_pug)

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
    async def mutereporter(self, ctx):
        """Mutes the UT Server Reporter until the next active pug. Admin only"""
        if (self.pugInfo.gameServer.utQueryReporterActive or self.pugInfo.gameServer.utQueryStatsActive) and self.utReporterChannel is not None:
            self.pugInfo.gameServer.utQueryReporterActive = False
            self.pugInfo.gameServer.utQueryStatsActive = False
            await ctx.send('Muted UT Reporter threads in the reporter channel')
        else:
            await ctx.send('UT Reporter channel not defined, or threads not currently running.')
        return

    @commands.hybrid_command(aliases = ['startrep','forcerep'])
    @commands.guild_only()
    @commands.check(admin.hasManagerRole_Check)
    async def startreporter(self, ctx):
        """Force-starts the UT Server Reporter, whether an active pug is running or not. Admin only"""
        if self.pugInfo.gameServer.utQueryStatsActive or self.pugInfo.gameServer.utQueryReporterActive:
            if self.utReporterChannel is None:
                await ctx.send('UT Reporter channel has not yet been configured, use **!setreporter** to configure the target channel.')
            elif self.utReporterChannel != ctx.message.channel:
                await ctx.send('UT Reporter is already active in another channel.')
            else:
                await ctx.send('UT Reporter is already active in this channel.')
        else:
            if self.pugInfo.gameServer.utQueryServer('info'):
                self.utReporterChannel = ctx.message.channel
                if 'code' in self.pugInfo.gameServer.utQueryData and self.pugInfo.gameServer.utQueryData['code'] == 200:
                    self.pugInfo.gameServer.utQueryStatsActive = True
                    self.pugInfo.gameServer.utQueryReporterActive = True
                    await ctx.send('Force-started UT Reporter threads in this channel')
        return
async def setup(bot):
    await bot.add_cog(PUG(bot, DEFAULT_CONFIG_FILE))
