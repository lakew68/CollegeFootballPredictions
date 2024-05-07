'''
Gathers relevant CFB stats by game. Takes a long time to run the first time, but is much faster in subsequent runs.
'''

# Requires cfbd: pip install cfbd
from __future__ import print_function
import cfbd
from cfbd.rest import ApiException
from pprint import pprint
from datetime import datetime
import os.path
import pickle
import requests
import numpy as np

def gather_game_data(configuration):
    '''Takes in the configuration for the cfb data api and returns statistics organized by college football game for use
    in the model. Also backs up gathered data for faster processing.'''

    if os.path.isfile('CFBGameData.dat'):
        with open("CFBGameData.dat",'rb') as f:
            cached_games = pickle.load(f) # Multi-level dictionary. 
                                        #Outer level is year of game, then week, then game id keying a dictionary of game data
    else:
        cached_games = {2013:{0:{}}}


    current_year = datetime.now().year
    max_year_in_cache = max(cached_games.keys())
    max_week_in_cache = max(cached_games[max_year_in_cache].keys())

    lines = []
    games = []
    for year in range(max_year_in_cache, current_year):
        print('Gathering games from ', year)
        response = games_api.get_games(year=year)
        games = [*games, *response]

        response = betting_api.get_lines(year=year)
        lines = [*lines, *response]

    games2 = [
        dict(
            gid = g.id,
            year = g.season,
            week = g.week,
            neutral_site = g.neutral_site,
            home_team = g.home_team,
            home_conference = g.home_conference,
            home_points = g.home_points,
            home_elo = g.home_pregame_elo,
            away_team = g.away_team,
            away_conference = g.away_conference,
            away_points = g.away_points,
            away_elo = g.away_pregame_elo
        ) for g in games if g.home_points is not None and g.away_points is not None]

    for game in games2:
        game['margin'] = game['away_points'] - game['home_points'] # Create margin of victory statistic

    for game in games2:
        # This loop finds game betting data for games that have it
        game_lines = [l for l in lines if l.id == game['gid']]
        if len(game_lines) > 0:
            game_line = [l for l in game_lines[0].lines if l.provider == 'consensus']
            if len(game_line) == 0 or game_line[0].spread is None:
                if len(game_lines[0].lines) > 0:
                    game_line = [game_lines[0].lines[0]]
            if len(game_line) > 0 and game_line[0].spread is not None:
                game['spread'] = float(game_line[0].spread)

    headers = {'Authorization': configuration.api_key_prefix['Authorization'] + ' ' + configuration.api_key['Authorization']} 
    games2 = process_games(games2, headers)

    max_year_in_games2 = max([games2[i]['year'] for i in range(len(games2))])
    
    for year in range(max_year_in_cache,max_year_in_games2+1):
        print('Adding games to cache from ', year)
        cached_games[year] = {}
        max_week = max([games2[i]['week'] for i in range(len(games2)) if games2[i]['year']==year])
        for week in range(0,max_week_in_games2+1): # Possibly refilling old data, data gets updates for a few weeks after games
            cached_games[year][week] = {}
            for game in games2:
                if game['week'] == week and game['year'] == year:
                    # Some missing data checked below
                    if ('away', 'interceptionYards') not in game.keys():
                        game[('away', 'interceptionYards')] = 0
                    if ('away', 'passesIntercepted') not in game.keys():
                        game[('away', 'passesIntercepted')] = 0
                    if ('away', 'interceptionTDs') not in game.keys():
                        game[('away', 'interceptionTDs')] = 0
                    if ('home', 'interceptionYards') not in game.keys():
                        game[('home', 'interceptionYards')] = 0
                    if ('home', 'passesIntercepted') not in game.keys():
                        game[('home', 'passesIntercepted')] = 0
                    if ('home', 'interceptionTDs') not in game.keys():
                        game[('home', 'interceptionTDs')] = 0

                    cached_games[year][week][game['gid']] = {key:game[key] for key in game if key!='gid'} # Don't need ID data
    
    with open("CFBGameData.dat",'wb') as f:
        pickle.dump(cached_games,f)
        
    return cached_games

def process_games(games, headers):
    '''Takes in a list of games, where each game is a dictionary of game information. Populates that list with game stats. '''
    
    count = 0
    curr_week_year = (0,0)
    base_url = 'https://api.collegefootballdata.com'
    total_stats = [('defense', 'plays'),('defense', 'drives'),('defense', 'totalPPA'),('defense', 'lineYardsTotal'),('defense', 'secondLevelYardsTotal'),
             ('defense', 'openFieldYardsTotal'),('defense', 'totalOpportunies'),('defense','passingDowns', 'totalPPA'),('defense','rushingPlays', 'totalPPA'),
              ('defense', 'passingPlays', 'totalPPA'),('offense', 'plays'),('offense', 'drives'),('offense', 'totalPPA'),('offense', 'lineYardsTotal'),
              ('offense', 'secondLevelYardsTotal'),('offense', 'openFieldYardsTotal'),('offense', 'totalOpportunies'),('offense','passingDowns', 'totalPPA'),
              ('offense','rushingPlays', 'totalPPA'),('offense', 'passingPlays', 'totalPPA')] # For normalizing advanced stats
                
    
    for game in games:
        year = game['year']
        week = game['week']
        home_team = game['home_team']
        away_team = game['away_team']
        if (week,year) != curr_week_year and (year != 2014 or week != 2): # Site has an error for 2014 week 2
            season_stats = [] # This will be all stats from the season endpoint
            advanced_stats = [] # This will be all stats from the season/advanced endpoint
            print('Compiling games from week, year: ', week,year)
            curr_week_year = (week,year)
            if week > 1:
                endpoint = "/stats/season"
                params = {
                            "year": year,
                            "endWeek" : week - 1,
                            "excludeGarbageTime" : True
                        }
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code != 200:
                    print('AHHHH', response)
                season_stats = response.json()
                params = {
                    "year": year,
                    "endWeek" : week - 1

                }
                endpoint = "/stats/season/advanced"
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
                advanced_stats = response.json()

            else:
                # Need placeholder data that contains all the statistics
                endpoint = "/stats/season"
                params = {
                            "year": 2022,
                            "endWeek" : 5,
                            "team" : 'Texas',
                            "excludeGarbageTime" : True
                        }
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
                season_stats = response.json()
                params = {
                    "year": 2022,
                    "endWeek" : 5,
                    "team" : 'Texas'

                }
                endpoint = "/stats/season/advanced"
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
                advanced_stats = response.json()


        if week > 1:
            stat_dict = {}

            for location in ['home','away']:

                if location == 'home':
                    idx_team1 = [i for i in range(len(season_stats)) if season_stats[i]['team'] == home_team]
                    idx_team2 = [i for i in range(len(advanced_stats)) if advanced_stats[i]['team'] == home_team]
                else:
                    idx_team1 = [i for i in range(len(season_stats)) if season_stats[i]['team'] == away_team]
                    idx_team2 = [i for i in range(len(advanced_stats)) if advanced_stats[i]['team'] == away_team]

                if len(idx_team1) > 0:
                    team_stats = np.array(season_stats)[idx_team1]
                else:
                    team_stats = []
                if len(idx_team2) > 0:
                    team_advanced_stats = [advanced_stats[idx_team2[0]]]
                else:
                    team_advanced_stats = []
                    
                # Below block normalizes team_stats statistics by games played
                team_stats_copy = {}
                for stat_idx, stat in enumerate(team_stats):
                    if stat['statName'] == 'games':
                        num_games = stat['statValue']
                
                for stat in team_stats:
                    if stat['statName'] == 'games':
                        stat_dict[(location,stat['statName'])] = stat['statValue']
                    else:
                        stat_dict[(location,stat['statName'])] = stat['statValue'] / num_games
                
                if len(team_advanced_stats) > 0:
                    defense_stats = team_advanced_stats[0]['defense']
                    defense_plays = defense_stats['plays']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    stat_per_play = defense_stats[name][name2] / defense_plays
                                    stat_dict[(location,'defense',name,name2,'perPlay')] = stat_per_play
                                    defense_stats[name][name2] /= num_games
                                stat_dict[(location,'defense',name,name2)] = defense_stats[name][name2]
                        else:
                            if ('defense',name) in total_stats:
                                stat_per_play = defense_stats[name] / defense_plays
                                stat_dict[(location,'defense',name,'perPlay')] = stat_per_play
                                defenseStats[name] /= num_games
                            stat_dict[(location,'defense',name)] = defense_stats[name]
                    offense_stats = team_advanced_stats[0]['offense']
                    offense_plays = offense_stats['plays']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    stat_per_play = offense_stats[name][name2] / offense_plays
                                    stat_dict[(location,'offense',name,name2,'perPlay')] = stat_per_play
                                    offense_stats[name][name2] /= num_games
                                stat_dict[(location,'offense',name,name2)] = offense_stats[name][name2]
                        else:
                            if ('offense',name) in total_stats:
                                stat_per_play = offense_stats[name] / offense_plays
                                stat_dict[(location,'offense',name,'perPlay')] = stat_per_play
                                offense_stats[name] /= num_games
                            stat_dict[(location,'offense',name)] = offense_stats[name]
        else:
            stat_dict = {}
            for location in ['home','away']:
                for stat in season_stats:
                    stat_dict[(location,stat['statName'])] = 0
                if len(advanced_stats) > 0:
                    defense_stats = advanced_stats[0]['defense']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    stat_dict[(location,'defense',name,name2,'perPlay')] = 0
                                stat_dict[(location,'defense',name,name2)] = 0
                        else:
                            if ('defense',name) in total_stats:
                                stat_dict[(location,'defense',name,'perPlay')] = 0
                            stat_dict[(location,'defense',name)] = 0
                    offense_stats = advanced_stats[0]['offense']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    stat_dict[(location,'offense',name,name2,'perPlay')] = 0
                                stat_dict[(location,'offense',name,name2)] = 0
                        else:
                            if ('offense',name) in total_stats:
                                stat_dict[(location,'offense',name,'perPlay')] = 0
                            stat_dict[(location,'offense',name)] = 0

        for stat in stat_dict.keys():
            game[stat] = stat_dict[stat]
        count += 1
        
    return games
