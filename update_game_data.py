import os.path
import pickle
from datetime import datetime
import cfbd
from cfbd.rest import ApiException
import requests
import numpy as np

def gather_game_data(configuration):
    '''Takes in the configuration for the cfb data api and returns statistics organized by college football game for use
    in the model. Also backs up gathered data for faster processing.'''
    
    api_config = cfbd.ApiClient(configuration)
    headers = {'Authorization': configuration.api_key_prefix['Authorization'] + ' ' + configuration.api_key['Authorization']} 

    # Some useful shortcuts
    teams_api = cfbd.TeamsApi(api_config)
    ratings_api = cfbd.RatingsApi(api_config)
    games_api = cfbd.GamesApi(api_config)
    stats_api = cfbd.StatsApi(api_config)
    betting_api = cfbd.BettingApi(api_config)

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
    
    stat_keys = ['year', 'week', 'neutral_site', 'home_team', 'home_conference', 'home_points', 'home_elo', 'away_team', 'away_conference', 'away_points', 'away_elo', 'margin', 'spread', ('home', 'rushingYards'), ('home', 'rushingTDs'), ('home', 'passAttempts'), ('home', 'passingTDs'), ('home', 'games'), ('home', 'puntReturnTDs'), ('home', 'firstDowns'), ('home', 'sacks'), ('home', 'interceptionTDs'), ('home', 'kickReturnTDs'), ('home', 'totalYards'), ('home', 'fourthDownConversions'),('home', 'rushingAttempts'),('home', 'possessionTime'),('home', 'fourthDowns'),('home', 'tacklesForLoss'),('home', 'puntReturnYards'),('home', 'passCompletions'),('home', 'puntReturns'),('home', 'kickReturns'),('home', 'thirdDownConversions'),('home', 'fumblesRecovered'),('home', 'passesIntercepted'),('home', 'thirdDowns'),('home', 'kickReturnYards'),('home', 'interceptions'),('home', 'turnovers'),('home', 'penaltyYards'),('home', 'fumblesLost'),('home', 'netPassingYards'),('home', 'penalties'),('home', 'interceptionYards'),('home', 'defense', 'plays', 'perPlay'),('home', 'defense', 'plays'),('home', 'defense', 'drives', 'perPlay'),('home', 'defense', 'drives'),('home', 'defense', 'ppa'),('home', 'defense', 'totalPPA', 'perPlay'),('home', 'defense', 'totalPPA'),('home', 'defense', 'successRate'),('home', 'defense', 'explosiveness'),('home', 'defense', 'powerSuccess'),('home', 'defense', 'stuffRate'),('home', 'defense', 'lineYards'),('home', 'defense', 'lineYardsTotal', 'perPlay'),('home', 'defense', 'lineYardsTotal'),('home', 'defense', 'secondLevelYards'),('home', 'defense', 'secondLevelYardsTotal', 'perPlay'),('home', 'defense', 'secondLevelYardsTotal'),('home', 'defense', 'openFieldYards'),('home', 'defense', 'openFieldYardsTotal', 'perPlay'),('home', 'defense', 'openFieldYardsTotal'),('home', 'defense', 'totalOpportunies', 'perPlay'),('home', 'defense', 'totalOpportunies'),('home', 'defense', 'pointsPerOpportunity'),('home', 'defense', 'fieldPosition', 'averageStart'),('home', 'defense', 'fieldPosition', 'averagePredictedPoints'),('home', 'defense', 'havoc', 'total'),('home', 'defense', 'havoc', 'frontSeven'),('home', 'defense', 'havoc', 'db'),('home', 'defense', 'standardDowns', 'rate'),('home', 'defense', 'standardDowns', 'ppa'),('home', 'defense', 'standardDowns', 'successRate'),('home', 'defense', 'standardDowns', 'explosiveness'),('home', 'defense', 'passingDowns', 'rate'),('home', 'defense', 'passingDowns', 'ppa'),('home', 'defense', 'passingDowns', 'totalPPA', 'perPlay'),('home', 'defense', 'passingDowns', 'totalPPA'),('home', 'defense', 'passingDowns', 'successRate'),('home', 'defense', 'passingDowns', 'explosiveness'),('home', 'defense', 'rushingPlays', 'rate'),('home', 'defense', 'rushingPlays', 'ppa'),('home', 'defense', 'rushingPlays', 'totalPPA', 'perPlay'),('home', 'defense', 'rushingPlays', 'totalPPA'),('home', 'defense', 'rushingPlays', 'successRate'),('home', 'defense', 'rushingPlays', 'explosiveness'),('home', 'defense', 'passingPlays', 'rate'),('home', 'defense', 'passingPlays', 'ppa'),('home', 'defense', 'passingPlays', 'totalPPA', 'perPlay'),('home', 'defense', 'passingPlays', 'totalPPA'),('home', 'defense', 'passingPlays', 'successRate'),('home', 'defense', 'passingPlays', 'explosiveness'),('home', 'offense', 'plays', 'perPlay'),('home', 'offense', 'plays'),('home', 'offense', 'drives', 'perPlay'),('home', 'offense', 'drives'),('home', 'offense', 'ppa'),('home', 'offense', 'totalPPA', 'perPlay'),('home', 'offense', 'totalPPA'),('home', 'offense', 'successRate'),('home', 'offense', 'explosiveness'),('home', 'offense', 'powerSuccess'),('home', 'offense', 'stuffRate'),('home', 'offense', 'lineYards'),('home', 'offense', 'lineYardsTotal', 'perPlay'),('home', 'offense', 'lineYardsTotal'),('home', 'offense', 'secondLevelYards'),('home', 'offense', 'secondLevelYardsTotal', 'perPlay'),('home', 'offense', 'secondLevelYardsTotal'),('home', 'offense', 'openFieldYards'),('home', 'offense', 'openFieldYardsTotal', 'perPlay'),('home', 'offense', 'openFieldYardsTotal'),('home', 'offense', 'totalOpportunies', 'perPlay'),('home', 'offense', 'totalOpportunies'),('home', 'offense', 'pointsPerOpportunity'),('home', 'offense', 'fieldPosition', 'averageStart'),('home', 'offense', 'fieldPosition', 'averagePredictedPoints'),('home', 'offense', 'havoc', 'total'),('home', 'offense', 'havoc', 'frontSeven'),('home', 'offense', 'havoc', 'db'),('home', 'offense', 'standardDowns', 'rate'),('home', 'offense', 'standardDowns', 'ppa'),('home', 'offense', 'standardDowns', 'successRate'),('home', 'offense', 'standardDowns', 'explosiveness'),('home', 'offense', 'passingDowns', 'rate'),('home', 'offense', 'passingDowns', 'ppa'),('home', 'offense', 'passingDowns', 'successRate'),('home', 'offense', 'passingDowns', 'explosiveness'),('home', 'offense', 'rushingPlays', 'rate'),('home', 'offense', 'rushingPlays', 'ppa'),('home', 'offense', 'rushingPlays', 'totalPPA', 'perPlay'),('home', 'offense', 'rushingPlays', 'totalPPA'),('home', 'offense', 'rushingPlays', 'successRate'),('home', 'offense', 'rushingPlays', 'explosiveness'),('home', 'offense', 'passingPlays', 'rate'),('home', 'offense', 'passingPlays', 'ppa'),('home', 'offense', 'passingPlays', 'totalPPA', 'perPlay'),('home', 'offense', 'passingPlays', 'totalPPA'),('home', 'offense', 'passingPlays', 'successRate'),('home', 'offense', 'passingPlays', 'explosiveness'),('away', 'rushingYards'),('away', 'rushingTDs'),('away', 'passAttempts'),('away', 'passingTDs'),('away', 'games'),('away', 'puntReturnTDs'),('away', 'firstDowns'),('away', 'sacks'),('away', 'interceptionTDs'),('away', 'kickReturnTDs'),('away', 'totalYards'),('away', 'fourthDownConversions'),('away', 'rushingAttempts'),('away', 'possessionTime'),('away', 'fourthDowns'),('away', 'tacklesForLoss'),('away', 'puntReturnYards'),('away', 'passCompletions'),('away', 'puntReturns'),('away', 'kickReturns'),('away', 'thirdDownConversions'),('away', 'fumblesRecovered'),('away', 'passesIntercepted'),('away', 'thirdDowns'),('away', 'kickReturnYards'),('away', 'interceptions'),('away', 'turnovers'),('away', 'penaltyYards'),('away', 'fumblesLost'),('away', 'netPassingYards'),('away', 'penalties'),('away', 'interceptionYards'),('away', 'defense', 'plays', 'perPlay'),('away', 'defense', 'plays'),('away', 'defense', 'drives', 'perPlay'),('away', 'defense', 'drives'),('away', 'defense', 'ppa'),('away', 'defense', 'totalPPA', 'perPlay'),('away', 'defense', 'totalPPA'),('away', 'defense', 'successRate'),('away', 'defense', 'explosiveness'),('away', 'defense', 'powerSuccess'),('away', 'defense', 'stuffRate'),('away', 'defense', 'lineYards'),('away', 'defense', 'lineYardsTotal', 'perPlay'),('away', 'defense', 'lineYardsTotal'),('away', 'defense', 'secondLevelYards'),('away', 'defense', 'secondLevelYardsTotal', 'perPlay'),('away', 'defense', 'secondLevelYardsTotal'),('away', 'defense', 'openFieldYards'),('away', 'defense', 'openFieldYardsTotal', 'perPlay'),('away', 'defense', 'openFieldYardsTotal'),('away', 'defense', 'totalOpportunies', 'perPlay'),('away', 'defense', 'totalOpportunies'),('away', 'defense', 'pointsPerOpportunity'),('away', 'defense', 'fieldPosition', 'averageStart'),('away', 'defense', 'fieldPosition', 'averagePredictedPoints'),('away', 'defense', 'havoc', 'total'),('away', 'defense', 'havoc', 'frontSeven'),('away', 'defense', 'havoc', 'db'),('away', 'defense', 'standardDowns', 'rate'),('away', 'defense', 'standardDowns', 'ppa'),('away', 'defense', 'standardDowns', 'successRate'),('away', 'defense', 'standardDowns', 'explosiveness'),('away', 'defense', 'passingDowns', 'rate'),('away', 'defense', 'passingDowns', 'ppa'),('away', 'defense', 'passingDowns', 'totalPPA', 'perPlay'),('away', 'defense', 'passingDowns', 'totalPPA'),('away', 'defense', 'passingDowns', 'successRate'),('away', 'defense', 'passingDowns', 'explosiveness'),('away', 'defense', 'rushingPlays', 'rate'),('away', 'defense', 'rushingPlays', 'ppa'),('away', 'defense', 'rushingPlays', 'totalPPA', 'perPlay'),('away', 'defense', 'rushingPlays', 'totalPPA'),('away', 'defense', 'rushingPlays', 'successRate'),('away', 'defense', 'rushingPlays', 'explosiveness'),('away', 'defense', 'passingPlays', 'rate'),('away', 'defense', 'passingPlays', 'ppa'),('away', 'defense', 'passingPlays', 'totalPPA', 'perPlay'),('away', 'defense', 'passingPlays', 'totalPPA'),('away', 'defense', 'passingPlays', 'successRate'),('away', 'defense', 'passingPlays', 'explosiveness'),('away', 'offense', 'plays', 'perPlay'),('away', 'offense', 'plays'),('away', 'offense', 'drives', 'perPlay'),('away', 'offense', 'drives'),('away', 'offense', 'ppa'),('away', 'offense', 'totalPPA', 'perPlay'),('away', 'offense', 'totalPPA'),('away', 'offense', 'successRate'),('away', 'offense', 'explosiveness'),('away', 'offense', 'powerSuccess'),('away', 'offense', 'stuffRate'),('away', 'offense', 'lineYards'),('away', 'offense', 'lineYardsTotal', 'perPlay'),('away', 'offense', 'lineYardsTotal'),('away', 'offense', 'secondLevelYards'),('away', 'offense', 'secondLevelYardsTotal', 'perPlay'),('away', 'offense', 'secondLevelYardsTotal'),('away', 'offense', 'openFieldYards'),('away', 'offense', 'openFieldYardsTotal', 'perPlay'),('away', 'offense', 'openFieldYardsTotal'),('away', 'offense', 'totalOpportunies', 'perPlay'),('away', 'offense', 'totalOpportunies'),('away', 'offense', 'pointsPerOpportunity'),('away', 'offense', 'fieldPosition', 'averageStart'),('away', 'offense', 'fieldPosition', 'averagePredictedPoints'),('away', 'offense', 'havoc', 'total'),('away', 'offense', 'havoc', 'frontSeven'),('away', 'offense', 'havoc', 'db'),('away', 'offense', 'standardDowns', 'rate'),('away', 'offense', 'standardDowns', 'ppa'),('away', 'offense', 'standardDowns', 'successRate'),('away', 'offense', 'standardDowns', 'explosiveness'),('away', 'offense', 'passingDowns', 'rate'),('away', 'offense', 'passingDowns', 'ppa'),('away', 'offense', 'passingDowns', 'successRate'),('away', 'offense', 'passingDowns', 'explosiveness'),('away', 'offense', 'rushingPlays', 'rate'),('away', 'offense', 'rushingPlays', 'ppa'),('away', 'offense', 'rushingPlays', 'totalPPA', 'perPlay'),('away', 'offense', 'rushingPlays', 'totalPPA'),('away', 'offense', 'rushingPlays', 'successRate'),('away', 'offense', 'rushingPlays', 'explosiveness'),('away', 'offense', 'passingPlays', 'rate'),('away', 'offense', 'passingPlays', 'ppa'),('away', 'offense', 'passingPlays', 'totalPPA', 'perPlay'),('away', 'offense', 'passingPlays', 'totalPPA'),('away', 'offense', 'passingPlays', 'successRate'),('away', 'offense', 'passingPlays', 'explosiveness')]
    
    for year in range(max_year_in_cache,max_year_in_games2+1):
        print('Adding games to cache from ', year)
        cached_games[year] = {}
        max_week = max([games2[i]['week'] for i in range(len(games2)) if games2[i]['year']==year])
        for week in range(0,max_week+1): # Possibly refilling old data, data gets updates for a few weeks after games
            cached_games[year][week] = {}
            for game in games2:
                if game['week'] == week and game['year'] == year:
                    # Some missing data checked below
#                     for stat in stat_keys:
#                         if stat not in game.keys() and stat not in do_not_impute_zeros:
#                             game[stat] = 0

                    cached_games[year][week][game['gid']] = {key:game[key] for key in game if key!='gid'} # Don't need ID data
    
    with open("CFBGameData.dat",'wb') as f:
        pickle.dump(cached_games,f)
        
    return cached_games
	
def gather_new_game_data(configuration):
    '''Takes in the configuration for the cfb data api and returns statistics organized by college football game for use
    in predictions.'''

    if os.path.isfile('CFBGameData.dat'):
        with open("CFBGameData.dat",'rb') as f:
            cached_games = pickle.load(f) # Multi-level dictionary. 
                                        #Outer level is year of game, then week, then game id keying a dictionary of game data
    else:
        cached_games = {2013:{0:{}}}

    print(cached_games.keys())
    current_year = datetime.now().year
    max_year_in_cache = max(cached_games.keys())
    max_week_in_cache = max(cached_games[max_year_in_cache].keys())

    lines = []
    games = []
    year = max(max_year_in_cache, current_year)

    if year == max_year_in_cache:
        week = max_week_in_cache + 1
    else:
        week = 1
        
    print('Gathering games')
    response = games_api.get_games(year=year, week=week)
    games = [*games, *response]

    response = betting_api.get_lines(year=year, week=week)
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
        ) for g in games if g.home_points is None and g.away_points is None]

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
    
    stat_keys = ['year', 'week', 'neutral_site', 'home_team', 'home_conference', 'home_points', 'home_elo', 'away_team', 'away_conference', 'away_points', 'away_elo', 'margin', 'spread', ('home', 'rushingYards'), ('home', 'rushingTDs'), ('home', 'passAttempts'), ('home', 'passingTDs'), ('home', 'games'), ('home', 'puntReturnTDs'), ('home', 'firstDowns'), ('home', 'sacks'), ('home', 'interceptionTDs'), ('home', 'kickReturnTDs'), ('home', 'totalYards'), ('home', 'fourthDownConversions'),('home', 'rushingAttempts'),('home', 'possessionTime'),('home', 'fourthDowns'),('home', 'tacklesForLoss'),('home', 'puntReturnYards'),('home', 'passCompletions'),('home', 'puntReturns'),('home', 'kickReturns'),('home', 'thirdDownConversions'),('home', 'fumblesRecovered'),('home', 'passesIntercepted'),('home', 'thirdDowns'),('home', 'kickReturnYards'),('home', 'interceptions'),('home', 'turnovers'),('home', 'penaltyYards'),('home', 'fumblesLost'),('home', 'netPassingYards'),('home', 'penalties'),('home', 'interceptionYards'),('home', 'defense', 'plays', 'perPlay'),('home', 'defense', 'plays'),('home', 'defense', 'drives', 'perPlay'),('home', 'defense', 'drives'),('home', 'defense', 'ppa'),('home', 'defense', 'totalPPA', 'perPlay'),('home', 'defense', 'totalPPA'),('home', 'defense', 'successRate'),('home', 'defense', 'explosiveness'),('home', 'defense', 'powerSuccess'),('home', 'defense', 'stuffRate'),('home', 'defense', 'lineYards'),('home', 'defense', 'lineYardsTotal', 'perPlay'),('home', 'defense', 'lineYardsTotal'),('home', 'defense', 'secondLevelYards'),('home', 'defense', 'secondLevelYardsTotal', 'perPlay'),('home', 'defense', 'secondLevelYardsTotal'),('home', 'defense', 'openFieldYards'),('home', 'defense', 'openFieldYardsTotal', 'perPlay'),('home', 'defense', 'openFieldYardsTotal'),('home', 'defense', 'totalOpportunies', 'perPlay'),('home', 'defense', 'totalOpportunies'),('home', 'defense', 'pointsPerOpportunity'),('home', 'defense', 'fieldPosition', 'averageStart'),('home', 'defense', 'fieldPosition', 'averagePredictedPoints'),('home', 'defense', 'havoc', 'total'),('home', 'defense', 'havoc', 'frontSeven'),('home', 'defense', 'havoc', 'db'),('home', 'defense', 'standardDowns', 'rate'),('home', 'defense', 'standardDowns', 'ppa'),('home', 'defense', 'standardDowns', 'successRate'),('home', 'defense', 'standardDowns', 'explosiveness'),('home', 'defense', 'passingDowns', 'rate'),('home', 'defense', 'passingDowns', 'ppa'),('home', 'defense', 'passingDowns', 'totalPPA', 'perPlay'),('home', 'defense', 'passingDowns', 'totalPPA'),('home', 'defense', 'passingDowns', 'successRate'),('home', 'defense', 'passingDowns', 'explosiveness'),('home', 'defense', 'rushingPlays', 'rate'),('home', 'defense', 'rushingPlays', 'ppa'),('home', 'defense', 'rushingPlays', 'totalPPA', 'perPlay'),('home', 'defense', 'rushingPlays', 'totalPPA'),('home', 'defense', 'rushingPlays', 'successRate'),('home', 'defense', 'rushingPlays', 'explosiveness'),('home', 'defense', 'passingPlays', 'rate'),('home', 'defense', 'passingPlays', 'ppa'),('home', 'defense', 'passingPlays', 'totalPPA', 'perPlay'),('home', 'defense', 'passingPlays', 'totalPPA'),('home', 'defense', 'passingPlays', 'successRate'),('home', 'defense', 'passingPlays', 'explosiveness'),('home', 'offense', 'plays', 'perPlay'),('home', 'offense', 'plays'),('home', 'offense', 'drives', 'perPlay'),('home', 'offense', 'drives'),('home', 'offense', 'ppa'),('home', 'offense', 'totalPPA', 'perPlay'),('home', 'offense', 'totalPPA'),('home', 'offense', 'successRate'),('home', 'offense', 'explosiveness'),('home', 'offense', 'powerSuccess'),('home', 'offense', 'stuffRate'),('home', 'offense', 'lineYards'),('home', 'offense', 'lineYardsTotal', 'perPlay'),('home', 'offense', 'lineYardsTotal'),('home', 'offense', 'secondLevelYards'),('home', 'offense', 'secondLevelYardsTotal', 'perPlay'),('home', 'offense', 'secondLevelYardsTotal'),('home', 'offense', 'openFieldYards'),('home', 'offense', 'openFieldYardsTotal', 'perPlay'),('home', 'offense', 'openFieldYardsTotal'),('home', 'offense', 'totalOpportunies', 'perPlay'),('home', 'offense', 'totalOpportunies'),('home', 'offense', 'pointsPerOpportunity'),('home', 'offense', 'fieldPosition', 'averageStart'),('home', 'offense', 'fieldPosition', 'averagePredictedPoints'),('home', 'offense', 'havoc', 'total'),('home', 'offense', 'havoc', 'frontSeven'),('home', 'offense', 'havoc', 'db'),('home', 'offense', 'standardDowns', 'rate'),('home', 'offense', 'standardDowns', 'ppa'),('home', 'offense', 'standardDowns', 'successRate'),('home', 'offense', 'standardDowns', 'explosiveness'),('home', 'offense', 'passingDowns', 'rate'),('home', 'offense', 'passingDowns', 'ppa'),('home', 'offense', 'passingDowns', 'successRate'),('home', 'offense', 'passingDowns', 'explosiveness'),('home', 'offense', 'rushingPlays', 'rate'),('home', 'offense', 'rushingPlays', 'ppa'),('home', 'offense', 'rushingPlays', 'totalPPA', 'perPlay'),('home', 'offense', 'rushingPlays', 'totalPPA'),('home', 'offense', 'rushingPlays', 'successRate'),('home', 'offense', 'rushingPlays', 'explosiveness'),('home', 'offense', 'passingPlays', 'rate'),('home', 'offense', 'passingPlays', 'ppa'),('home', 'offense', 'passingPlays', 'totalPPA', 'perPlay'),('home', 'offense', 'passingPlays', 'totalPPA'),('home', 'offense', 'passingPlays', 'successRate'),('home', 'offense', 'passingPlays', 'explosiveness'),('away', 'rushingYards'),('away', 'rushingTDs'),('away', 'passAttempts'),('away', 'passingTDs'),('away', 'games'),('away', 'puntReturnTDs'),('away', 'firstDowns'),('away', 'sacks'),('away', 'interceptionTDs'),('away', 'kickReturnTDs'),('away', 'totalYards'),('away', 'fourthDownConversions'),('away', 'rushingAttempts'),('away', 'possessionTime'),('away', 'fourthDowns'),('away', 'tacklesForLoss'),('away', 'puntReturnYards'),('away', 'passCompletions'),('away', 'puntReturns'),('away', 'kickReturns'),('away', 'thirdDownConversions'),('away', 'fumblesRecovered'),('away', 'passesIntercepted'),('away', 'thirdDowns'),('away', 'kickReturnYards'),('away', 'interceptions'),('away', 'turnovers'),('away', 'penaltyYards'),('away', 'fumblesLost'),('away', 'netPassingYards'),('away', 'penalties'),('away', 'interceptionYards'),('away', 'defense', 'plays', 'perPlay'),('away', 'defense', 'plays'),('away', 'defense', 'drives', 'perPlay'),('away', 'defense', 'drives'),('away', 'defense', 'ppa'),('away', 'defense', 'totalPPA', 'perPlay'),('away', 'defense', 'totalPPA'),('away', 'defense', 'successRate'),('away', 'defense', 'explosiveness'),('away', 'defense', 'powerSuccess'),('away', 'defense', 'stuffRate'),('away', 'defense', 'lineYards'),('away', 'defense', 'lineYardsTotal', 'perPlay'),('away', 'defense', 'lineYardsTotal'),('away', 'defense', 'secondLevelYards'),('away', 'defense', 'secondLevelYardsTotal', 'perPlay'),('away', 'defense', 'secondLevelYardsTotal'),('away', 'defense', 'openFieldYards'),('away', 'defense', 'openFieldYardsTotal', 'perPlay'),('away', 'defense', 'openFieldYardsTotal'),('away', 'defense', 'totalOpportunies', 'perPlay'),('away', 'defense', 'totalOpportunies'),('away', 'defense', 'pointsPerOpportunity'),('away', 'defense', 'fieldPosition', 'averageStart'),('away', 'defense', 'fieldPosition', 'averagePredictedPoints'),('away', 'defense', 'havoc', 'total'),('away', 'defense', 'havoc', 'frontSeven'),('away', 'defense', 'havoc', 'db'),('away', 'defense', 'standardDowns', 'rate'),('away', 'defense', 'standardDowns', 'ppa'),('away', 'defense', 'standardDowns', 'successRate'),('away', 'defense', 'standardDowns', 'explosiveness'),('away', 'defense', 'passingDowns', 'rate'),('away', 'defense', 'passingDowns', 'ppa'),('away', 'defense', 'passingDowns', 'totalPPA', 'perPlay'),('away', 'defense', 'passingDowns', 'totalPPA'),('away', 'defense', 'passingDowns', 'successRate'),('away', 'defense', 'passingDowns', 'explosiveness'),('away', 'defense', 'rushingPlays', 'rate'),('away', 'defense', 'rushingPlays', 'ppa'),('away', 'defense', 'rushingPlays', 'totalPPA', 'perPlay'),('away', 'defense', 'rushingPlays', 'totalPPA'),('away', 'defense', 'rushingPlays', 'successRate'),('away', 'defense', 'rushingPlays', 'explosiveness'),('away', 'defense', 'passingPlays', 'rate'),('away', 'defense', 'passingPlays', 'ppa'),('away', 'defense', 'passingPlays', 'totalPPA', 'perPlay'),('away', 'defense', 'passingPlays', 'totalPPA'),('away', 'defense', 'passingPlays', 'successRate'),('away', 'defense', 'passingPlays', 'explosiveness'),('away', 'offense', 'plays', 'perPlay'),('away', 'offense', 'plays'),('away', 'offense', 'drives', 'perPlay'),('away', 'offense', 'drives'),('away', 'offense', 'ppa'),('away', 'offense', 'totalPPA', 'perPlay'),('away', 'offense', 'totalPPA'),('away', 'offense', 'successRate'),('away', 'offense', 'explosiveness'),('away', 'offense', 'powerSuccess'),('away', 'offense', 'stuffRate'),('away', 'offense', 'lineYards'),('away', 'offense', 'lineYardsTotal', 'perPlay'),('away', 'offense', 'lineYardsTotal'),('away', 'offense', 'secondLevelYards'),('away', 'offense', 'secondLevelYardsTotal', 'perPlay'),('away', 'offense', 'secondLevelYardsTotal'),('away', 'offense', 'openFieldYards'),('away', 'offense', 'openFieldYardsTotal', 'perPlay'),('away', 'offense', 'openFieldYardsTotal'),('away', 'offense', 'totalOpportunies', 'perPlay'),('away', 'offense', 'totalOpportunies'),('away', 'offense', 'pointsPerOpportunity'),('away', 'offense', 'fieldPosition', 'averageStart'),('away', 'offense', 'fieldPosition', 'averagePredictedPoints'),('away', 'offense', 'havoc', 'total'),('away', 'offense', 'havoc', 'frontSeven'),('away', 'offense', 'havoc', 'db'),('away', 'offense', 'standardDowns', 'rate'),('away', 'offense', 'standardDowns', 'ppa'),('away', 'offense', 'standardDowns', 'successRate'),('away', 'offense', 'standardDowns', 'explosiveness'),('away', 'offense', 'passingDowns', 'rate'),('away', 'offense', 'passingDowns', 'ppa'),('away', 'offense', 'passingDowns', 'successRate'),('away', 'offense', 'passingDowns', 'explosiveness'),('away', 'offense', 'rushingPlays', 'rate'),('away', 'offense', 'rushingPlays', 'ppa'),('away', 'offense', 'rushingPlays', 'totalPPA', 'perPlay'),('away', 'offense', 'rushingPlays', 'totalPPA'),('away', 'offense', 'rushingPlays', 'successRate'),('away', 'offense', 'rushingPlays', 'explosiveness'),('away', 'offense', 'passingPlays', 'rate'),('away', 'offense', 'passingPlays', 'ppa'),('away', 'offense', 'passingPlays', 'totalPPA', 'perPlay'),('away', 'offense', 'passingPlays', 'totalPPA'),('away', 'offense', 'passingPlays', 'successRate'),('away', 'offense', 'passingPlays', 'explosiveness')]
    
    print('Adding games to output dictionary.')
    new_games = []
    for game in games2:
        new_games.append({key:game[key] for key in game if key!='gid'}) # Don't need ID data
        
    return new_games
	
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
        
        old_week, old_year = curr_week_year
        if old_year != year and year > 2013 and old_year > 0:
            old_season_stats = season_stats
            old_advanced_stats = advanced_stats
        elif old_year == 0 and year > 2013:
            old_season_stats = [] # This will be all stats from the season endpoint
            old_advanced_stats = [] # This will be all stats from the season/advanced endpoint
            endpoint = "/stats/season"
            params = {
                        "year": year-1,
                        "excludeGarbageTime" : True
                    }
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)

            # Check if the request was successful (status code 200)
            if response.status_code != 200:
                print('AHHHH', response)
            old_season_stats = response.json()
            endpoint = "/stats/season/advanced"
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
            old_advanced_stats = response.json()
        
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
                endpoint = "/stats/season/advanced"
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
                advanced_stats = response.json()
                
                endpoint = "/stats/season"
                params = {
                            "year": year,
                            "startWeek": max(week-3,1),
                            "endWeek" : week - 1,
                            "excludeGarbageTime" : True
                        }
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code != 200:
                    print('AHHHH', response)
                season_stats_last_3 = response.json()
                endpoint = "/stats/season/advanced"
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
                advanced_stats_last_3 = response.json()
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
                season_stats_last_3 = response.json()
                endpoint = "/stats/season/advanced"
                response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers)
                advanced_stats = response.json()
                advanced_stats_last_3 = response.json()
        

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
                
                for stat_idx, stat in enumerate(team_stats):
                    if stat['statName'] == 'games':
                        num_games = stat['statValue']
                        
                for stat in team_stats:
                    if stat['statName'] == 'games':
                        stat_dict[location+'_'+stat['statName']] = stat['statValue']
                    else:
                        stat_dict[location+'_'+stat['statName']] = stat['statValue'] / num_games
                        
                if len(team_advanced_stats) > 0:
                    defense_stats = team_advanced_stats[0]['defense']
                    defense_drives = defense_stats['drives']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    if defense_stats[name][name2] is not None: 
                                        stat_per_drive = defense_stats[name][name2] / defense_drives
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'perDrive'] = stat_per_drive
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2] = defense_stats[name][name2] / num_games
                                    else:
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'perDrive'] = None
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2] = None
                                else:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+name2] = defense_stats[name][name2]
                        else:
                            if ('defense',name) in total_stats:
                                if defense_stats[name] is not None:
                                    stat_per_drive = defense_stats[name] / defense_drives
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'perDrive'] = stat_per_drive
                                    stat_dict[location+'_'+'defense'+'_'+name] = defense_stats[name] / num_games
                                else:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'perDrive'] = None
                                    stat_dict[location+'_'+'defense'+'_'+name] = None
                            else:
                                stat_dict[location+'_'+'defense'+'_'+name] = defense_stats[name]
                    offense_stats = team_advanced_stats[0]['offense']
                    offense_drives = offense_stats['drives']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    if offense_stats[name][name2] is not None:
                                        stat_per_drive = offense_stats[name][name2] / offense_drives
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'perDrive'] = stat_per_drive
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2] = offense_stats[name][name2] / num_games
                                    else:
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'perDrive'] = None
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2] = None
                                else:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+name2] = offense_stats[name][name2]
                        else:
                            if ('offense',name) in total_stats:
                                if offense_stats[name] is not None:
                                    stat_per_drive = offense_stats[name] / offense_drives
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'perDrive'] = stat_per_drive
                                    stat_dict[location+'_'+'offense'+'_'+name] = offense_stats[name] / num_games
                                else:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'perDrive'] = None
                                    stat_dict[location+'_'+'offense'+'_'+name] = None
                            else:
                                stat_dict[location+'_'+'offense'+'_'+name] = offense_stats[name]
        else:
            stat_dict = {}
            for location in ['home','away']:
                for stat in season_stats:
                    stat_dict[location+'_'+stat['statName']] = 0
                if len(advanced_stats) > 0:
                    defense_stats = advanced_stats[0]['defense']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'perDrive'] = 0
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+name2] = 0
                        else:
                            if ('defense',name) in total_stats:
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+'perDrive'] = 0
                            stat_dict[location+'_'+'defense'+'_'+name] = 0
                    offense_stats = advanced_stats[0]['offense']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'perDrive'] = 0
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+name2] = 0
                        else:
                            if ('offense',name) in total_stats:
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+'perDrive'] = 0
                            stat_dict[location+'_'+'offense'+'_'+name] = 0
                            
        # ------------------------------------Last season stats.--------------------------------------------
        

        if year > 2013:
            for location in ['home','away']:

                if location == 'home':
                    idx_team1 = [i for i in range(len(old_season_stats)) if old_season_stats[i]['team'] == home_team]
                    idx_team2 = [i for i in range(len(old_advanced_stats)) if old_advanced_stats[i]['team'] == home_team]
                else:
                    idx_team1 = [i for i in range(len(old_season_stats)) if old_season_stats[i]['team'] == away_team]
                    idx_team2 = [i for i in range(len(old_advanced_stats)) if old_advanced_stats[i]['team'] == away_team]

                if len(idx_team1) > 0:
                    team_stats = np.array(old_season_stats)[idx_team1]
                else:
                    team_stats = []
                if len(idx_team2) > 0:
                    team_advanced_stats = [old_advanced_stats[idx_team2[0]]]
                else:
                    team_advanced_stats = []
                    
                # Below block normalizes team_stats statistics by games played
                
                for stat_idx, stat in enumerate(team_stats):
                    if stat['statName'] == 'games':
                        num_games = stat['statValue']
                        
                for stat in team_stats:
                    if stat['statName'] == 'games':
                        stat_dict[location+'_'+stat['statName']+'_'+'lastSeason'] = stat['statValue']
                    else:
                        stat_dict[location+'_'+stat['statName']+'_'+'lastSeason'] = stat['statValue'] / num_games
                if len(team_advanced_stats) > 0:
                    defense_stats = team_advanced_stats[0]['defense']
                    defense_drives = defense_stats['drives']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    if defense_stats[name][name2] is not None: 
                                        stat_per_drive = defense_stats[name][name2] / defense_drives
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'+'_'+'perDrive'] = stat_per_drive
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'] = defense_stats[name][name2] / num_games
                                    else:
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'+'_'+'perDrive'] = None
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'] = None
                                else:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'] = defense_stats[name][name2]
                        else:
                            if ('defense',name) in total_stats:
                                if defense_stats[name] is not None:
                                    stat_per_drive = defense_stats[name] / defense_drives
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'+'_'+'perDrive'] = stat_per_drive
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'] = defense_stats[name] / num_games
                                else:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'+'_'+'perDrive'] = None
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'] = None
                            else:
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'] = defense_stats[name]
                    offense_stats = team_advanced_stats[0]['offense']
                    offense_drives = offense_stats['drives']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    if offense_stats[name][name2] is not None:
                                        stat_per_drive = offense_stats[name][name2] / offense_drives
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'+'_'+'perDrive'] = stat_per_drive
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'] = offense_stats[name][name2] / num_games
                                    else:
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'+'_'+'perDrive'] = None
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'] = None
                                else:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'] = offense_stats[name][name2]
                        else:
                            if ('offense',name) in total_stats:
                                if offense_stats[name] is not None:
                                    stat_per_drive = offense_stats[name] / offense_drives
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'+'_'+'perDrive'] = stat_per_drive
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'] = offense_stats[name] / num_games
                                else:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'+'_'+'perDrive'] = None
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'] = None
                            else:
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'] = offense_stats[name]
        else:
            for location in ['home','away']:
                for stat in season_stats:
                    stat_dict[location+'_'+stat['statName']+'_'+'lastSeason'] = 0
                if len(advanced_stats) > 0:
                    defense_stats = advanced_stats[0]['defense']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'+'_'+'perDrive'] = 0
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastSeason'] = 0
                        else:
                            if ('defense',name) in total_stats:
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'+'_'+'perDrive'] = 0
                            stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastSeason'] = 0
                    offense_stats = advanced_stats[0]['offense']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'+'_'+'perDrive'] = 0
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastSeason'] = 0
                        else:
                            if ('offense',name) in total_stats:
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'+'_'+'perDrive'] = 0
                            stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastSeason'] = 0
                            
        #----------------------------------------Last 3 weeks stats------------------------------------------------
        
        if week > 1:
            for location in ['home','away']:

                if location == 'home':
                    idx_team1 = [i for i in range(len(season_stats_last_3)) if season_stats_last_3[i]['team'] == home_team]
                    idx_team2 = [i for i in range(len(advanced_stats_last_3)) if advanced_stats_last_3[i]['team'] == home_team]
                else:
                    idx_team1 = [i for i in range(len(season_stats_last_3)) if season_stats_last_3[i]['team'] == away_team]
                    idx_team2 = [i for i in range(len(advanced_stats_last_3)) if advanced_stats_last_3[i]['team'] == away_team]

                if len(idx_team1) > 0:
                    team_stats = np.array(season_stats_last_3)[idx_team1]
                else:
                    team_stats = []
                if len(idx_team2) > 0:
                    team_advanced_stats = [advanced_stats_last_3[idx_team2[0]]]
                else:
                    team_advanced_stats = []
                    
                # Below block normalizes team_stats statistics by games played
                
                for stat_idx, stat in enumerate(team_stats):
                    if stat['statName'] == 'games':
                        num_games = stat['statValue']
                        
                for stat in team_stats:
                    if stat['statName'] == 'games':
                        stat_dict[location+'_'+stat['statName']+'_'+'lastThree'] = stat['statValue']
                    else:
                        stat_dict[location+'_'+stat['statName']+'_'+'lastThree'] = stat['statValue'] / num_games
                if len(team_advanced_stats) > 0:
                    defense_stats = team_advanced_stats[0]['defense']
                    defense_drives = defense_stats['drives']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    if defense_stats[name][name2] is not None: 
                                        stat_per_drive = defense_stats[name][name2] / defense_drives
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'+'_'+'perDrive'] = stat_per_drive
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'] = defense_stats[name][name2] / num_games
                                    else:
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'+'_'+'perDrive'] = None
                                        stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'] = None
                                else:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'] = defense_stats[name][name2]
                        else:
                            if ('defense',name) in total_stats:
                                if defense_stats[name] is not None:
                                    stat_per_drive = defense_stats[name] / defense_drives
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'+'_'+'perDrive'] = stat_per_drive
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'] = defense_stats[name] / num_games
                                else:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'+'_'+'perDrive'] = None
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'] = None
                            else:
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'] = defense_stats[name]
                    offense_stats = team_advanced_stats[0]['offense']
                    offense_drives = offense_stats['drives']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    if offense_stats[name][name2] is not None:
                                        stat_per_drive = offense_stats[name][name2] / offense_drives
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'+'_'+'perDrive'] = stat_per_drive
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'] = offense_stats[name][name2] / num_games
                                    else:
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'+'_'+'perDrive'] = None
                                        stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'] = None
                                else:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'] = offense_stats[name][name2]
                        else:
                            if ('offense',name) in total_stats:
                                if offense_stats[name] is not None:
                                    stat_per_drive = offense_stats[name] / offense_drives
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'+'_'+'perDrive'] = stat_per_drive
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'] = offense_stats[name] / num_games
                                else:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'+'_'+'perDrive'] = None
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'] = None
                            else:
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'] = offense_stats[name]
        else:
            for location in ['home','away']:
                for stat in season_stats:
                    stat_dict[location+'_'+stat['statName']+'_'+'lastThree'] = 0
                if len(advanced_stats) > 0:
                    defense_stats = advanced_stats[0]['defense']
                    for name in defense_stats.keys():
                        if type(defense_stats[name]) == type({}):
                            for name2 in defense_stats[name].keys():
                                if ('defense',name,name2) in total_stats:
                                    stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'+'_'+'perDrive'] = 0
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+name2+'_'+'lastThree'] = 0
                        else:
                            if ('defense',name) in total_stats:
                                stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'+'_'+'perDrive'] = 0
                            stat_dict[location+'_'+'defense'+'_'+name+'_'+'lastThree'] = 0
                    offense_stats = advanced_stats[0]['offense']
                    for name in offense_stats.keys():
                        if type(offense_stats[name]) == type({}):
                            for name2 in offense_stats[name].keys():
                                if ('offense',name,name2) in total_stats:
                                    stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'+'_'+'perDrive'] = 0
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+name2+'_'+'lastThree'] = 0
                        else:
                            if ('offense',name) in total_stats:
                                stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'+'_'+'perDrive'] = 0
                            stat_dict[location+'_'+'offense'+'_'+name+'_'+'lastThree'] = 0

        for stat in stat_dict.keys():
            game[stat] = stat_dict[stat]
        count += 1
        
    return games