from update_game_data import gather_game_data, gather_new_game_data
from __future__ import print_function
import cfbd
from cfbd.rest import ApiException
from pprint import pprint
from datetime import datetime
import os.path
import pickle
import requests
import numpy as np

# Configure API key authorization: ApiKeyAuth
configuration = cfbd.Configuration()
configuration.api_key['Authorization'] = '' # Redacted
configuration.api_key_prefix['Authorization'] = 'Bearer'
api_config = cfbd.ApiClient(configuration)
headers = {'Authorization': configuration.api_key_prefix['Authorization'] + ' ' + configuration.api_key['Authorization']} 

# Some useful shortcuts
teams_api = cfbd.TeamsApi(api_config)
ratings_api = cfbd.RatingsApi(api_config)
games_api = cfbd.GamesApi(api_config)
stats_api = cfbd.StatsApi(api_config)
betting_api = cfbd.BettingApi(api_config)

with open('neural_net_for_spread_cfb.dat','rb') as f:
    learn = pickle.load(f)
  
games = gather_game_data(configuration)

games_flat = []
for year in games.keys():
    for week in games[year].keys():
        games_flat.extend([games[year][week][gid] for gid in games[year][week].keys() if ('home_elo' in games[year][week][gid].keys()) and ('away_elo' in games[year][week][gid].keys()) and games[year][week][gid]['home_elo'] is not None and games[year][week][gid]['away_elo'] is not None])

df = pd.DataFrame.from_records(games_flat).dropna()

games_to_predict = gather_new_game_data(configuration)

df_pred = pd.DataFrame.from_records(games_to_predict).dropna()

df_pred_z_scaled = df_pred.copy()
excluded = ['spread','gid','year','home_team','away_team', 'home_points','margin', 'away_points','home_wins','home_interceptions','away_interceptions','home_interceptionYards','away_interceptionYards','home_fumblesLost','away_fumblesLost','home_fumblesRecovered','away_fumblesRecovered','home_interceptions_lastSeason','away_interceptions_lastSeason','home_interceptionYards_lastSeason','away_interceptionYards_lastSeason','home_fumblesLost_lastSeason','away_fumblesLost_lastSeason','home_fumblesRecovered_lastSeason','away_fumblesRecovered_lastSeason','home_interceptions_lastThree','away_interceptions_lastThree','home_interceptionYards_lastThree','away_interceptionYards_lastThree','home_fumblesLost_lastThree','away_fumblesLost_lastThree','home_fumblesRecovered_lastThree','away_fumblesRecovered_lastThree']
# spread currently excluded
cat_features = ['home_conference','away_conference','neutral_site']


for column in df_pred_z_scaled.columns:
    if column not in excluded and column not in cat_features:
        df_pred_z_scaled[column] = (df_pred_z_scaled[column] - df[column].mean())/df[column].std()
        
pdf = df_pred_z_scaled.copy()
dl = learn.dls.test_dl(pdf)
pdf['predicted'] = learn.get_preds(dl=dl)[0].numpy()

results = [(pdf['home_team'][i],pdf['away_team'][i],pdf['predicted'][i],pdf['spread'][i]) for i in range(len(pdf['spread'])) if (not math.isnan(pdf['real_predicted'][i])) and np.isclose(pdf['week'][i],weekTransformed,atol=.05)]
countHome = 0
countAway = 0
count = 0
for result in results:
    count += 1
    hometeam,awayteam,prediction,spread = result
    if prediction < 0:
        print(hometeam +' favored over ' +awayteam+ ' by ' + str(round(-1.*prediction,2)) + ' points.')
    elif prediction >= 0:
        print(awayteam +' favored over ' +hometeam+ ' by ' + str(round(prediction,2)) + ' points.')
    if prediction <= spread:
        countHome += 1
    elif prediction > spread:
        countAway += 1
        
print(count)
print(countAway/count,countHome/count)

for result in results:
    hometeam,awayteam,prediction,spread = result
    if prediction < 0:
        print(hometeam +' favored over ' +awayteam+ ' by ' + str(round(-1.*prediction,2)) + ' points.')
    elif prediction >= 0:
        print(awayteam +' favored over ' +hometeam+ ' by ' + str(round(prediction,2)) + ' points.')
