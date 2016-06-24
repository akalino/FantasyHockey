"""
Project Name: WebScrapers 
@author: alkal 
Created on: 6/4/16 at  1:07 PM 
"""

import FanAPI
import pandas as pd
import ast

var = FanAPI.CreateConn('/home/alkal/Documents/FantasyHockey/WebScrapers/API/auth_key.csv')
stat_blob = var.execute_query('game/nhl/stat_categories?format=json')
blob_stat = ast.literal_eval(stat_blob)
blob_stat = blob_stat['fantasy_content']['game'][1]['stat_categories']['stats']
stats_list = []
for stat in blob_stat:
    a = {}
    a['id'] = stat['stat']['stat_id']
    a['short_name'] = stat['stat']['display_name']
    a['full_name'] = stat['stat']['name']

    if 'position_types' in stat['stat']:
        for pos in stat['stat']['position_types']:
            if pos['position_type'] == 'P':
                a['player_stat'] = 1
            else:
                a['player_stat'] = 0
            if pos['position_type'] == 'G':
                a['goalie_stat'] = 1
            else:
                a['goalie_stat'] = 0
    else:
        a['player_stat'] = 0
        a['goalie_stat'] = 0

    if 'is_composite_stat' in stat['stat']:
        a['composite'] = 1
    else:
        a['composite'] = 0

    stats_list.append(a)

stat_categories = pd.DataFrame(stats_list)
print stat_categories


pos_blob = var.execute_query('game/nhl/roster_positions?format=json')
blob_pos = ast.literal_eval(pos_blob)
blob_pos = blob_pos['fantasy_content']['game'][1]['roster_positions']
pos_list = []
for pos in blob_pos:
    pos = pos['roster_position']
    b = {}
    b['position'] = pos['position']
    b['long_name'] = pos['display_name']
    if 'position_type' in pos:
        b['position_type'] = pos['position_type']
    else:
        b['position_type'] = 'NA'

    pos_list.append(b)

position_df = pd.DataFrame(pos_list)
print position_df

weeks_blob = var.execute_query('game/nhl/game_weeks?format=json')
blob_week = ast.literal_eval(weeks_blob)
blob_week = blob_week['fantasy_content']['game'][1]['game_weeks']
key_list = blob_week.keys()
sort_key_list = []
# Do this to remove the 'count' string
for x in sorted(key_list):
    sort_key_list.append(x)
skl = sort_key_list[:-1]
week_list = []
for k in skl:
    weeks = {}
    weeks['idx'] = k
    cur_week = blob_week[k]
    cw = cur_week['game_week']
    weeks['week'] = cw['week']
    weeks['start'] = cw['start']
    weeks['end'] = cw['end']

    week_list.append(weeks)

week_df = pd.DataFrame(week_list)
print week_df
