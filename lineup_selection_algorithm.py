from pulp import *
import pandas as pd
import pyodbc
from pprint import pprint
import time

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_colwidth', -1)
pd.set_option('expand_frame_repr', True)
pd.set_option('display.width',None)

class InfiniteLoopException(Exception):
    def __init__(self, message, errors):
        super().__init__(message)
        
def get_sql_connection(connection_string):
    try:
        cnxn = pyodbc.connect(connection_string)
    except Exception as e:
        print(e)
        return None
    return cnxn
            
def get_db_connection_items(file):
    with open(file,'r') as fo:
        lines = fo.readlines()
        server = lines[0].split('=')[1].replace('\n','')
        database = lines[1].split('=')[1].replace('\n','')
        uid = lines[2].split('=')[1].replace('\n','')
        pwd = lines[3].split('=')[1]
    return server, database, uid, pwd

def get_player_set(year, week, db_conn):
    sql_string = f"exec sp_DK_Player_Set @week = {week}, @year = {year}"
    return pd.read_sql_query(sql_string, db_conn)

#want to have an exclude list, but should remove from df before this function
def optimal_lineup(player_df, salary_con = 50000, qb_con = 1,
                   def_con = 1, rb_max_con = 3, rb_min_con = 2,
                   wr_max_con = 4, wr_min_con = 3, te_max_con = 2,
                   te_min_con = 1, total_players_con = 9, qb_id = None,
                   stack_num = 0, one_team_con = False, two_team_con = False,
                   lineup_num = 0, exclude_list = [], locked_players = [],
                   maximum_score = None):
    players = [row['Player_ID'] for index, row in player_df.iterrows()
               if row['Player_ID']]
    positions = get_column_dict(player_df,'Position')
    salaries = get_column_dict(player_df,'DK_Salary')
    x_points = get_column_dict(player_df,'xDK_Points')
    team = get_column_dict(player_df,'Team_Abbr')
    player_name = get_column_dict(player_df,'Player')
    real_points = get_column_dict(player_df,'rDK_Points')
    dk_ids = get_column_dict(player_df,'DK_ID')
    
    prob = LpProblem('Optimal Lineup',LpMaximize)

    player_vars = LpVariable.dicts('player',players,lowBound=0,upBound=1,
                                   cat='Integer')

    prob += lpSum([x_points[i]*player_vars[i]
                   for i in players]), 'Lineup Points'

    if qb_id is not None:
        salary_con -= salaries[qb_id]
        qb_con = 0
        total_players_con -= 1
        qb_team = team[qb_id]
        if stack_num > 0:
            prob += lpSum([player_vars[player]
                           for player in players
                           if team[player] == qb_team
                           and positions[player] != 'DST']) == stack_num
    
    prob += lpSum([salaries[player]*player_vars[player]
                   for player in players]) <= salary_con,\
                   'Salary Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'QB']) == qb_con,\
                   'QB Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'DST']) == def_con,\
                   'DEF Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'RB']) <= rb_max_con,\
                   'RB Max Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'RB']) >= rb_min_con,\
                   'RB Min Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'WR']) <= wr_max_con,\
                   'WR Max Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'WR']) >= wr_min_con,\
                   'WR Min Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'TE']) <= te_max_con,\
                   'TE Max Constraint'
    prob += lpSum([player_vars[player]
                   for player in players
                   if positions[player] == 'TE']) >= te_min_con,\
                   'TE Min Constraint'
    prob += lpSum([player_vars[player]
                   for player in players]) == total_players_con,\
                   'Full Lineup Constraint'


###This is hacked together early in the morning, may need to refactor
    if one_team_con == True:
        #need constraint that each team is only mapped to x players
        #ex: 1 player per team to minimize variance or more to max variance
        team_con_dict = get_team_con_dict(team)
        for team_key in team_con_dict:
            prob += lpSum([player_vars[player]
                          for player in players
                          if player in team_con_dict[team_key]]) <= 1,\
                          f'Max One Player From {team_key} Constraint'
    if two_team_con == True:
        #need constraint that each team is only mapped to x players
        #ex: 1 player per team to minimize variance or more to max variance
        team_con_dict = get_team_con_dict(team)
        for team_key in team_con_dict:
            prob += lpSum([player_vars[player]
                          for player in players
                          if player in team_con_dict[team_key]]) <= 2,\
                          f'Max Two Players From {team_key} Constraint'

    if len(locked_players) > 0:
        for locked_player in locked_players:
            prob += lpSum([player_vars[player]
                           for player in players
                           if player == locked_player]) == 1, \
                           f'Must include {player_name[locked_player]}'+\
                           'Constraint'

    if maximum_score is not None:
        prob += lpSum([x_points[player]*player_vars[player]
                       for player in players]) <= maximum_score - 0.01, \
                       'Multiple lineup constraint'

    prob.writeLP('LineupOptimizationModel.lp')
    prob.solve()

    players = []
    real_score = 0

    qb_score = 0
    if qb_id is not None:
        players.append({'player_name':player_name[qb_id],
                        'player_id':qb_id,
                        'position':positions[qb_id],
                        'team':team[qb_id],
                        'x_pts':x_points[qb_id],
                        'dk_salary':salaries[qb_id],
                        'dk_id':dk_ids[qb_id]})
        qb_score = x_points[qb_id]
    for v in prob.variables():
        if v.varValue == 1:
            player_id = int(v.name.split('_')[1])
            players.append({'player_name':player_name[player_id],
                            'player_id':player_id,
                            'position':positions[player_id],
                            'team':team[player_id],
                            'x_pts':x_points[player_id],
                            'dk_salary':salaries[player_id],
                            'dk_id':dk_ids[player_id]})
            real_score += real_points[player_id]
    return {'players':players,
            'expected_lineup_score':value(prob.objective)+qb_score}

def get_column_dict(player_df, column):
    column_dict = {}
    for index, row in player_df.iterrows():
        column_dict[row['Player_ID']] = row[column]
    return column_dict

def get_team_con_dict(team):
    team_dict = {}
    for key in team:
        try:
            team_dict[team[key]] = team_dict[team[key]] + [key]
        except KeyError:
            team_dict[team[key]] = [key]
    return team_dict

def generate_lineups(week, year, player_df, n_lineups = 1, qb_list = [],
                     locked_players = [],
                     max_good_player_allocation = 1,
                     max_bad_player_allocation = 1,
                     max_defense_allocation = 1):

    #going to have some issues with max score when using different qb stacks
    lineup_list = []
    optimal_scores = []
    for _ in range(n_lineups):
        if len(optimal_scores) > 0:
            maximum_score = min(optimal_scores)
        else:
            maximum_score = None
        lineup = optimal_lineup(player_df, maximum_score = maximum_score)
        optimal_scores.append(lineup['expected_lineup_score'])
        lineup_list.append(lineup)

    return lineup_list

def update_player_df(player_df, exclude_list):
    new_player_df = player_df.copy()
    for player_id in exclude_list:
        new_player_df = new_player_df[new_player_df.Player_ID != player_id]
    return new_player_df
    
def get_player_df_id_tuple(player_df):
    player_id_tuple = ()
    for index, row in player_df.iterrows():
        player_id_tuple += (row['Player_ID'],)
    return player_id_tuple

def get_worst_value_from_lineup(lineup):
    min_player_value = 1
    for player in lineup['players']:
        if player['x_pts'] / player['dk_salary'] < min_player_value:
            min_player_value = player['x_pts'] / player['dk_salary']
            player_id_to_exclude = player['player_id']
    return player_id_to_exclude

def get_worst_pts_from_lineup(lineup, locked_players):
    min_player_points = 100
    gen = (player for player in lineup['players']
           if player['position'] != 'DST'
           and player['player_id'] not in locked_players)
    for player in gen:
        if player['x_pts'] < min_player_points:
            min_player_points = player['x_pts']
            player_id_to_exclude = player['player_id']
    return player_id_to_exclude

def get_player_set_id(player_df):
    n = 0
    player_id_checksum = 0
    for index, row in player_df.iterrows():
        n += 1
        player_id_checksum += row['Player_ID']
    return (n, player_id_checksum,)

def pretty_print_lineup(lineup):
    for player in lineup['players']:
        if player['position'] == 'QB':
            print(player['position'].ljust(4),
                  player['player_name'].ljust(25),
                  player['team'].ljust(6),
                  str(player['x_pts']).ljust(6),
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'RB':
            print(player['position'].ljust(4),
                  player['player_name'].ljust(25),
                  player['team'].ljust(6),
                  str(player['x_pts']).ljust(6),
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'WR':
            print(player['position'].ljust(4),
                  player['player_name'].ljust(25),
                  player['team'].ljust(6),
                  str(player['x_pts']).ljust(6),
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'TE':
            print(player['position'].ljust(4),
                  player['player_name'].ljust(25),
                  player['team'].ljust(6),
                  str(player['x_pts']).ljust(6),
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'DST':
            print(player['position'].ljust(4),
                  player['player_name'].ljust(25),
                  player['team'].ljust(6),
                  str(player['x_pts']).ljust(6),
                  str(player['dk_salary']))
    print(f'Expected Lineup Score = {lineup["expected_lineup_score"]}')

def pretty_print_psid(psids):
    for key in psids:
        print(key,'->',psids[key]['next_psid'])

def set_alls_from_lineup(player_allocations, lineup, player_type,
                         good_player_salary = 4500, qb = False):
    for player in lineup['players']:
        if qb == True:
            if player['position'] == 'QB':
                continue
        player_salary = player['dk_salary']
        if player_type == 'defense':
            try:
                player_allocations[player['player_id']] += 1
            except KeyError:
                player_allocations[player['player_id']] = 1 
        else:
            if player_salary < good_player_salary:
                if player_type == 'good':
                    continue
                else:
                    try:
                        player_allocations[player['player_id']] += 1
                    except KeyError:
                        player_allocations[player['player_id']] = 1  
            else:
                if player_type == 'bad':
                    continue
                else:
                    try:
                        player_allocations[player['player_id']] += 1
                    except KeyError:
                        player_allocations[player['player_id']] = 1
    return player_allocations

def new_exclude_list(new_exclude_list, good_player_alls, bad_player_alls,
                     defense_alls,
                     max_good_player_allocation,
                     max_bad_player_allocation,
                     max_defense_allocation,
                     n_lineups):
    for player, allocation in good_player_alls.items():
        if (allocation + 1) / n_lineups >= max_good_player_allocation:
            new_exclude_list.append(player)
    for player, allocation in bad_player_alls.items():
        if (allocation + 1) / n_lineups >= max_bad_player_allocation:
            new_exclude_list.append(player)
    for player, allocation in defense_alls.items():
        if (allocation + 1) / n_lineups >= max_defense_allocation:
            new_exclude_list.append(player)
    return new_exclude_list



def exclude_player(exclude_list, lineup,qb = False):
    #this excludes the worst value player from the lineup
    if qb == True:
        for player in lineup['players']:
            if player['position'] == 'QB':
                qb_team = player['team']
    else:
        qb_team = None
        
    min_player_value = 1
    for player in lineup['players']:
        if qb == True:
            if (player['team'] == qb_team):
                continue
        if player['position'] == 'TE':
            continue
        if player['x_pts'] / player['dk_salary'] < min_player_value:
            min_player_value = player['x_pts'] / player['dk_salary']
            player_id_to_exclude = player['player_id']
    exclude_list.append(player_id_to_exclude)
    return exclude_list

def lineup_analytics(lineups):
    #player allocation
    players = {}
    for lineup in lineups:
        for player in lineup['players']:
            if player['player_name'] not in players:
                players[player['player_name']] = 1
            else:
                players[player['player_name']] += 1
    for key, value in sorted(players.items(), key=lambda x: x[1]): 
        print("{} : {}".format(key, value))

def write_lineups_to_csv(week, year, lineups):
    if week < 10:
        week_str = '0' + str(week)
    else:
        week_str = str(week)
    with open(f'lineups_{year}{week_str}.csv','a') as fo:
        for lineup in lineups:
            pretty_print_lineup(lineup)
            fo.write(lineup_csv_string(lineup))

def lineup_csv_string(lineup):
    lineup_string = ''
    rb_count = 0
    wr_count = 0
    te_count = 0
    flex_id = ''
    for player in lineup['players']:
        if player['position'] == 'QB':
            lineup_string += str(player['dk_id'])+','
    for player in lineup['players']:
        if player['position'] == 'RB':
            if rb_count < 2:
                lineup_string += str(player['dk_id'])+','
                rb_count += 1
            else:
                flex_id = str(player['dk_id'])
    for player in lineup['players']:
        if player['position'] == 'WR':
            if wr_count < 3:
                lineup_string += str(player['dk_id'])+','
                wr_count += 1
            else:
                flex_id = str(player['dk_id'])
    for player in lineup['players']:
        if player['position'] == 'TE':
            if te_count < 1:
                lineup_string += str(player['dk_id'])+','
                te_count += 1
            else:
                flex_id = str(player['dk_id'])
    lineup_string += flex_id+','
    for player in lineup['players']:
        if player['position'] == 'DST':
            lineup_string += str(player['dk_id'])+','
    return lineup_string[:-1]+',,\n'
    

def main():
    server, database, uid, pwd = get_db_connection_items('database.prop')
    CONNECTION_STRING = "Driver={SQL Server Native Client 11.0};"+\
                        f"Server={server};Database={database};"+\
                        "Trusted_Connection=no;"+\
                        f"uid={uid};pwd={pwd}"
    YEAR = 2018
    WEEK = 11
    
    db_conn = get_sql_connection(CONNECTION_STRING)
    players = get_player_set(YEAR, WEEK, db_conn)
    #print(players)
    start = time.time()
    lineups = generate_lineups(WEEK, YEAR, players, n_lineups = 5)
    end = time.time()
    print(end - start)
    for lineup in lineups:
        pretty_print_lineup(lineup)

    db_conn.close()


if __name__ == "__main__":
    main()
