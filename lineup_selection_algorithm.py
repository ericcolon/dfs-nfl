from pulp import *
import pandas as pd
import pyodbc
from pprint import pprint
import time
import numpy as np
import matplotlib.pyplot as plt; plt.rcdefaults()
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_colwidth', -1)
pd.set_option('expand_frame_repr', True)
pd.set_option('display.width',None)

      
def get_sql_connection(connection_string):
    try:
        cnxn = pyodbc.connect(connection_string, autocommit=True)
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
    player_set = pd.read_sql_query(sql_string, db_conn)
    means = player_set.groupby('Position')['xDK_Points'].mean()
    stds = player_set.groupby('Position')['xDK_Points'].std()
    for index, row in player_set.iterrows():
        if row['xDK_Points'] >= (means[row['Position']] +
                                 stds[row['Position']]):
            player_set.at[index,'Quality'] = 'Great'
        elif row['xDK_Points'] < means[row['Position']]:
            player_set.at[index,'Quality'] = 'Below_Avg'
        else:
            player_set.at[index,'Quality'] = 'Above_Avg'
    return player_set

#want to have an exclude list, but should remove from df before this function
def optimal_lineup(player_df, salary_con = 50000, qb_con = 1,
                   def_con = 1, rb_max_con = 3, rb_min_con = 2,
                   wr_max_con = 4, wr_min_con = 3, te_max_con = 2,
                   te_min_con = 1, total_players_con = 9, qb_id = None,
                   stack_num = 0, one_team_con = False, two_team_con = False,
                   three_team_con = False, lineup_num = 0,
                   locked_players = [], maximum_score = None):
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
    if three_team_con == True:
        #need constraint that each team is only mapped to x players
        #ex: 1 player per team to minimize variance or more to max variance
        team_con_dict = get_team_con_dict(team)
        for team_key in team_con_dict:
            prob += lpSum([player_vars[player]
                          for player in players
                          if player in team_con_dict[team_key]]) <= 3,\
                          f'Max Three Players From {team_key} Constraint'

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

    assert LpStatus[prob.status] == 'Optimal'

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
                            'dk_id':dk_ids[player_id],
                            'real_points':real_points[player_id]})
            real_score += real_points[player_id]
    return {'players':players,
            'expected_lineup_score':value(prob.objective)+qb_score
            ,'actual_lineup_score':real_score}

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

def generate_lineups(week, year, player_df, n_lineups = 1,
                     locked_players = [],
                     max_great_player_allocation = 1,
                     max_above_avg_player_allocation = 1,
                     max_below_avg_player_allocation = 1,
                     max_defense_allocation = 1):

    #going to have some issues with max score when using different qb stacks
    lineup_list = []
    player_allocations = {}
    maximum_score = None
    for _ in range(n_lineups):
        lineup = optimal_lineup(player_df, maximum_score = maximum_score,
                                locked_players = locked_players,
                                three_team_con = True)
        update_player_allocations(player_allocations, lineup['players'])
        maximum_score = lineup['expected_lineup_score']
        update_player_list(player_df, n_lineups, player_allocations,
                           max_great_player_allocation,
                           max_above_avg_player_allocation,
                           max_below_avg_player_allocation,
                           max_defense_allocation,
                           locked_players)
        #pprint(player_allocations)
        lineup_list.append(lineup)
    return lineup_list

def update_player_allocations(player_allocations, lineup_players):
    for player in lineup_players:
        player_id = player['player_id']
        if player_id in player_allocations:
            player_allocations[player_id]['count'] += 1
        else:
            player_allocations[player_id] = {'count':1,
                                             'name':player['player_name'],
                                             'position':player['position'],
                                             'dk_salary':player['dk_salary']}

def update_player_list(player_df, n_lineups, player_allocations,
                       max_great_player_allocation,
                       max_above_avg_player_allocation,
                       max_below_avg_player_allocation,
                       max_defense_allocation,
                       locked_players):
    for index, row in player_df.iterrows():
        if row['Player_ID'] in locked_players:
            continue
        if row['Player_ID'] in player_allocations:
            player_allocation = ((player_allocations[row['Player_ID']]['count']
                                 + 1) / n_lineups)
            if row['Quality'] == 'Great' and row['Position'] != 'DST':
                if player_allocation > max_great_player_allocation:
                    player_df.drop(index,inplace=True)
            if row['Quality'] == 'Above_Avg' and row['Position'] != 'DST':
                if player_allocation > max_above_avg_player_allocation:
                    player_df.drop(index,inplace=True)
            if row['Quality'] == 'Below_Avg' and row['Position'] != 'DST':
                if player_allocation > max_below_avg_player_allocation:
                    player_df.drop(index,inplace=True)
            if row['Position'] == 'DST':
                if player_allocation > max_defense_allocation:
                    player_df.drop(index,inplace=True)
 

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
    print(f'Actual Lineup Score = {lineup["actual_lineup_score"]}')

def pretty_print_lineup_excel(lineup):
    for player in lineup['players']:
        if player['position'] == 'QB':
            print(player['position']+','+\
                  player['player_name']+','+\
                  player['team']+','+\
                  str(player['x_pts'])+','+\
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'RB':
            print(player['position']+','+\
                  player['player_name']+','+\
                  player['team']+','+\
                  str(player['x_pts'])+','+\
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'WR':
            print(player['position']+','+\
                  player['player_name']+','+\
                  player['team']+','+\
                  str(player['x_pts'])+','+\
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'TE':
            print(player['position']+','+\
                  player['player_name']+','+\
                  player['team']+','+\
                  str(player['x_pts'])+','+\
                  str(player['dk_salary']))
    for player in lineup['players']:
        if player['position'] == 'DST':
            print(player['position']+','+\
                  player['player_name']+','+\
                  player['team']+','+\
                  str(player['x_pts'])+','+\
                  str(player['dk_salary']))

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
        print("{}: {}".format(key, value))

def write_lineups_to_dk_csv(week, year, lineups):
    if week < 10:
        week_str = '0' + str(week)
    else:
        week_str = str(week)
    with open(f'lineups_dk_{year}{week_str}.csv','a') as fo:
        for lineup in lineups:
            pretty_print_lineup(lineup)
            fo.write(lineup_dk_csv_string(lineup))

def lineup_dk_csv_string(lineup):
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

def write_lineups_to_csv(week, year, lineups):
    if week < 10:
        week_str = '0' + str(week)
    else:
        week_str = str(week)
    with open(f'lineups_{year}{week_str}.csv','a') as fo:
        print('Writing lineups to CSV')
        fo.write('Position,Player,Team,xPts,Salary,Count\n')
        for lineup in lineups:
            fo.write(lineup_csv_string(lineup))

def lineup_csv_string(lineup):
    lineup_string = ''
    rb_count = 0
    wr_count = 0
    te_count = 0
    flex_string = ''
    for player in lineup['players']:
        if player['position'] == 'QB':
            lineup_string += player['position']+','+\
                             player['player_name']+','+\
                             player['team']+','+\
                             str(player['x_pts'])+','+\
                             str(player['dk_salary'])+',1\n'
    for player in lineup['players']:
        if player['position'] == 'RB':
            if rb_count < 2:
                lineup_string += player['position']+','+\
                                 player['player_name']+','+\
                                 player['team']+','+\
                                 str(player['x_pts'])+','+\
                                 str(player['dk_salary'])+',1\n'
                rb_count += 1
            else:
                flex_string += player['position']+','+\
                               player['player_name']+','+\
                               player['team']+','+\
                               str(player['x_pts'])+','+\
                               str(player['dk_salary'])+',1\n'
    for player in lineup['players']:
        if player['position'] == 'WR':
            if wr_count < 3:
                lineup_string += player['position']+','+\
                                 player['player_name']+','+\
                                 player['team']+','+\
                                 str(player['x_pts'])+','+\
                                 str(player['dk_salary'])+',1\n'
                wr_count += 1
            else:
                flex_string += player['position']+','+\
                               player['player_name']+','+\
                               player['team']+','+\
                               str(player['x_pts'])+','+\
                               str(player['dk_salary'])+',1\n'
    for player in lineup['players']:
        if player['position'] == 'TE':
            if te_count < 1:
                lineup_string += player['position']+','+\
                                 player['player_name']+','+\
                                 player['team']+','+\
                                 str(player['x_pts'])+','+\
                                 str(player['dk_salary'])+',1\n'
                te_count += 1
            else:
                flex_string += player['position']+','+\
                               player['player_name']+','+\
                               player['team']+','+\
                               str(player['x_pts'])+','+\
                               str(player['dk_salary'])+',1\n'
    lineup_string += flex_string
    for player in lineup['players']:
        if player['position'] == 'DST':
            lineup_string += player['position']+','+\
                             player['player_name']+','+\
                             player['team']+','+\
                             str(player['x_pts'])+','+\
                             str(player['dk_salary'])+',1\n'
    return lineup_string

def show_max_lineup(lineups):
    max_lineup_score = 0
    for lineup in lineups:
        if lineup['actual_lineup_score'] > max_lineup_score:
            max_lineup_score = lineup['actual_lineup_score']
            max_lineup = lineup
    pretty_print_lineup(max_lineup)

def get_locked_players_list(players):
    locked_list = []
    #get top 5 QBs
    qbs = players.loc[players['Position']
                == 'QB'].nlargest(10,'xDK_Points')['Player_ID'].tolist()
##    qbs = players.loc[players['Position']
##                == 'QB'].nlargest(5,'AVG_DK_Points')['Player_ID'].tolist()
    
    #get top 2 receivers for QB
    for qb in qbs:
        team = players.loc[players['Player_ID'] == qb]['Team_Abbr'].max()
        wrs = players.loc[players['Position']
                          == 'WR'].loc[players['Team_Abbr'] == team]
        tes = players.loc[players['Position']
                          == 'TE'].loc[players['Team_Abbr'] == team]
        receivers = pd.concat([wrs,tes]).nlargest(
            2,'xDK_Points')['Player_ID'].tolist()
        for receiver in receivers:
            locked_list.append([qb,receiver])
    return locked_list
        
def load_lineups_to_sql(week, year, lineups, db_conn):
    sql_string = "INSERT INTO DK_CDG7_Lineups (Week, Year, "+\
                 "Lineup_Number, Player, Position, Team, DK_Salary, "+\
                 "xDK_Points, Player_ID) VALUES (?,?,?,?,?,?,?,?,?)"
    params = []
    for n, lineup in enumerate(lineups, start=1):
        for player in lineup['players']:
            params.append((week, year, n, player['player_name'],
                           player['position'], player['team'],
                           player['dk_salary'], player['x_pts'],
                           player['player_id'],))
    cursor = db_conn.cursor()
    cursor.fast_executemany = True
    print('Loading lineups to SQL')
    try:
        cursor.executemany(sql_string, params)
    except Exception as e:
        print(e)
    cursor.close()

def main():
    server, database, uid, pwd = get_db_connection_items('database.prop')
    CONNECTION_STRING = "Driver={SQL Server Native Client 11.0};"+\
                        f"Server={server};Database={database};"+\
                        "Trusted_Connection=no;"+\
                        f"uid={uid};pwd={pwd}"
    YEAR = 2018
    WEEK = 9
    
    db_conn = get_sql_connection(CONNECTION_STRING)

    for WEEK in range(9,13):
        players = get_player_set(YEAR, WEEK, db_conn)
        start = time.time()
        locked_players_list = get_locked_players_list(players)

        lineups = []
        for locked_players in locked_players_list:
            player_df = players.copy()
            lineups += generate_lineups(WEEK, YEAR, player_df, n_lineups = 15,
                                        locked_players = locked_players,
                         max_great_player_allocation = .50,
                         max_above_avg_player_allocation = .25,
                         max_below_avg_player_allocation = .25,
                         max_defense_allocation = .25
                                        )
        #lineup_to_sort_on = 'actual_lineup_score'
    ##    sorted_lineups = sorted(lineups,
    ##                            key=lambda k: k[lineup_to_sort_on],
    ##                            reverse=True)
    ##    sorted_lineups = [lineup for lineup in sorted_lineups
    ##                      if lineup['actual_lineup_score'] > 168.62]
    ##    
        end = time.time()
        print(f'Time: {end - start}')
        #show_max_lineup(lineups)
        #pretty_print_lineup(lineup)
    ##    for lineup in sorted_lineups:
        #for lineup in lineups:
            #pretty_print_lineup(lineup)
        load_lineups_to_sql(WEEK, YEAR, lineups, db_conn)

    #lineup_analytics(lineups)

    db_conn.close()


if __name__ == "__main__":
    main()
