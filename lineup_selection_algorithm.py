from pulp import *
import pandas as pd
import pyodbc
import pprint

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_colwidth', -1)
pd.set_option('expand_frame_repr', True)
pd.set_option('display.width',None)

def get_sql_connection(connection_string):
    try:
        cnxn = pyodbc.connect(connection_string)
    except Exception as e:
        print(e)
        return None
    return cnxn

def get_player_set(year, week, db_conn):
    sql_string = f"exec sp_DK_Player_Set @week = {week}, @year = {year}"
    return pd.read_sql_query(sql_string, db_conn)

#want to have an exclude list, but should remove from df before this function
def optimal_lineup(player_df, salary_con = 50000, qb_con = 1,
                   def_con = 1, rb_max_con = 3, rb_min_con = 2,
                   wr_max_con = 4, wr_min_con = 3, te_max_con = 2,
                   te_min_con = 1, total_players_con = 9, qb_id = None,
                   stack_num = 0, one_team_con = False, two_team_con = False,
                   lineup_num = 0, exclude_list = []):
    players = [row['Player_ID'] for index, row in player_df.iterrows()
               if row['Player_ID']]
    positions = get_column_dict(player_df,'Position')
    salaries = get_column_dict(player_df,'DK_Salary')
    x_points = get_column_dict(player_df,'xDK_Points')
    team = get_column_dict(player_df,'Team_Abbr')
    player_name = get_column_dict(player_df,'Player')
    real_points = get_column_dict(player_df,'rDK_Points')
    
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


###End potential poopy shit
            
    prob.writeLP('LineupOptimizationModel.lp')
    prob.solve()

    lineup = {}
    
    print('Status: ',LpStatus[prob.status])
    real_score = 0

    qb_score = 0
    if qb_id is not None:
        print(player_name[qb_id].ljust(20),
              positions[qb_id].ljust(3),
              team[qb_id].ljust(5),
              str(x_points[qb_id]).ljust(6),
              str(salaries[qb_id]).ljust(6),
              lineup_num)
        qb_score = x_points[qb_id]
    for v in prob.variables():
        if v.varValue == 1:
            player_id = int(v.name.split('_')[1])
            print(player_name[player_id].ljust(20),
                  positions[player_id].ljust(3),
                  team[player_id].ljust(5),
                  str(x_points[player_id]).ljust(6),
                  str(salaries[player_id]).ljust(6),
                  lineup_num)
            real_score += real_points[player_id]
        #print(v.name,'=',v.varValue)

    print("Expected Lineup Score = ", value(prob.objective)+qb_score)
    print("Actual Lineup Score = ",real_score)
    #pprint.pprint(player_name)

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
                     max_player_allocation = None,
                     max_defense_allocation = None):
    if len(qb_list) > n_lineups:
        raise ValueError(f'Length of qb_list ({len(qb_list)}) cannot exceed '+\
                         f'n_lineups ({n_lineups})')
    lineups = {}
    player_allocations = {}
    qbs = len(qb_list)
    for lineup_index in range(n_lineups):
        if qbs > 0:
            qb_index = lineup_index % qbs
            #do stuff with stack
        else:
            #do stuff without stack
            pass
            
def get_db_connection_items(file):
    with open(file,'r') as fo:
        lines = fo.readlines()
        server = lines[0].split('=')[1].replace('\n','')
        database = lines[1].split('=')[1].replace('\n','')
        uid = lines[2].split('=')[1].replace('\n','')
        pwd = lines[3].split('=')[1]
    return server, database, uid, pwd


def main():
    server, database, uid, pwd = get_db_connection_items('database.prop')
    CONNECTION_STRING = "Driver={SQL Server Native Client 11.0};"+\
                        f"Server={server};Database={database};"+\
                        "Trusted_Connection=no;"+\
                        f"uid={uid};pwd={pwd}"
    YEAR = 2018
    WEEK = 10
    
    db_conn = get_sql_connection(CONNECTION_STRING)
    players = get_player_set(YEAR, WEEK, db_conn)
    qb_id = 872
    stack_num = 2
    lineup_num = '19'
    #print(players)
    optimal_lineup(players,one_team_con=True)
    #optimal_lineup(players,def_con=0,total_players_con=8)
##    optimal_lineup(players,qb_id=qb_id,stack_num=stack_num,
##                   lineup_num=lineup_num)
    #               def_con=0,total_players_con=8,salary_con=47000)
    db_conn.close()
    #generate_lineups(WEEK, YEAR, [], qb_list = [1,2])

if __name__ == "__main__":
    main()
