import pymongo
import datetime as dt
import time
import pandas as pd
import mysql
import mysql.connector as connector
import numpy as np
from openpyxl.utils import get_column_letter
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    '--report_date', default='2022-09-01 00:00:00', type = str,
    help="date you want to report: format: 'YYYYY-mm-dd HH:MM:SS'"
)
parser.add_argument(
    "--report_day", default=1, type = int, 
    help="day; format: 1 or 2 or ... or 31"
)
args = parser.parse_args()

rep_date = args.report_date
if len(rep_date) != 19:
    print("date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")
day = args.report_day
yes_date = dt.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S")

##############################################################################
def get_user_bet_win_income(gid : str, game_type : str, report_date : str) -> str:
    """
        Description : 取得gid, oid, pid, uid, total_bet, total_win, total_round,
                      valid_bet, income, currency, total_rake, room_fee, onlineime和brand
                      (所有金額都已經轉化成人民幣CNY)
    """
    if game_type in ('slot', 'arcade', 'fish'):
        query = f"""
            SELECT 
                stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, 
                SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
                SUM(total_round) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
                SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, user_list.currency,
                0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand, pf.firstGamingTime,
                IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0) AS `(non)first`
            FROM
            cypress.statistic_user_by_game AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid={gid}
            GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid
        """
    elif game_type in ('lotto', 'sport'):
        query = f"""
            SELECT 
                stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, 
                SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
                SUM(total_bet_count) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
                SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, user_list.currency,
                0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand, pf.firstGamingTime,
                IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0) AS `(non)first`
            FROM
            cypress.statistic_user_by_lottogame AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid={gid}
            GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid
        """
    elif game_type == 'table':
        query = f"""
            SELECT 
                stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, 
                SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_win/fx_rate.rate) AS total_win, 
                SUM(total_round) AS total_round, SUM(total_bet/fx_rate.rate) AS valid_bet,
                SUM(total_bet/fx_rate.rate)-SUM(total_win/fx_rate.rate) AS income, user_list.currency,
                0 AS total_rake, 0 AS room_fee, game_info.onlinetime, game_info.brand, pf.firstGamingTime,
                IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0) AS `(non)first`
            FROM
            cypress.statistic_user_by_tablegame AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid={gid}
            GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid
        """
    else:
        print('error : ', game_type)
    return query

def get_gid_player(gid : str, game_type : str, report_date : str):
    """
        Description : 取得gid, oid, pid,uid,acc和firsttime_gamming
    """
    if game_type in ('slot', 'arcade', 'fish'):
        query = f"""
            SELECT
                stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, user_list.account, player_first.firstGamingTime
            FROM
            cypress.statistic_user_by_game AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_first ON player_first.uid = stat.uid 
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid={gid}
            GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid
        """
    elif game_type in ('lotto', 'sport'):
        query = f"""
            SELECT
                stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, user_list.account, player_first.firstGamingTime
            FROM
            cypress.statistic_user_by_lottogame AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_first ON player_first.uid = stat.uid 
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid={gid}
            GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid
        """
    elif game_type == 'table':
        query = f"""
            SELECT
                stat.gid, user_list.ownerid AS oid, user_list.parentid AS pid, stat.uid, user_list.account, player_first.firstGamingTime
            FROM
            cypress.statistic_user_by_tablegame AS stat
            JOIN MaReport.game_info ON game_info.gid = stat.gid
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency=fx_rate.short_name
            JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_first ON player_first.uid = stat.uid 
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0 AND stat.gid={gid}
            GROUP BY stat.gid, user_list.ownerid, user_list.parentid, stat.uid
        """
    else:
        print('error : ', game_type)

    return query

def get_gid_set(gid_query : str, mycursor : mysql.connector.cursor.MySQLCursor) -> list:
    """
        Description : 取得所有gid, game_type, game_code和game_name_cn
    """
    mycursor.execute(gid_query)
    gid_set = mycursor.fetchall()

    return gid_set

def get_ALL_user_bet_win_income(gid : str, game_type : str, report_date : str, mycursor : mysql.connector.cursor.MySQLCursor) -> list:
    """
        Description : 取得所有currency為ALL的gid, bets, wins, valid_bet, income, currency, kill_rate,onlinetime, rakes, room_fee和brand
    """
    user_info = get_user_bet_win_income(gid, game_type, report_date)
    query = f"""
        SELECT 
            DATE_FORMAT('{report_date}','%Y-%m-%d'), gid, SUM(total_bet) AS bets, SUM(total_win) AS wins,
            SUM(total_round) AS rounds, SUM(valid_bet) AS valid_bet, SUM(income) AS income, 'ALL' AS currency, 
            FORMAT((SUM(total_bet)-SUM(total_win))/SUM(total_bet)*100 ,2) AS kill_rate, onlinetime, 
            SUM(total_rake) AS rakes, SUM(room_fee) AS room_fee, brand, 
            SUM(IF(`(non)first`=1, 1, 0)) AS player_first, SUM(IF(`(non)first`=1, total_bet, 0)) AS bets_first
        FROM
        (
            {user_info}
        ) AS tb
    """
    mycursor.execute(query)
    result = mycursor.fetchone()
    # print(result)
    # [date, gid, bets, wins,rounds, valid_bet, income, currency, kill_rate, onlinetime, rakes, room_fee, brand, (non)first, bets_first]
    return result

def get_ALL_gid_players(
        gid : str, 
        game_code : str,
        game_type : str, 
        report_date : str,
        yesterday_date : str, 
        mycursor : mysql.connector.cursor.MySQLCursor, 
    ) -> list:
    query_rep = get_gid_player(gid, game_type, report_date)
    query_yes = get_gid_player(gid, game_type, yesterday_date)

    mycursor.execute(query_rep)
    result_rep = mycursor.fetchall()    # 取得gid, oid, pid,uid,acc和firsttime_gamming
    
    # for res in result_rep:
    #     res[4] = str(res[4], 'utf-8')

    mycursor.execute(query_yes)
    result_yes = mycursor.fetchall()

    ###########
    query_start_end_time = get_uid_start_end_time(game_code, report_date)
    mycursor.execute(query_start_end_time)

    result_start_end_time = []

    # # [oid, pid, uid, 1 or 0, minbet, maxwin]
    for ele in mycursor.fetchall():
        result_start_end_time.append(list(ele))
    firsttime =  []
    nonfirsttime = []
    for ele in result_start_end_time:
        ele[4] = (ele[5].timestamp()-ele[4].timestamp())/60
        if ele[3] == '1':
            firsttime.append(ele[4])
        else:
            nonfirsttime.append(ele[4])
    player_first = len(firsttime)
    player_nonfirst = len(nonfirsttime)
    play_time = 0
    play_time_first = 0
    play_time_nonfirst = 0
    for ele in firsttime:
        play_time += ele
        play_time_first += ele
    for ele in nonfirsttime:
        play_time+= ele
        play_time_nonfirst += ele

    ###########
    result = [gid]                 # result = [gid]
    rep_players = len(result_rep)
    yes_players = len(result_yes)
    result.append(rep_players)  # result = [gid, players]
   
    add_players = 0
    lose_players = 0

    for rep in result_rep:
        if rep not in result_yes:
            add_players += 1
    
    for yes in result_yes:
        if yes not in result_rep:
            lose_players += 1
    diff_players = add_players-lose_players
    result.append(add_players)      # result = [gid, players, add_player]
    result.append(lose_players)     # result = [gid, players, add_player, lose_player]
    result.append(diff_players)     # result = [gid, players, add_player, lose_player, diff_player]
    if yes_players == 0:
        result.append(100.00)       # result = [gid, players, add_player, lose_player, diff_player, add_rate]
        result.append(100.00)       # result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate]
        result.append(100.00)       # result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate]
    else:
        result.append(round(add_players/yes_players, 2))
        result.append(round(lose_players/yes_players, 2))
        result.append(round(diff_players/yes_players, 2))

    if player_first != 0 and player_nonfirst != 0:
        result.append(play_time/(player_first+player_nonfirst)/60)# result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate,play_time]
        result.append(play_time_first/player_first/60)# result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate,play_time,playtime_first]
        result.append(play_time_nonfirst/player_nonfirst/60)# result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate,play_time,playtime_first, playtime_nonfirst]
    elif player_first == 0 and player_nonfirst != 0:
        result.append(play_time/(player_first+player_nonfirst)/60)
        result.append(0)
        result.append(play_time_nonfirst/player_nonfirst/60)
    elif player_first != 0 and player_nonfirst == 0: 
        result.append(play_time/(player_first+player_nonfirst)/60)
        result.append(play_time_first/player_first/60)
        result.append(0)
    else:
        result.append(0)
        result.append(0)
        result.append(0)
    result.append(player_first)# result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate,play_time,playtime_first, playtime_nonfirst, player_first]
    
    h5_rate = get_h5_rate(gid, report_date, mycursor)

    result.append(h5_rate)# result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate,play_time,playtime_first, playtime_nonfirst, player_first, h5_rate]

    return result   

def get_uid_start_end_time(game_code : str, report_date : str)-> str:
    """
        Description : 取得oid,pid,uid,game_code,min(starttime),max(endtime), firstgamingtime
    """
    query = f"""
    SELECT 
        owner_list.id AS oid, parent_list.id AS pid, user_list.id, 
        IF(pf.firstGamingTime>=@date1 AND pf.firstGamingTime<DATE_ADD(@date1,INTERVAL 1 DAY),1,0),
        MIN(starttime), MAX(endtime)
    FROM 
    MaReport.user_gametoken_log AS ugl
    JOIN cypress.user_list ON user_list.userid=ugl.userid
    JOIN cypress.parent_list ON user_list.parentid = parent_list.id
    JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
    JOIN MaReport.game_info ON game_info.game_code=ugl.game_code
    JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS pf ON owner_list.id=pf.oid AND parent_list.id=pf.pid AND user_list.id=pf.uid AND pf.gid=game_info.gid
    WHERE ugl.`date`>='{report_date}' AND ugl.`date`<DATE_ADD('{report_date}',INTERVAL 1 DAY) AND ugl.game_code = '{game_code}'
    GROUP BY owner_list.id, parent_list.id, user_list.id
    """
    
    return query # [oid, pid, uid, 1 or 0, minbet, maxwin]

def get_h5_rate(gid : str, report_date : str, mycursor : mysql.connector.cursor.MySQLCursor) -> float:
    query = f"""
        SELECT 
        SUM(h5_round)/SUM(total_round)*100
        FROM MaReport.report_rounds_daily 
        WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND gid = {gid};
    """
    mycursor.execute(query)

    for ele in mycursor.fetchone():
        # print(ele)
        if ele != None:
            h5_rate = float(ele)
        else:
            h5_rate = 0

    return h5_rate

#################
# connect mysql #
#################
try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                 host='10.100.8.166')
except connector.Error as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)

gid_query = """
SELECT 
    gid, game_type, game_code, game_name_cn
FROM
MaReport.game_info
"""

# ###################
# # connect mongodb #
# ###################
# username = "DAxHermes_aries"
# passw = "AK4EyH7Nx96_GDwz"
# host = ["10.100.8.87","10.100.8.88","10.100.8.89"]
# port = "27017"
# replicaset = "pro_da_ana_rs"
# authentication_source = "admin"

# uri = f"mongodb://{username}:{passw}@"
# uri += f":{port},".join(host)
# uri += f"/?replicaSet={replicaset}&authSource={authentication_source}"
# # print(uri)
# myclient = pymongo.MongoClient(uri) # port is default 27017

# # call database 'order'
# db = myclient.order

# # call collection 'order'
# collec = db.order

######################################################################################

start = time.time()
cursor = connection.cursor()

cursor.execute("SET time_zone = '+00:00';")
gid_set = get_gid_set(gid_query, cursor)


result = {}
for (gid, game_type, game_code,game_name_cn) in gid_set:
    if game_type=='sport':
        print(game_type, game_name_cn)
    result_income = get_ALL_user_bet_win_income(gid, game_type, rep_date, cursor)
    # print(result_income)
    # [date, gid, bets, wins,rounds, valid_bet, income, currency, kill_rate, onlinetime, rakes, room_fee, brand, (non)first, bets_first]
    result_players = get_ALL_gid_players(gid, game_code, game_type, rep_date, yes_date, cursor)
    # print(result_players)
    # result = [gid, players, add_player, lose_player, diff_player, add_rate, lose_rate, diff_rate,play_time,playtime_first, playtime_nonfirst, player_first, bets_first, h5_rate]
    if result_income[1] != None:

        result[f'{gid}'] = {
            "日期":result_income[0], "no":-1, "遊戲id":result_income[1] ,"遊戲名稱":game_name_cn, 
            "碼量":round(result_income[2],2), "吐錢":round(result_income[3],2), "有效投注":round(result_income[5],2), 
            "開房費":round(result_income[11],2), "抽水":round(result_income[10],2), "盈利": round(result_income[6],2), 
            "會員數":result_players[1], "新增會員數":result_players[2], "流失會員數":result_players[3],
            "會員增減數":result_players[4], "新增率":round(result_players[5],2), "流失率":round(result_players[6],2), 
            "增減率":round(result_players[7],2), 
            "殺率":round(float(result_income[8]),2), 
            "手機佔比":round(result_players[12],2), 
            "場次":result_income[4], "會員留存時間(分鐘)":round(result_players[8],2), "上線時間":str(result_players[9]),#result_players[9],
            "品牌":result_income[11], "匯率":'ALL', 
            "首遊碼量":round(result_income[14],2), 
            "手游玩家數":result_players[11], 
            "首游玩家留存時間":round(result_players[9],2), "非首游玩家留存時間":round(result_players[10],2)
        }

cursor.close()
connection.close()
end = time.time()
print("time_used : ", (end-start)/60, "min")
cols = list(result['1'].keys())
df = pd.DataFrame.from_dict(result, orient='index', columns=cols)
df = df.sort_values(by = ['碼量'], ascending=False)
df['no'] = range(1,len(df)+1)
# df['no'] = df["碼量"].rank(method = 'max', ascending=False)

df.to_excel(f'{rep_date[:10]}_report_by_game_{day}_day.xlsx')