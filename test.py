import pymongo
import datetime as dt
from time import time
import pandas as pd
import mysql
import mysql.connector as connector
import numpy as np
from openpyxl.utils import get_column_letter

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

# cursor = collec.find_one()
# print(type(cursor))
# # for result in cursor:
# #     print(result[0])

#################
# connect mysql #
#################
try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                 host='10.100.8.166')
except connector.Error as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)

cursor_ = connection.cursor()

cursor_.execute("SET time_zone = '+00:00';")
cursor_.execute("SET @date1 = '2022-10-15';")
cursor_.execute("SET @date2 = DATE_ADD(@date1, INTERVAL 1 DAY);")

get_gid_set = """
SELECT 
    gid, game_type
FROM
MaReport.game_info
"""

def report_by_daily_user(gid : str, game_type : str) -> str:
    if game_type in ('slot', 'arcade', 'fish'):
        query = f"""
        SELECT 
            @date1 AS `date`, player_hour.oid, player_hour.pid, user_list.currency, fx_rate.rate, player_hour.gid, player_hour.uid, FORMAT(SUM(bets)*fx_rate.rate,2) AS total_bet, FORMAT(SUM(wins)*fx_rate.rate,2) AS total_win, 0 AS total_rake, FORMAT(SUM(rounds),2) AS total_round, FORMAT(SUM(bets)*fx_rate.rate,2) AS valid_bet, 0 AS room_fee, game_info.brand, DATE_FORMAT(player_active_info.firstGamingDate, '%Y-%m-%d')AS parent_first_date, DATE_FORMAT(player_info.firstGamingTime,'%Y-%m-%d') AS game_first_date 
        FROM 
        dataWarehouse.player_game_by_hour AS player_hour
        JOIN cypress.parent_list ON parent_list.id = player_hour.pid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = player_hour.oid
        JOIN MaReport.game_info ON game_info.gid = player_hour.gid
        JOIN cypress.user_list ON user_list.id = player_hour.uid
        JOIN cypress.fx_rate ON fx_rate.short_name = user_list.currency
        JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_info ON player_info.uid = player_hour.uid AND player_info.oid = owner_list.id AND player_info.pid = parent_list.id AND player_info.gid = game_info.gid
        JOIN dataWarehouse.player_active_info ON player_active_info.oid = owner_list.id AND player_active_info.pid = parent_list.id AND player_active_info.uid = user_list.id
        WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND player_hour.gid = \'{gid}\'
        GROUP BY oid, pid, gid, uid;
        """
    elif game_type in ('lotto','sport'):
        query = f"""
        SELECT
            @date1 AS `date`, player_hour.oid, player_hour.pid, user_list.currency, fx_rate.rate, player_hour.gid, player_hour.uid, FORMAT(SUM(bets)*fx_rate.rate,2) AS total_bet, FORMAT(SUM(wins)*fx_rate.rate,2) AS total_win, 0 AS total_rake, FORMAT(SUM(rounds),2) AS total_round, FORMAT(SUM(bets)*fx_rate.rate,2) AS valid_bet, 0 AS room_fee, game_info.brand, DATE_FORMAT(player_active_info.firstGamingDate, '%Y-%m-%d')AS parent_first_date, DATE_FORMAT(player_info.firstGamingTime,'%Y-%m-%d') AS game_first_date 
        FROM
        dataWarehouse.player_fish_game_by_hour AS player_hour
        JOIN cypress.parent_list ON parent_list.id = player_hour.pid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = player_hour.oid
        JOIN MaReport.game_info ON game_info.gid = player_hour.gid
        JOIN cypress.user_list ON user_list.id = player_hour.uid
        JOIN cypress.fx_rate ON fx_rate.short_name = user_list.currency
        JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_info ON player_info.uid = player_hour.uid AND player_info.oid = owner_list.id AND player_info.pid = parent_list.id AND player_info.gid = game_info.gid
        JOIN dataWarehouse.player_active_info ON player_active_info.oid = owner_list.id AND player_active_info.pid = parent_list.id AND player_active_info.uid = user_list.id
        WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND player_hour.gid = \'{gid}\'
        GROUP BY oid, pid, gid, uid;
        """
    elif game_type == 'table':
        query = f"""
        SELECT
            @date1 AS `date`, player_hour.oid, player_hour.pid, user_list.currency, fx_rate.rate, player_hour.gid, player_hour.uid, FORMAT(SUM(bets)*fx_rate.rate,2) AS total_bet, FORMAT(SUM(wins)*fx_rate.rate,2) AS total_win, FORMAT(SUM(rakes)*fx_rate.rate,2) AS total_rake, FORMAT(SUM(rounds),2) AS total_round, FORMAT(SUM(bets)*fx_rate.rate,2) AS valid_bet, FORMAT(SUM(roomFee)*fx_rate.rate,2) AS room_fee, game_info.brand, DATE_FORMAT(player_active_info.firstGamingDate, '%Y-%m-%d')AS parent_first_date, DATE_FORMAT(player_info.firstGamingTime,'%Y-%m-%d') AS game_first_date 
        FROM
        dataWarehouse.player_table_game_by_hour AS player_hour
        JOIN cypress.parent_list ON parent_list.id = player_hour.pid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = player_hour.oid
        JOIN MaReport.game_info ON game_info.gid = player_hour.gid
        JOIN cypress.user_list ON user_list.id = player_hour.uid
        JOIN cypress.fx_rate ON fx_rate.short_name = user_list.currency
        JOIN dataWarehouse.player_firstTime_lastTime_gaming_info AS player_info ON player_info.uid = player_hour.uid AND player_info.oid = owner_list.id AND player_info.pid = parent_list.id AND player_info.gid = game_info.gid
        JOIN dataWarehouse.player_active_info ON player_active_info.oid = owner_list.id AND player_active_info.pid = parent_list.id AND player_active_info.uid = user_list.id
        WHERE `date`>=@date1 AND `date`<DATE_ADD(@date1, INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND player_hour.gid = \'{gid}\'
        GROUP BY oid, pid, gid, uid;
        """
    else:
        print('')
        print(game_type,'error')
        print('')
        query = "SELECT gid FROM MaReport.game_info WHERE gid=-100;"
    return query


def exe(cursor : mysql.connector.cursor.MySQLCursor, gid_set_query : str) -> list:
    cursor.execute(gid_set_query)
    gid_set = cursor_.fetchall()
    daily_result = []
    for gid, game_type in gid_set:
        
        query = report_by_daily_user(gid = gid, game_type = game_type)
        # print(idx, game_type)
        cursor.execute(query)
        result = cursor_.fetchall()
        if len(result) != 0:
            for r in result:
                daily_result.append(r)
    return daily_result        

print('撈資料')
s = time()
result = exe(cursor_, get_gid_set)
cursor_.close()
connection.close()
e = time()
print('撈資料所需時間', (e-s)/60 )

print('存資料')
s = time()
cols = ['日期', '總代理商', '子代理商','幣值', '匯率','遊戲id','玩家id','總押注', '總吐錢', '抽水','總局數','總有效押注','房費','品牌','子代第一時間','玩家首遊時間']
df = pd.DataFrame(result, columns=cols)
def to_excel_auto_column_weight(df : pd.DataFrame, writer : pd.ExcelWriter, sheet_name='Sheet1'):
    df.to_excel(writer)
    column_widths=(
        df.columns.to_series().apply(lambda x: len(str(x).encode('gbk'))).values
    )

    max_widths = (
        df.astype(str).applymap(lambda x: len(str(x).encode('gbk'))).agg(max).values
    )

    widths = np.max([column_widths, max_widths], axis = 0)

    worksheet = writer.sheets[sheet_name]
    for i, width in enumerate(widths, 1):
        worksheet.column_dimensions[get_column_letter(i)].width = width + 4

with pd.ExcelWriter("10-15.xlsx") as writer:
    for id in range(len(cols), 0, -1):
        # df = df[df["game_code"] = id]
        to_excel_auto_column_weight(df, writer)

e = time()
print('存資料所需時間', (e-s)/60)