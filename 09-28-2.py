import pandas as pd
import mysql
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
from time import time
import datetime

#################
# connect mysql #
#################
try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                 host='10.100.8.166')
except connector.Error as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)

time_start = '2022-09-28 00:00:00'

cursor = connection.cursor()

cursor.execute("SET time_zone = '+00:00';")
cursor.execute(f"SET @date1 = '{time_start}';")

get_gid_set = """
SELECT 
    gid, game_type, game_name_cn
FROM
MaReport.game_info
"""

def report_by_hour_user(gid : int, game_type : str, times : str) -> str:

    if game_type in ('slot', 'arcade', 'fish'):
        query = f"""
            SELECT 
                DATE_FORMAT(@date1, '%Y-%m-%d') AS `date`, game_info.game_name_cn, FORMAT(SUM(total_bet/fx_rate.rate),2) AS total_bet, SUM(total_round) AS total_round
            FROM
            cypress.statistic_user_by_game AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= '{times}' AND `date` < DATE_ADD('{times}', INTERVAL 1 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};
        """
    elif game_type in ('lotto', 'sport'):
        query = f"""
            SELECT 
                DATE_FORMAT(@date1, '%Y-%m-%d') AS `date`, game_name_cn, FORMAT(SUM(total_bet/fx_rate.rate),2) AS total_bet, SUM(total_bet_count) AS total_round
            FROM
            cypress.statistic_user_by_lottogame AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= '{times}' AND `date` < DATE_ADD('{times}', INTERVAL 1 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};        
            """
    elif game_type == 'table':
        query = f"""
            SELECT 
                DATE_FORMAT(@date1, '%Y-%m-%d') AS `date`, game_name_cn, FORMAT(SUM(total_bet/fx_rate.rate),2) AS total_bet, SUM(total_round) AS total_round
            FROM
            cypress.statistic_user_by_tablegame AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= '{times}' AND `date` < DATE_ADD('{times}', INTERVAL 1 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};
        """
    return query

print("executing query")
s = time()

cursor.execute(get_gid_set)
gid_set = cursor.fetchall()

time_set = [str(datetime.datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S")+datetime.timedelta(hours=i)) for i in range(24)]

ans = []
del_list = []

for g in gid_set:
    ans.append([g[0],g[1],0 , 0])

for idx, (gid, game_type, game_name_cn) in enumerate(gid_set):
    for time_ in time_set:
        query = report_by_hour_user(gid=gid, game_type=game_type, times = time_)

        cursor.execute(query)

        result = cursor.fetchall()
        # print(gid, result)
        for r in result:
            # print(r)
            if r[2] != None:
                if type(r[2]) == str:
                    ans[idx][2] += float(r[2].replace(',',''))
                    ans[idx][3] += int(r[3])
                else:
                    ans[idx][2] += float(r[2])
                    ans[idx][3] += int(r[3])
    if ans[idx][2] == 0:
        del_list.append(idx)
    else:
        ans[idx][0] = result[0][0]
        ans[idx][1] = game_name_cn

for idx in del_list[::-1]:
    del ans[idx]


e = time()
print("query done, time:",(e-s))

cursor.close()
connection.close()

cols = ['日期','遊戲名稱','  總押注  ','  總局數  ']

print('prepare to export to excel')
s = time()
df = pd.DataFrame(ans, columns=cols)

df = df.sort_values(by = ['  總押注  '], ascending=False)
# print(df.info())

df.loc[:, '  總押注  '] = df['  總押注  '].map('{:,.2f}'.format)
df.loc[:, '  總局數  '] = df['  總局數  '].map('{:,.0f}'.format)

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

with pd.ExcelWriter("09-28_bet_round2.xlsx") as writer:
    for id in range(len(cols), 0, -1):
        # df = df[df["game_code"] = id]
        to_excel_auto_column_weight(df, writer)
e = time()
print("export done, time:", e-s)