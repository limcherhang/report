import pandas as pd
import mysql
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
from time import time

#################
# connect mysql #
#################
try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                 host='10.100.8.166')
except connector.Error as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)

cursor = connection.cursor()

cursor.execute("SET time_zone = '+00:00';")
cursor.execute("SET @date1 = '2022-09-30';")

get_gid_set = """
SELECT 
    gid, game_type
FROM
MaReport.game_info
"""

def report_by_daily_user(gid : int, game_type : str) -> str:

    if game_type in ('slot', 'arcade', 'fish'):
        query = f"""
            SELECT 
                @date1 AS `date`, game_name_cn, FORMAT(SUM(total_bet/fx_rate.rate),2) AS total_bet, SUM(total_round) AS total_round
            FROM
            cypress.statistic_user_by_game AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 6 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};
        """
    elif game_type in ('lotto', 'sport'):
        query = f"""
            SELECT 
                @date1 AS `date`, game_name_cn, FORMAT(SUM(total_bet/fx_rate.rate),2) AS total_bet, SUM(total_bet_count) AS total_round
            FROM
            cypress.statistic_user_by_lottogame AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 6 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};        
            """
    elif game_type == 'table':
        query = f"""
            SELECT 
                @date1 AS `date`, game_name_cn, FORMAT(SUM(total_bet/fx_rate.rate),2) AS total_bet, SUM(total_round) AS total_round
            FROM
            cypress.statistic_user_by_tablegame AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= @date1 AND `date` < DATE_ADD(@date1, INTERVAL 6 HOUR) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};
        """
    return query

print("executing query")
s = time()

cursor.execute(get_gid_set)
gid_set = cursor.fetchall()

ans = []
for gid, game_type in gid_set:
    cursor.execute(report_by_daily_user(gid, game_type))
    result = cursor.fetchall()
    if result[0][2] != None:
        for r in result:
            ans.append([r[0], r[1], float(r[2].replace(',','')), int(r[3])])

e = time()
print("query done, time:",(e-s))

cursor.close()
connection.close()

cols = ['日期','遊戲名稱','總押注','總局數']

print('prepare to export to excel')
s = time()
df = pd.DataFrame(ans, columns=cols)
print(df)
df = df.sort_values(by = ['總押注'], ascending=False)

df.loc[:, '  總押注  '] = df['總押注'].map('{:,.2f}'.format)
df.loc[:, '  總局數  '] = df['總局數'].map('{:,.0f}'.format)

print(df)
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

with pd.ExcelWriter("09-30_bet_round.xlsx") as writer:
    for id in range(len(cols), 0, -1):
        # df = df[df["game_code"] = id]
        to_excel_auto_column_weight(df, writer)
e = time()
print("export done, time:", e-s)