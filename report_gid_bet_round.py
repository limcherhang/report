import datetime
import pandas as pd
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
from time import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    '--report_date', default='2022-10-10 00:00:00', type = str,
    help="date you want to report: format: 'YYYYY-mm-dd HH:MM:SS'"
)
parser.add_argument(
    "--report_day", default=1, type = int, 
    help="day; format: 1 or 2 or ... 31"
)
args = parser.parse_args()

#################
# connect mysql #
#################
try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                 host='10.100.8.166')
except connector.Error as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)

time_start = args.report_date
filename = time_start
if len(time_start) != 19:
    print("date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")
day = args.report_day
cursor = connection.cursor()

cursor.execute("SET time_zone = '+00:00';")
# cursor.execute(f"SET @date1 = '{time_start}';")

get_gid_set = """
SELECT 
    gid, game_type, game_name_cn
FROM
MaReport.game_info
"""

def report_by_daily_user(gid : int, game_type : str, rep_date : str) -> str:

    if game_type in ('slot', 'arcade', 'fish'):
        query = f"""
            SELECT 
                SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round
            FROM
            cypress.statistic_user_by_game AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};
        """
    elif game_type in ('lotto', 'sport'):
        query = f"""
            SELECT 
                SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_bet_count) AS total_round
            FROM
            cypress.statistic_user_by_lottogame AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};        
            """
    elif game_type == 'table':
        query = f"""
            SELECT 
                SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round
            FROM
            cypress.statistic_user_by_tablegame AS stat
            JOIN MaReport.game_info ON game_info.gid=stat.gid
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid};
        """
    return query

print("executing query")
s = time()

cursor.execute(get_gid_set)
gid_set = cursor.fetchall()

ans = []
for _, _, game_name_cn in gid_set:
    ans.append([time_start[0:10], game_name_cn, 0, 0])

for _ in range(day):

    for idx, (gid, game_type, _) in enumerate(gid_set):
        cursor.execute(report_by_daily_user(gid, game_type, time_start))
        result = cursor.fetchall()
        if result[0][0] != None:
            for r in result:
                # ans.append([r[0], r[1], float(r[2].replace(',','')), int(r[3])])
                # ans[idx][2] += float(r[0].replace(',',''))
                ans[idx][2] += r[0]
                ans[idx][3] += int(r[1])
    print(time_start, "done")
    time_start = str(datetime.datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(days=1))

i = 0
while i < len(ans):
    if ans[i][2] == 0:
        del ans[i]
    else:
        i+=1

e = time()
print("query done, time:",(e-s)/60, 'minutes')

cursor.close()
connection.close()

cols = ['日期','遊戲名稱','  總押注  ','  總局數  ']

print('prepare to import into excel')
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

with pd.ExcelWriter(f"{filename[:10]}_bet_round_{day}_day.xlsx") as writer:
    for id in range(len(cols), 0, -1):
        # df = df[df["game_code"] = id]
        to_excel_auto_column_weight(df, writer)
e = time()
print("import complete, time:", e-s, 'sec')