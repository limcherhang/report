import datetime
import openpyxl
import pandas as pd
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
from time import time
import argparse
# import os
# from pydrive.drive import GoogleDrive
# from pydrive.auth import GoogleAuth

# gauth = GoogleAuth()
# gauth.LocalWebserverAuth()
# drive = GoogleDrive(gauth)

# pa

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

# country = ['ALL', 'CN', 'TH', 'KR', 'VN', 'PH']
currency = ['ALL', 'CNY', 'THB', 'KRW', 'VND', 'PHP']

cursor = connection.cursor()

cursor.execute("SET time_zone = '+00:00';")
# cursor.execute(f"SET @date1 = '{time_start}';")

get_gid_set = """
SELECT 
    gid, game_type, game_name_cn
FROM
MaReport.game_info
"""

def report_by_daily_user(gid : int, game_type : str, currency : str, rep_date : str) -> str:

    if currency == 'ALL':
        if game_type in ('slot', 'arcade', 'fish'):
            query = f"""
                SELECT 
                    SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win/fx_rate.rate) AS player_win, SUM(total_win/fx_rate.rate) AS total_win
                FROM
                cypress.statistic_user_by_game AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid}
            """
        elif game_type in ('lotto', 'sport'):
            query = f"""
                SELECT 
                    SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_bet_count) AS total_round, SUM(total_win/fx_rate.rate) AS player_win ,SUM(total_win/fx_rate.rate) AS total_win
                FROM
                cypress.statistic_user_by_lottogame AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid}        
                """
        elif game_type == 'table':
            query = f"""
                SELECT 
                    SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win/fx_rate.rate+total_bet/fx_rate.rate-total_rake/fx_rate.rate-room_fee/fx_rate.rate) AS player_win, SUM((total_win+total_bet)/fx_rate.rate) AS total_win
                FROM
                cypress.statistic_user_by_tablegame AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid}
            """
    else:
        if game_type in ('slot', 'arcade', 'fish'):
            if currency == 'VND':
                query = f"""
                    SELECT 
                        SUM(total_bet), SUM(total_round), SUM(total_win) AS player_win, SUM(total_win)
                    FROM
                    (
                        SELECT 
                            SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win/fx_rate.rate) AS total_win
                        FROM
                        cypress.statistic_user_by_game AS stat
                        JOIN MaReport.game_info ON game_info.gid=stat.gid
                        JOIN cypress.user_list ON stat.uid = user_list.id
                        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                        WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='{currency}'
                        UNION
                        SELECT 
                            SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win/fx_rate.rate) AS total_win
                        FROM
                        cypress.statistic_user_by_game AS stat
                        JOIN MaReport.game_info ON game_info.gid=stat.gid
                        JOIN cypress.user_list ON stat.uid = user_list.id
                        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                        WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='VND(K)'
                    ) AS tb
                """
            else:
                query = f"""
                    SELECT 
                        SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win/fx_rate.rate) AS player_win , SUM(total_win/fx_rate.rate) AS total_win
                    FROM
                    cypress.statistic_user_by_game AS stat
                    JOIN MaReport.game_info ON game_info.gid=stat.gid
                    JOIN cypress.user_list ON stat.uid = user_list.id
                    JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                    JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                    JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                    WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                    parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='{currency}'
                """
        elif game_type in ('lotto', 'sport'):
            if currency == 'VND':
                query = f"""
                    SELECT 
                        SUM(total_bet), SUM(total_round), SUM(total_win) AS player_win , SUM(total_win)
                    FROM
                    (
                        SELECT 
                            SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_bet_count) AS total_round, SUM(total_win/fx_rate.rate) AS total_win
                        FROM
                        cypress.statistic_user_by_lottogame AS stat
                        JOIN MaReport.game_info ON game_info.gid=stat.gid
                        JOIN cypress.user_list ON stat.uid = user_list.id
                        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                        WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='{currency}'
                        UNION
                        SELECT 
                            SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_bet_count) AS total_round, SUM(total_win/fx_rate.rate) AS total_win
                        FROM
                        cypress.statistic_user_by_lottogame AS stat
                        JOIN MaReport.game_info ON game_info.gid=stat.gid
                        JOIN cypress.user_list ON stat.uid = user_list.id
                        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                        WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='VND(K)'
                    )AS tb
                    """
            else:
                query = f"""
                    SELECT 
                        SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_bet_count) AS total_round, SUM(total_win/fx_rate.rate) AS player_win, SUM(total_win/fx_rate.rate) AS total_win
                    FROM
                    cypress.statistic_user_by_lottogame AS stat
                    JOIN MaReport.game_info ON game_info.gid=stat.gid
                    JOIN cypress.user_list ON stat.uid = user_list.id
                    JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                    JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                    JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                    WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                    parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='{currency}'
                    """
        elif game_type == 'table':
            if currency == 'VND':
                query = f"""
                    SELECT 
                        SUM(total_bet), SUM(total_round), SUM(player_win) , SUM(total_win)
                    FROM
                    (
                        SELECT 
                            SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win+total_bet-total_rake-room_fee)/fx_rate.rate AS player_win, SUM((total_win+total_bet)/fx_rate.rate) AS total_win
                        FROM
                        cypress.statistic_user_by_tablegame AS stat
                        JOIN MaReport.game_info ON game_info.gid=stat.gid
                        JOIN cypress.user_list ON stat.uid = user_list.id
                        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                        WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='{currency}'
                        UNION
                        SELECT 
                            SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round, SUM(total_win+total_bet-total_rake-room_fee)/fx_rate.rate AS player_win, SUM((total_win+total_bet)/fx_rate.rate) AS total_win
                        FROM
                        cypress.statistic_user_by_tablegame AS stat
                        JOIN MaReport.game_info ON game_info.gid=stat.gid
                        JOIN cypress.user_list ON stat.uid = user_list.id
                        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                        WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='VND(K)'
                    )AS tb
                """
            else:
                query = f"""
                    SELECT 
                        SUM(total_bet/fx_rate.rate) AS total_bet, SUM(total_round) AS total_round,SUM(total_win+total_bet-total_rake-room_fee)/fx_rate.rate AS player_win, SUM((total_win+total_bet)/fx_rate.rate) AS total_win
                    FROM
                    cypress.statistic_user_by_tablegame AS stat
                    JOIN MaReport.game_info ON game_info.gid=stat.gid
                    JOIN cypress.user_list ON stat.uid = user_list.id
                    JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                    JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                    JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
                    WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND 
                    parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid} AND user_list.currency='{currency}'
                """
    if datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S").timestamp() < 1666022400 and currency in ('ALL','PHP'):
        query += f" AND owner_list.id != 19242"
    return query

print("executing query")
s = time()

cursor.execute(get_gid_set)
gid_set = cursor.fetchall()

dfs = []

for idx_x, cur in enumerate(currency):
    ans = {}
    date = time_start
    for _ in range(day):
        for idx, (gid, game_type, game_name_cn) in enumerate(gid_set):
            cursor.execute(report_by_daily_user(gid, game_type, cur, date))
            result = cursor.fetchone()
            if result[0] != None:
                try:
                    ans[f'{gid}'][2] += float(str(result[0]).replace(',', ''))
                    ans[f'{gid}'][3] += result[1]
                    ans[f'{gid}'][4] += float(str(result[2]).replace(',',''))
                    ans[f'{gid}'][5] += float(str(result[3]).replace(',',''))
                except KeyError:
                    ans[f'{gid}'] = [date[0:10], game_name_cn, float(str(result[0]).replace(',', '')), result[1], float(str(result[2]).replace(',','')), 
                    float(str(result[3]).replace(',','')),
                    cur, game_type]
        print(date, ':', cur, "done")
        date = str(datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(days=1))
    
    cols = ['??????','????????????','  ?????????  ','  ?????????  ', '  RTP  ', 
        '?????????', 
        'currency', 'game_type'
    ]
    
    df = pd.DataFrame.from_dict(ans, orient='index', columns=cols)
    df['  RTP  '] = df['  RTP  ']/df['  ?????????  '] * 100
    df = df.sort_values(by = ['  ?????????  '], ascending=False)

    df.loc[:, '  ?????????  '] = df['  ?????????  '].map('{:,.2f}'.format)
    df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
    df.loc[:, '  ?????????  '] = df['  ?????????  '].map('{:,.2f}'.format)
    df.loc[:, '  RTP  '] = df['  RTP  '].map('{:,.2f}'.format)

    df['??????'] = range(1, len(df)+1)
    df = df[['??????']+cols]
    dfs.append(df)
e = time()
print("query done, time:",(e-s)/60, 'minutes')

print('prepare to import into excel')
s = time()

with pd.ExcelWriter(f"./month/{filename[:10]}_bet_round_rtp_{day}_day.xlsx") as writer:
    for cur, df in zip(currency, dfs):
        df.to_excel(writer,sheet_name=f'{cur}')
    
        column_widths=(
                df.columns.to_series().apply(lambda x: len(str(x).encode('gbk'))).values
            )

        max_widths = (
            df.astype(str).applymap(lambda x: len(str(x).encode('gbk'))).agg(max).values
        )

        widths = np.max([column_widths, max_widths], axis = 0)

        worksheet = writer.sheets[f'{cur}']
        for i, width in enumerate(widths, 1):
            worksheet.column_dimensions[get_column_letter(i)].width = width + 4

cursor.close()
connection.close()
e = time()
print(f"{filename[:10]}_bet_round_rtp_{day}_day.xlsx import complete, time:", e-s, 'sec')