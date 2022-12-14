import datetime
from gridfs import ConfigurationError
import xlsxwriter
import pandas as pd
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
from time import time
import argparse
import os
import logging
# from pydrive.drive import GoogleDrive
# from pydrive.auth import GoogleAuth

# gauth = GoogleAuth()
# gauth.LocalWebserverAuth()
# drive = GoogleDrive(gauth)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

try:
    os.mkdir('month_vs_ans')
except FileExistsError:
    pass

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
day = args.report_day
filename = time_start

# formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logfile = f"./month_vs_ans/log_{filename[0:10]}_{day}_day.log"
logging.basicConfig(filename=logfile, level=logging.INFO, encoding='utf-8')
# fh = logging.FileHandler(logfile, mode='w')
# fh.setLevel(logging.INFO)
# fh.setFormatter(formatter)

# sh = logging.StreamHandler()
# sh.setLevel(logging.INFO)
# sh.setFormatter(formatter)

# logger.addHandler(fh)
# logger.addHandler(sh)

if len(time_start) != 19:
    logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx', your given date is {time_start}")
    raise ValueError

if day > 1 and time_start[8:10] != '01':
    logger.error(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot find from Mareport')
    raise ConfigurationError
logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : config done!")

# country = ['ALL', 'CN', 'TH', 'KR', 'VN', 'PH']
currency = ['ALL', 'CNY', 'THB', 'KRW', 'VND']
logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : connecting the cursor")
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
                WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) 
                AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid}
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
                WHERE `date` >= '{rep_date}' AND updated_time < DATE_ADD('{rep_date}', INTERVAL 25 HOUR)  AND parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = {gid}        
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
                        WHERE `date` >= '{rep_date}' AND updated_time < DATE_ADD('{rep_date}', INTERVAL 25 HOUR) AND 
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
                        WHERE `date` >= '{rep_date}' AND updated_time < DATE_ADD('{rep_date}', INTERVAL 25 HOUR) AND 
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
                    WHERE `date` >= '{rep_date}' AND updated_time < DATE_ADD('{rep_date}', INTERVAL 25 HOUR) AND 
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

def ans_query_daily(gid : int, currency : str, rep_date : str) -> str:
    if currency == 'ALL':
        query = f"""
            SELECT 
            bets, rounds, FORMAT(100-kill_rate,2) AS RTP , wins, currency 
            FROM 
            MaReport.report_by_game_daily AS rep 
            JOIN MaReport.game_info ON game_info.gid=rep.gid
            WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND game_info.gid={gid} AND currency = 'ALL'
        """    
    else:

        query = f"""
            SELECT 
            bets, rounds, FORMAT(100-kill_rate,2) AS RTP , wins, currency 
            FROM 
            MaReport.report_by_game_daily AS rep 
            JOIN MaReport.game_info ON game_info.gid=rep.gid
            WHERE `date` >= '{rep_date}' AND `date` < DATE_ADD('{rep_date}', INTERVAL 1 DAY) AND currency = '{currency}' AND game_info.gid={gid}
        """
    # print(query)
    return query

def ans_query_monthly(gid : int, currency : str, rep_date : str) -> str:
    if currency == 'ALL':
        query = f"""
            SELECT 
            bets, rounds, FORMAT(100-kill_rate,2) AS RTP , wins, currency 
            FROM 
            MaReport.report_by_game_monthly AS rep 
            JOIN MaReport.game_info ON game_info.gid=rep.gid
            WHERE `date` = '{rep_date}' AND game_info.gid={gid} AND currency = 'ALL'
        """    
    else:

        query = f"""
            SELECT 
            bets, rounds, FORMAT(100-kill_rate,2) AS RTP , wins, currency 
            FROM 
            MaReport.report_by_game_monthly AS rep 
            JOIN MaReport.game_info ON game_info.gid=rep.gid
            WHERE `date` = '{rep_date}' AND currency = '{currency}' AND game_info.gid={gid}
        """
    # print(query)
    return query


logging.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : executing query")
s = time()

logger.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the gid set!')
cursor.execute(get_gid_set)
gid_set = cursor.fetchall()

dfs = []

logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to fetch date")

for idx_x, cur in enumerate(currency):
    
    ans = {}
    date = time_start
    for _ in range(day):
        for idx, (gid, game_type, game_name_cn) in enumerate(gid_set):
            # print(cur, gid, game_type, game_name_cn)
            # cursor.execute(report_by_daily_user(gid, game_type, cur, date))
            cursor.execute(report_by_daily_user(gid, game_type, cur, date)) # [SUM(total_bet), SUM(total_round), SUM(player_win) , SUM(total_win), bets, rounds, FORMAT(100-kill_rate,2) AS RTP , wins, currency ]
            result = cursor.fetchone()
            if day == 1:
                cursor.execute(ans_query_daily(gid, cur, date))
            else:
                cursor.execute(ans_query_monthly(gid, cur, time_start))
            result2 = cursor.fetchone()
            if result[0] != None:
                dict_gid = gid
                result += result2
            else:
                logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {date}, gid={gid}:{game_name_cn} doesn't have player at currency={cur}")
            if result[0] != None:
                try:
                    ans[f'{gid}']['?????????'] += float(str(result[0]).replace(',', ''))
                    ans[f'{gid}']['??????'] += int(result[1])
                    ans[f'{gid}']['RTP'] += float(str(result[2]).replace(',',''))
                    ans[f'{gid}']['?????????'] += float(str(result[3]).replace(',',''))
                except KeyError:
                    ans[f'{gid}'] = {"??????":time_start[0:10], "????????????":game_name_cn, "?????????":float(str(result[0]).replace(',', '')), 
                    "??????":int(result[1]), "RTP":float(str(result[2]).replace(',','')), 
                    "?????????":float(str(result[3]).replace(',','')),
                    # cur, game_type
                    "?????????(Ma)":float(str(result[4]).replace(',','')), "??????(Ma)":int(result[5]),
                    "RTP(Ma)":float(str(result[6]).replace(',','')),
                    "?????????(Ma)":float(str(result[7]).replace(',',''))
                    }
        logging.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {date} : {cur} done")
        date = str(datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')+datetime.timedelta(days=1))
    
    cols = list(ans[f'{dict_gid}'].keys())
    
    df = pd.DataFrame.from_dict(ans, orient='index', columns=cols)
    df['RTP'] = df['RTP']/df['?????????'] * 100
    df = df.sort_values(by = ['?????????'], ascending=False)

    df['?????????'] = df['?????????'] - df['?????????(Ma)']
    df['?????????'] = df['??????'] - df['??????(Ma)']
    df['RTP???'] = df['RTP'] - df['RTP(Ma)']
    df['?????????'] = df['?????????'] - df['?????????(Ma)']

    df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
    df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
    df.loc[:, '??????'] = df['??????'].map('{:,.0f}'.format)
    df.loc[:, 'RTP'] = df['RTP'].map('{:,.2f}'.format)
    df.loc[:, '?????????(Ma)'] = df['?????????(Ma)'].map('{:,.2f}'.format)
    df.loc[:, '?????????(Ma)'] = df['?????????(Ma)'].map('{:,.2f}'.format)
    df.loc[:, '??????(Ma)'] = df['??????(Ma)'].map('{:,.0f}'.format)
    df.loc[:, 'RTP(Ma)'] = df['RTP(Ma)'].map('{:,.2f}'.format)
    df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
    df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
    df.loc[:, '?????????'] = df['?????????'].map('{:,.0f}'.format)
    df.loc[:, 'RTP???'] = df['RTP???'].map('{:,.2f}'.format)

    df['??????'] = range(1, len(df)+1)
    df = df[['??????']+cols+["?????????", "?????????", "RTP???"]]
    dfs.append(df)
e = time()
logging.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : query done, time : {(e-s)/60} minutes")

logging.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to import into excel')
s = time()

with pd.ExcelWriter(f"./month_vs_ans/{filename[:10]}_bet_round_rtp_vs_ans_{day}_day.xlsx" ,engine='xlsxwriter') as writer:
    for cur, df in zip(currency, dfs):
        df.to_excel(writer,sheet_name=f'{cur}')
        worksheet = writer.sheets[f'{cur}']
        
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max(
                series.astype(str).map(len).max(),
                len(str(series.name))
            )+10
            worksheet.set_column(idx,idx,max_len)

cursor.close()
connection.close()
e = time()
logging.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {filename[:10]}_bet_round_rtp_vs_ans_{day}_day.xlsx import complete, time : {e-s} sec")