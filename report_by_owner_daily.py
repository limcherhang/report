import datetime
import xlsxwriter
import pandas as pd
import mysql
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
import time
import argparse
import logging
import os

parser = argparse.ArgumentParser()
parser.add_argument(
    '--report_date', default='2022-10-10 00:00:00', type = str,
    help="date you want to report: format: 'YYYYY-mm-dd HH:MM:SS'"
)
parser.add_argument(
    "--report_type", default='daily', type = str, 
    help="daily or monthly"
)
parser.add_argument(
    "--log", default='log', type = str,
    help = "path for log file"
)
args = parser.parse_args()

def get_user_owner(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):

    query = f"""
        SELECT 
            `account`, COUNT(uid) AS players, SUM(total_bet) AS total_bet, SUM(total_win) AS total_win, SUM(valid_bet) AS valid_bet,
            SUM(room_fee) AS room_fee, SUM(total_rake) AS total_rake, SUM(income) AS income, SUM(player_win) AS player_win,
            SUM(total_round) AS total_round, currency
        FROM
        (
            SELECT 
                owner_list.id AS `account`, uid, SUM(total_bet/rate) AS total_bet, SUM(total_win/rate) AS total_win, SUM(total_bet/rate) AS valid_bet, 
                0 AS room_fee, 0 AS total_rake, SUM(total_bet-total_win)/rate AS income, SUM(total_win/rate) AS player_win,
                SUM(total_round) AS total_round, user_list.currency
            FROM cypress.statistic_user_by_game AS stat
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.`account`, uid
            UNION
            SELECT 
                owner_list.id AS `account`, uid, SUM(total_bet/rate) AS total_bet, SUM(total_win/rate) AS total_win, SUM(valid_bet/rate) AS valid_bet, 
                0 AS room_fee, 0 AS total_rake, SUM(total_bet-total_win)/rate AS income, SUM(total_win/rate) AS player_win,
                SUM(total_bet_count) AS total_round, user_list.currency
            FROM cypress.statistic_user_by_lottogame AS stat
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.`account`, uid
            UNION
            SELECT 
                owner_list.id AS `account`, uid, SUM(total_bet/rate) AS total_bet, SUM(total_win+total_bet)/rate AS total_win, SUM(valid_bet/rate) AS valid_bet, 
                SUM(room_fee/rate) AS room_fee, SUM(total_rake/rate) AS total_rake, SUM(total_rake+room_fee-total_win)/rate AS income, SUM(total_win+total_bet-total_rake-room_fee)/rate AS player_win,
                SUM(total_round) AS total_round,user_list.currency
            FROM cypress.statistic_user_by_tablegame AS stat
            JOIN cypress.user_list ON user_list.id = stat.uid
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
            WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.`account`, uid
        ) AS stat
        GROUP BY `account`, currency
    """

    Mycursor.execute(query)
    return Mycursor.fetchall()

def get_last_day_players(report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):

    query = f"""
        SELECT owner_list.id AS `account`, user_list.currency, uid
        FROM
        (
            SELECT uid FROM cypress.statistic_user_by_game
            WHERE `date` >= '{report_last_date}' AND `date`< DATE_ADD('{report_last_date}', INTERVAL 1 DAY)
            UNION
            SELECT uid FROM cypress.statistic_user_by_lottogame
            WHERE `date` >= '{report_last_date}' AND `date`< DATE_ADD('{report_last_date}', INTERVAL 1 DAY)
            UNION
            SELECT uid FROM cypress.statistic_user_by_tablegame
            WHERE `date` >= '{report_last_date}' AND `date`< DATE_ADD('{report_last_date}', INTERVAL 1 DAY)
        ) AS stat
        JOIN cypress.user_list ON user_list.id = stat.uid
        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        WHERE parent_list.istestss=0 AND owner_list.istestss = 0
        GROUP BY owner_list.`account`, user_list.currency, uid
    """
    Mycursor.execute(query)

    return Mycursor.fetchall()

def get_h5_round(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    query = f"""
        SELECT  
            owner_list.id AS `account`, SUM(h5_round) AS h5_round, currency
        FROM
        MaReport.report_rounds_daily AS rep
        JOIN cypress.parent_list AS owner_list ON owner_list.id = rep.oid
        WHERE `date` = '{report_date}'
        GROUP BY owner_list.id, currency
    """

    Mycursor.execute(query)
    return Mycursor.fetchall()

def get_user_play_time(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    query = f"""
        SELECT 
            owner_list.id AS `account`, ugl.userid, SUM(UNIX_TIMESTAMP(endtime)-UNIX_TIMESTAMP(starttime)) AS play_time, user_list.currency
        FROM
        MaReport.user_gametoken_log AS ugl
        JOIN cypress.user_list ON user_list.userid = ugl.userid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        WHERE `date` >= '{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
        GROUP BY owner_list.id, ugl.userid
    """
    Mycursor.execute(query)
    return Mycursor.fetchall()

def get_add_lose_daily(report_date : str, report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    query_add = f"""
        SELECT rep.`account`, rep.uid, rep.currency FROM
        (
            SELECT `account`, uid, currency FROM
            (
                SELECT  owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_lottogame AS stat
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_tablegame AS stat
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
            ) AS stat
        ) AS rep
        LEFT JOIN
        (
            SELECT `account`, uid, currency FROM
            (
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat 
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY  owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat 
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY  owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat 
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
            ) AS stat
        ) AS `last` ON rep.uid = `last`.uid AND rep.`account`=`last`.`account`
        WHERE `last`.uid IS NULL
        GROUP BY `account`, uid
    """
    query_lose = f"""
        SELECT `last`.`account`, `last`.uid, `last`.currency FROM
        (
            SELECT `account`, uid, currency FROM
            (
                SELECT  owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
                UNION
                SELECT  owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_lottogame AS stat
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_tablegame AS stat
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
            ) AS stat
        ) AS rep
        RIGHT JOIN
        (
            SELECT `account`, uid, currency FROM
            (
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat 
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY  owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat 
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY  owner_list.id, uid
                UNION
                SELECT owner_list.id AS `account`, uid, user_list.currency FROM cypress.statistic_user_by_game AS stat 
                JOIN cypress.user_list ON stat.uid = user_list.id
                JOIN cypress.parent_list ON parent_list.id = user_list.parentid
                JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND parent_list.istestss=0 AND owner_list.istestss=0
                GROUP BY owner_list.id, uid
            ) AS stat
        ) AS `last` ON rep.uid = `last`.uid AND rep.`account`=`last`.`account`
        WHERE rep.uid IS NULL
        GROUP BY `account`, uid
    """

    Mycursor.execute(query_add)
    add_player = Mycursor.fetchall()

    Mycursor.execute(query_lose)
    lose_player = Mycursor.fetchall()

    return add_player, lose_player

if __name__ == '__main__':
    codes = time.time()
    rep_date = args.report_date
    rep_type = args.report_type
    filename = rep_date
    log_folder = args.log
    try:
        os.mkdir(log_folder)
    except FileExistsError:
        pass
    logger = logging.getLogger(__name__)
    logfile = f"./{log_folder}/{filename[:10]}_report_by_owner_daily.log" if rep_type == 'daily' else f"./{log_folder}/{filename[:10]}_report_by_owner_monthly.log"
    try:
        os.remove(logfile)
    except:
        logger.warning(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot remove {filename[0:10]}_report_by_game_daily.log!')

    logging.basicConfig(filename=logfile, level=logging.INFO, encoding='utf-8')

    #################
    # connect mysql #
    #################
    try:
        connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz', host='10.100.8.166')
    except connector.Error as e:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Error: Could not make connection to the MySQL database")
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {e}")

    if len(rep_date)!=19:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")
        raise "date format error"
    
    if rep_type!='daily' and rep_date[8:10] != '01':
        logger.error(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot find from Mareport')
        raise "cannot find from Mareport"
    
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : connecting the cursor")
    cursor = connection.cursor(buffered=True, dictionary=True)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : executing query")
    cursor.execute("SET time_zone = '+00:00';")

    if rep_type == 'daily':
        # get_currency = (
        #     'ALL', 'THB', 'IDR', 'CNY', 'VND', 'KRW', 'HKD', 'MMK', 'MYR', 'USD', 
        #     'INR', 'PHP', 'JPY', 'SGD', 'BET', 'MXN', 'RUB', 'AUD', 'CAD', 'EUR', 
        #     'PLN', 'TRY', 'CLP', 'mBTC', 'mETH', 'USDT', 'BDT', 'BRL', 'KES', 'mLTC', 
        #     'NOK', 'SEK', 'ZAR', 'KHR', 'GBP', 'DOGE', 'uBTC', 'TRX', 'BND', 'NPR', 
        #     'LAK', 'AED'
        # )
        rep_last_date = str(datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=1))
        get_currency_set = """
            SELECT 
                display_currency, query_currency
            FROM MaReport.report_query_currency
        """
        cursor.execute(get_currency_set)
        currency = cursor.fetchall()
        get_currency = {}
        for cur in currency:
            if cur['display_currency'] in ('ALL', 'CNY', 'KRW', 'THB', 'VND'):
                get_currency[f"{cur['display_currency']}"] = cur['query_currency'].split(',')
        # get_currency : {'ALL': ['ALL'], 'CNY': ['CNY'], 'KRW': ['KRW'], 'THB': ['THB'], 'VND': ['VND', 'VND(K)']}
        rep_last_date = str(datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=1))

        get_owner_set = f"""
            SELECT 
                owner_info.id AS `account`, owner_info.`owner`, currency, onlinetime
            FROM
            cypress.parent_list AS owner_list
            JOIN MaReport.owner_info ON owner_list.id=owner_info.id
            WHERE istestss=0;
        """
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the owner set!")
        cursor.execute(get_owner_set)
        owner_set = cursor.fetchall()

        dfs = []
        dict_owner = 0

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to fetch date")
        s = time.time()

        qqs =time.time()

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user owner")
        t1 = time.time()
        user_owner = get_user_owner(rep_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user owner done, time used : {t2-t1} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting players yesterday")
        p_last = get_last_day_players(rep_last_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting players yesterday done, time used : {t1-t2}")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round")
        
        h5_round = get_h5_round(rep_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round done, time.used : {t2-t1} sec")
        
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user total play time")
        ptime = get_user_play_time(rep_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user total play time done , time used : {t1-t2} sec")
        
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting add & lose player")
        add_player_uid, lose_player_uid = get_add_lose_daily(rep_date, rep_last_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting add & lose player done, time used : {t2-t1} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting players last day")
        player_last_uid = get_last_day_players(rep_last_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting players last day done, time used : {t1-t2} sec")

        ans = {}
        tmp = {}
        for cur in get_currency:
            ans[f'{cur}'] = {}
            tmp[f'{cur}'] = {}
            for dic in owner_set: # {`account`:, currency:, onlinetime:}
                ans[f'{cur}'][f"{dic['account']}"] = {}
                tmp[f'{cur}'][f"{dic['account']}"] = {}
                ans[f'{cur}'][f"{dic['account']}"]['序號'] = -1
                ans[f'{cur}'][f"{dic['account']}"]['總代理商名稱'] = dic['owner']
                ans[f'{cur}'][f"{dic['account']}"]['碼量'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['吐錢'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['有效投注'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['開房費'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['抽水錢'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['盈利'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['會員數'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['新增會員數'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['流失會員數'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['會員增減數'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['新增率'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['流失率'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['增減率'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['殺率'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['手機佔比'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['場次'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['會員留存時間(分鐘)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['上線日期'] = dic['onlinetime']
                tmp[f'{cur}'][f"{dic['account']}"]['玩家遊玩數'] = 0
                tmp[f"{cur}"][f"{dic['account']}"]['昨日玩家數'] = 0
                if dict_owner == 0:
                    dict_owner = 1
                    cols = ans[f'{cur}'][f"{dic['account']}"].keys()

        for user in user_owner:
            ans[f"ALL"][f"{user['account']}"]['碼量'] += user['total_bet']
            ans[f"ALL"][f"{user['account']}"]['吐錢'] += user['total_win']
            ans[f"ALL"][f"{user['account']}"]['有效投注'] += user['valid_bet']
            ans[f"ALL"][f"{user['account']}"]['開房費'] += user['room_fee']
            ans[f"ALL"][f"{user['account']}"]['抽水錢'] += user['total_rake']
            ans[f"ALL"][f"{user['account']}"]['盈利'] += user['income']
            ans[f"ALL"][f"{user['account']}"]['會員數'] += user['players']
            ans[f"ALL"][f"{user['account']}"]['殺率'] += user['player_win']
            ans[f"ALL"][f"{user['account']}"]['場次'] += user['total_round']
        
            for cur in get_currency:
                if user['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{user['account']}"]['碼量'] += user['total_bet']
                    ans[f"{cur}"][f"{user['account']}"]['吐錢'] += user['total_win']
                    ans[f"{cur}"][f"{user['account']}"]['有效投注'] += user['valid_bet']
                    ans[f"{cur}"][f"{user['account']}"]['開房費'] += user['room_fee']
                    ans[f"{cur}"][f"{user['account']}"]['抽水錢'] += user['total_rake']
                    ans[f"{cur}"][f"{user['account']}"]['盈利'] += user['income']
                    ans[f"{cur}"][f"{user['account']}"]['會員數'] += user['players']
                    ans[f"{cur}"][f"{user['account']}"]['殺率'] += user['player_win']
                    ans[f"{cur}"][f"{user['account']}"]['場次'] += user['total_round']
                    break

#       rep.`account`, rep.uid, rep.currency

        for uid in add_player_uid:
            ans[f"ALL"][f"{uid['account']}"]['新增會員數'] += 1
            
            for cur in get_currency:
                if uid['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{uid['account']}"]['新增會員數'] += 1
                    break

        for uid in lose_player_uid:
            ans[f"ALL"][f"{uid['account']}"]['流失會員數'] += 1
        
            for cur in get_currency:
                if uid['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{uid['account']}"]['流失會員數'] += 1
                    break
        for pl in player_last_uid:
            tmp[f'ALL'][f"{pl['account']}"]['昨日玩家數'] += 1
            for cur in get_currency:
                if pl['currency'] in get_currency[cur]:
                    tmp[f'{cur}'][f"{pl['account']}"]['昨日玩家數'] += 1
                    break

        for h5 in h5_round:
            ans[f"ALL"][f"{h5['account']}"]['手機佔比'] += h5['h5_round']
            for cur in get_currency:
                if h5['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{h5['account']}"]['手機佔比'] += h5['h5_round']
                    break

        for pt in ptime:
            ans[f"ALL"][f"{pt['account']}"]['會員留存時間(分鐘)'] += pt['play_time']
            tmp[f"ALL"][f"{pt['account']}"]['玩家遊玩數'] += 1
            for cur in get_currency:
                if pt['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{pt['account']}"]['會員留存時間(分鐘)'] += pt['play_time']
                    tmp[f"{cur}"][f"{pt['account']}"]['玩家遊玩數'] += 1
        
        for cur in get_currency:
            for dic in owner_set:
                ans[f'{cur}'][f"{dic['account']}"]['會員增減數'] = ans[f'{cur}'][f"{dic['account']}"]['新增會員數'] - ans[f'{cur}'][f"{dic['account']}"]['流失會員數']
                # tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數'] = ans[f'{cur}'][f"{dic['account']}"]['會員數']-ans[f'{cur}'][f"{dic['account']}"]['會員增減數']
                ans[f'{cur}'][f"{dic['account']}"]['會員留存時間(分鐘)'] = ans[f'{cur}'][f"{dic['account']}"]['會員留存時間(分鐘)']/tmp[f'{cur}'][f"{dic['account']}"]['玩家遊玩數']/60 if tmp[f'{cur}'][f"{dic['account']}"]['玩家遊玩數']> 0 else 0
                ans[f'{cur}'][f"{dic['account']}"]['新增率'] = ans[f'{cur}'][f"{dic['account']}"]['新增會員數']/tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數']*100 if tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數']>0 else 100
                ans[f'{cur}'][f"{dic['account']}"]['流失率'] = ans[f'{cur}'][f"{dic['account']}"]['流失會員數']/tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數']*100 if tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數']>0 else 0
                ans[f'{cur}'][f"{dic['account']}"]['增減率'] = ans[f'{cur}'][f"{dic['account']}"]['會員增減數']/tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數']*100 if tmp[f'{cur}'][f"{dic['account']}"]['昨日玩家數']>0 else 100
                ans[f'{cur}'][f"{dic['account']}"]['殺率'] = (1-ans[f'{cur}'][f"{dic['account']}"]['殺率']/ans[f'{cur}'][f"{dic['account']}"]['碼量'])*100 if ans[f'{cur}'][f"{dic['account']}"]['碼量']*100 >0 else 0 
                ans[f'{cur}'][f"{dic['account']}"]['手機佔比'] = ans[f'{cur}'][f"{dic['account']}"]['手機佔比']/ans[f'{cur}'][f"{dic['account']}"]['場次']*100 if ans[f'{cur}'][f"{dic['account']}"]['場次'] > 0 else 0

        for cur in get_currency:
            df = pd.DataFrame.from_dict(ans[cur], orient='index', columns=cols)
            df = df[df['會員留存時間(分鐘)'] > 0]
            df = df.sort_values(by = ['碼量'], ascending=False)
            print(df)
            if len(ans[f"{cur}"]) > 0:
                df.loc[:, '碼量'] = df['碼量'].map('{:,.2f}'.format)
                df.loc[:, '吐錢'] = df['吐錢'].map('{:,.2f}'.format)
                df.loc[:, '有效投注'] = df['有效投注'].map('{:,.2f}'.format)
                df.loc[:, '開房費'] = df['開房費'].map('{:,.2f}'.format)
                df.loc[:, '抽水錢'] = df['抽水錢'].map('{:,.2f}'.format)
                df.loc[:, '盈利'] = df['盈利'].map('{:,.2f}'.format)
                df.loc[:, '會員數'] = df['會員數'].map('{:,.0f}'.format)
                df.loc[:, '新增會員數'] = df['新增會員數'].map('{:,.0f}'.format)
                df.loc[:, '流失會員數'] = df['流失會員數'].map('{:,.0f}'.format)
                df.loc[:, '會員增減數'] = df['會員增減數'].map('{:,.0f}'.format)
                df.loc[:, '新增率'] = df['新增率'].map('{:,.2f}'.format)
                df.loc[:, '流失率'] = df['流失率'].map('{:,.2f}'.format)
                df.loc[:, '增減率'] = df['增減率'].map('{:,.2f}'.format)
                df.loc[:, '殺率'] = df['殺率'].map('{:,.2f}'.format)
                df.loc[:, '手機佔比'] = df['手機佔比'].map('{:,.2f}'.format)
                df.loc[:, '場次'] = df['場次'].map('{:,.0f}'.format)
                df.loc[:, '會員留存時間(分鐘)'] = df['會員留存時間(分鐘)'].map('{:,.2f}'.format)
            
            df['序號'] = range(1, len(df)+1)
            


            logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {cur} done")
            dfs.append(df)

        qqe = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : time used for query {(qqe-qqs)/60} minutes")
        
    else:
        get_currency = ('ALL', 'CNY', 
            'IDR', 
            'KRW', 'THB', 'VND')

    cursor.close()
    connection.close()
    
    codee = time.time()
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : query done, time : {(codee-codes)/60} minutes")

    logger.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to import into excel')
    s = time.time()
    save_file = f"{filename[:10]}_report_by_owner_daily.xlsx" if rep_type == 'daily' else f"{filename[:10]}_report_by_owner_monthly.xlsx"
    with pd.ExcelWriter(save_file,engine='xlsxwriter') as writer:
        for cur, df in zip(get_currency, dfs):
            df.to_excel(writer,sheet_name=f'{cur}', index = False)
            worksheet = writer.sheets[f'{cur}']
            
            for idx, col in enumerate(df):
                series = df[col]
                max_len = max(
                    series.astype(str).map(len).max(),
                    len(str(series.name))
                )+5
                worksheet.set_column(idx,idx,max_len)

    e = time.time()
    print('time used : ', (e-s)/60)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {save_file} import complete, time : {e-s} sec")