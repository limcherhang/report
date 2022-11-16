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

def get_user_game(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    """
        In this function, we will get the dictionary include game_name_tw, uid, total_bet and etc. Please make sure your cursor is dictionary mode
    """
    query = f"""
        SELECT 
            game_name_tw, uid, SUM(total_bet/rate) AS total_bet, SUM(total_win/rate) AS total_win, SUM(total_bet/rate) AS valid_bet, 
            0 AS room_fee, 0 AS total_rake, SUM(total_bet-total_win)/rate AS income, SUM(total_win/rate) AS player_win,
            SUM(total_round) AS total_round, game_info.onlinetime, user_list.currency
        FROM 
        cypress.statistic_user_by_game AS stat
        JOIN MaReport.game_info ON game_info.gid = stat.gid
        JOIN cypress.user_list ON user_list.id = stat.uid
        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        JOIN cypress.fx_rate ON fx_rate.short_name = user_list.currency
        WHERE `date` >= '{report_date}' AND `date` < DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss = 0
        GROUP BY game_name_tw, uid
        UNION
        SELECT 
            game_name_tw, uid, SUM(total_bet/rate) AS total_bet, SUM(total_win/rate) AS total_win, SUM(valid_bet/rate) AS valid_bet, 
            0 AS room_fee, 0 AS total_rake, SUM(total_bet-total_win)/rate AS income, SUM(total_win/rate) AS player_win, 
            SUM(total_bet_count) AS total_round, game_info.onlinetime, user_list.currency
        FROM
        cypress.statistic_user_by_lottogame AS stat
        JOIN MaReport.game_info ON game_info.gid = stat.gid
        JOIN cypress.user_list ON user_list.id = stat.uid
        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        JOIN cypress.fx_rate ON fx_rate.short_name = user_list.currency
        WHERE `date` >= '{report_date}' AND `date` < DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss = 0
        GROUP BY game_name_tw, uid
        UNION
        SELECT 
            game_name_tw, uid, SUM(total_bet/rate) AS total_bet, SUM(total_win+total_bet)/rate AS total_win, SUM(valid_bet/rate) AS valid_bet, 
            SUM(room_fee/rate) AS room_fee, SUM(total_rake/rate) AS total_rake, SUM(total_rake+room_fee-total_win)/rate AS income, SUM(total_win+total_bet-total_rake-room_fee)/rate AS player_win, 
            SUM(total_round) AS total_round, game_info.onlinetime, user_list.currency
        FROM
        cypress.statistic_user_by_tablegame AS stat
        JOIN MaReport.game_info ON game_info.gid = stat.gid
        JOIN cypress.user_list ON user_list.id = stat.uid
        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        JOIN cypress.fx_rate ON fx_rate.short_name = user_list.currency
        WHERE `date` >= '{report_date}' AND `date` < DATE_ADD('{report_date}', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss = 0
        GROUP BY game_name_tw, uid
    """

    Mycursor.execute(query)

    return Mycursor.fetchall()

def get_h5_round(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    """
        In this function, we will get game_name_tw, sum of h5 round and corresponding currency
    """
    query = f"""
        SELECT 
            game_name_tw, SUM(h5_round) AS h5_round, currency
        FROM MaReport.report_rounds_daily AS rep
        JOIN cypress.parent_list ON parent_list.id = rep.pid
        JOIN MaReport.game_info ON game_info.gid=rep.gid
        WHERE `date`='{report_date}'
        GROUP BY game_name_tw, currency
    """
    Mycursor.execute(query)
    return Mycursor.fetchall()

def get_user_play_time(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    """
        In this function, we will get game_name_tw, userid, userid corresponding play_time and userid currency
    """
    query = f"""
        SELECT 
            game_name_tw, ugl.userid, SUM(UNIX_TIMESTAMP(endtime)-UNIX_TIMESTAMP(starttime)) AS play_time, user_list.currency
        FROM
        MaReport.user_gametoken_log AS ugl
        JOIN cypress.user_list ON user_list.userid=ugl.userid
        JOIN MaReport.game_info ON game_info.game_code = ugl.game_code
        WHERE `date` >= '{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
        GROUP BY game_name_tw, ugl.userid
    """

    Mycursor.execute(query)

    return Mycursor.fetchall()

def get_add_lose_daily(report_date : str, report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor) -> list:
    """
        In this function, we will get two result for add_player and lose_player, it include game_name_tw, uid and currency
    """
    query_add = f"""
        SELECT rep.game_name_tw, rep.uid, user_list.currency FROM
        (
            SELECT game_name_tw, uid FROM
            (
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_game AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_lottogame AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_tablegame AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
                GROUP BY game_name_tw, uid
            ) AS stat
        ) AS rep
        LEFT JOIN
        (
            SELECT game_name_tw, uid FROM
            (
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_game AS stat 
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}'
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_game AS stat 
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}'
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_game AS stat 
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}'
                GROUP BY game_name_tw, uid
            ) AS stat
        ) AS `last` ON rep.uid = `last`.uid AND rep.game_name_tw=`last`.game_name_tw
        JOIN cypress.user_list ON user_list.id=rep.uid
        JOIN cypress.parent_list ON parent_list.id=user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
        WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL
    """
    query_lose = f"""
        SELECT `last`.game_name_tw, `last`.uid, user_list.currency FROM
        (
            SELECT game_name_tw, uid FROM
            (
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_game AS stat 
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_lottogame AS stat 
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_tablegame AS stat 
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL 1 DAY)
                GROUP BY game_name_tw, uid
            ) AS stat
        ) AS rep
        RIGHT JOIN
        (
            SELECT game_name_tw, uid FROM
            (
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_game AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}'
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_lottogame AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}'
                GROUP BY game_name_tw, uid
                UNION
                SELECT game_name_tw, uid FROM cypress.statistic_user_by_tablegame AS stat
                JOIN MaReport.game_info ON game_info.gid=stat.gid
                WHERE `date`>='{report_last_date}' AND `date`<'{report_date}'
                GROUP BY game_name_tw, uid
            ) AS stat
        ) AS `last` ON rep.uid = `last`.uid AND rep.game_name_tw=`last`.game_name_tw
        JOIN cypress.user_list ON user_list.id=`last`.uid
        JOIN cypress.parent_list ON parent_list.id=user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
        WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND rep.uid IS NULL 
    """

    Mycursor.execute(query_add)
    add_player = Mycursor.fetchall()
    Mycursor.execute(query_lose)
    lose_player = Mycursor.fetchall()
    
    return add_player, lose_player

def get_last_day_players(report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    """
        In this function, we will get yesterday players for each game
    """
    # query = f"""
    #     SELECT game_name_tw, user_list.currency, uid
    #     FROM
    #     (
    #         SELECT gid, uid FROM cypress.statistic_user_by_game
    #         WHERE `date` >= '{report_last_date}' AND `date`< DATE_ADD('{report_last_date}', INTERVAL 1 DAY)
    #         UNION
    #         SELECT gid, uid FROM cypress.statistic_user_by_lottogame
    #         WHERE `date` >= '{report_last_date}' AND `date`< DATE_ADD('{report_last_date}', INTERVAL 1 DAY)
    #         UNION
    #         SELECT gid, uid FROM cypress.statistic_user_by_tablegame
    #         WHERE `date` >= '{report_last_date}' AND `date`< DATE_ADD('{report_last_date}', INTERVAL 1 DAY)
    #     ) AS stat
    #     JOIN MaReport.game_info ON game_info.gid=stat.gid
    #     JOIN cypress.user_list ON user_list.id = stat.uid
    #     JOIN cypress.parent_list ON parent_list.id = user_list.parentid
    #     JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
    #     WHERE parent_list.istestss=0 AND owner_list.istestss = 0
    #     GROUP BY game_name_tw, user_list.currency, uid
    # """
    query = f"""
        SELECT game_name_tw, currency, players
        FROM MaReport.report_by_game_daily AS rep
        JOIN MaReport.game_info ON game_info.gid=rep.gid
        WHERE `date`='{report_last_date}' AND currency IN ('ALL', 'CNY', 'KRW', 'THB', 'VND', 'IDR')
    """
    # print(query)
    Mycursor.execute(query)
    result = Mycursor.fetchall()

    return result

def get_last_month_players(report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
    """
        In this function, we will get last month players for each game
    """
    get_players = f"""
        SELECT 
            game_name_tw, players, currency
        FROM MaReport.report_by_game_monthly AS rep
        JOIN MaReport.game_info ON game_info.gid = rep.gid
        WHERE `date`='{report_last_date}' 
    """
    Mycursor.execute(get_players)
    return Mycursor.fetchall()

def get_add_lose_monthly(gid : int , game_type : str, report_date : str, report_day : int, report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor) -> list:
    """
        In this function, we will get add lose player for one game
    """
    if game_type in ('slot', 'fish', 'arcade'):
        query_add = f"""
            SELECT rep.uid, user_list.currency FROM
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_game AS stat 
                    WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS rep
            LEFT JOIN
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_game AS stat 
                    WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS `last` ON rep.uid = `last`.uid 
            JOIN cypress.user_list ON user_list.id=rep.uid
            JOIN cypress.parent_list ON parent_list.id=user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
            WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL
        """
        query_lose = f"""
            SELECT `last`.uid, user_list.currency FROM
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_game AS stat 
                    WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS rep
            RIGHT JOIN
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_game AS stat 
                    WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS `last` ON rep.uid = `last`.uid 
            JOIN cypress.user_list ON user_list.id=`last`.uid
            JOIN cypress.parent_list ON parent_list.id=user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
            WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND rep.uid IS NULL 
        """
    elif game_type in ('lotto', 'sport'):
        query_add = f"""
            SELECT rep.uid, user_list.currency FROM
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_lottogame AS stat 
                    WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS rep
            LEFT JOIN
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_lottogame AS stat 
                    WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS `last` ON rep.uid = `last`.uid 
            JOIN cypress.user_list ON user_list.id=rep.uid
            JOIN cypress.parent_list ON parent_list.id=user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
            WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL 
        """
        query_lose = f"""
            SELECT `last`.uid, user_list.currency FROM
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_lottogame AS stat 
                    WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS rep
            RIGHT JOIN
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_lottogame AS stat 
                    WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS `last` ON rep.uid = `last`.uid 
            JOIN cypress.user_list ON user_list.id=`last`.uid
            JOIN cypress.parent_list ON parent_list.id=user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
            WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND rep.uid IS NULL 
        """
    else:
        query_add = f"""
            SELECT rep.uid, user_list.currency FROM
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_tablegame AS stat 
                    WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS rep
            LEFT JOIN
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_tablegame AS stat 
                    WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS `last` ON rep.uid = `last`.uid 
            JOIN cypress.user_list ON user_list.id=rep.uid
            JOIN cypress.parent_list ON parent_list.id=user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
            WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL 
        """
        query_lose = f"""
            SELECT `last`.uid, user_list.currency FROM
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_tablegame AS stat 
                    WHERE `date`>='{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS rep
            RIGHT JOIN
            (
                SELECT uid FROM
                (
                    SELECT uid FROM cypress.statistic_user_by_tablegame AS stat 
                    WHERE `date`>='{report_last_date}' AND `date`<'{report_date}' AND gid={gid}
                    GROUP BY uid
                ) AS stat
            ) AS `last` ON rep.uid = `last`.uid 
            JOIN cypress.user_list ON user_list.id=`last`.uid
            JOIN cypress.parent_list ON parent_list.id=user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
            WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND rep.uid IS NULL
        """

    Mycursor.execute(query_add)
    add_player = Mycursor.fetchall()
    Mycursor.execute(query_lose)
    lose_player = Mycursor.fetchall()
    
    return add_player, lose_player

def get_day_occur(currency : str, report_date : str, report_day : int, Mycursor : mysql.connector.cursor.MySQLCursor):
    f"""
        In this function, we will get how many time occur in {report_day} day
    """
    query = f"""
        SELECT 
            game_name_tw, currency
        FROM MaReport.report_by_game_daily AS rep
        JOIN MaReport.game_info ON rep.gid=game_info.gid
        WHERE `date`>= '{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND currency IN {currency}
    """
    Mycursor.execute(query)

    return Mycursor.fetchall()

def get_daily_report(get_currency : list, report_date : str, report_day : int, Mycursor : mysql.connector.cursor.MySQLCursor):
    currency = ()
    for cur in get_currency:
        currency += (cur,)
    query = f"""
        SELECT 
            game_name_tw, SUM(bets) AS bets, SUM(wins)AS wins, SUM(valid_bet) AS valid_bet, SUM(room_fee) AS room_fee, SUM(rakes) AS rakes, 
            SUM(income) AS income, SUM((1-kill_rate/100)*bets) AS player_win, 
            SUM(h5_rate*rounds) AS h5_round, SUM(rounds) AS rounds, SUM(play_time*players) AS play_time, rep.onlinetime
        FROM MaReport.report_by_game_daily AS rep
        JOIN MaReport.game_info ON game_info.gid = rep.gid
        WHERE `date` >= '{report_date}' AND `date` < DATE_ADD('{report_date}', INTERVAL {report_day} DAY) AND currency IN {currency}
        GROUP BY game_name_tw, currency
    """

    Mycursor.execute(query)
    return Mycursor.fetchall()

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
    logfile = f"./{log_folder}/{filename[0:10]}_report_by_game_daily.log" if rep_type == "daily" else f"./{log_folder}/{filename[0:10]}_report_by_game_monthly.log"
    try:
        os.remove(logfile)
    except:
        logger.warning(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot remove {filename[0:10]}_report_by_game_daily.log!')

    

    logging.basicConfig(filename=logfile, level=logging.INFO, encoding='utf-8')

    #################
    # connect mysql #
    #################
    try:
        connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                    host='10.100.8.166')
    except connector.Error as e:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Error: Could not make connection to the MySQL database")
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {e}")


    if len(rep_date) != 19:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")

    if rep_type != 'daily' and rep_date[8:10] != '01':
        logger.error(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot find from Mareport')
        raise ValueError
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : connecting the cursor")
    cursor = connection.cursor(buffered=True, dictionary=True)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : executing query")
    cursor.execute("SET time_zone = '+00:00';")

    if rep_type == 'daily':
        get_currency = (
            'ALL', 'THB', 'IDR', 'CNY', 'VND', 'KRW', #'HKD', 'MMK', 'MYR', 'USD', 
            # 'INR', 'PHP', 'JPY', 'SGD', 'BET', 'MXN', 'RUB', 'AUD', 'CAD', 'EUR', 
            # 'PLN', 'TRY', 'CLP', 'mBTC', 'mETH', 'USDT', 'BDT', 'BRL', 'KES', 'mLTC', 
            # 'NOK', 'SEK', 'ZAR', 'KHR', 'GBP', 'DOGE', 'uBTC', 'TRX', 'BND', 'NPR', 
            # 'LAK', 'AED'
        )

        rep_last_date = str(datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=1))
        get_gid_set = f"""
        SELECT 
            game_info.gid, game_type, game_code, game_name_tw
        FROM
        MaReport.game_info
        """
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the gid set!")
        cursor.execute(get_gid_set)

        gid_set = cursor.fetchall()
        # for ele in cursor.fetchall():
        #     gid_set[f"{ele['gid']}"] = ele
            
        dfs = []
        dict_gid = 0
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to fetch date")
        s = time.time()

        bets = {}
        wins = {}
        valid_bet = {}
        room_fee = {}
        rakes = {}
        income = {}
        players = {}
        add_player = {}
        lose_player = {}
        diff_player = {}
        add_rate = {}
        lose_rate = {}
        diff_rate = {}
        player_win = {}
        h5_rate = {}
        rounds = {}
        play_time = {}
        onlinetime = {}
        uid_for_pt = {}
        players_last = {}

        qqs = time.time()
        
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user_game")
        t1 = time.time()
        user_game = get_user_game(rep_date, cursor)
        
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user_game done, time used : {t2-t1} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting player yesterday")
        p_last = get_last_day_players(rep_last_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting player yesterday done, time used : {t1-t2} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round")
        t1 = time.time()
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

        for dic in gid_set:
            
            bets[f"{dic['game_name_tw']}"] = {}
            wins[f"{dic['game_name_tw']}"] = {}
            valid_bet[f"{dic['game_name_tw']}"] = {}
            room_fee[f"{dic['game_name_tw']}"] = {}
            rakes[f"{dic['game_name_tw']}"] = {}
            income[f"{dic['game_name_tw']}"] = {}
            players[f"{dic['game_name_tw']}"] = {}
            add_player[f"{dic['game_name_tw']}"] = {}
            lose_player[f"{dic['game_name_tw']}"] = {}
            diff_player[f"{dic['game_name_tw']}"] = {}
            add_rate[f"{dic['game_name_tw']}"] = {}
            lose_rate[f"{dic['game_name_tw']}"] = {}
            diff_rate[f"{dic['game_name_tw']}"] = {}
            player_win[f"{dic['game_name_tw']}"] = {}
            h5_rate[f"{dic['game_name_tw']}"] = {}
            rounds[f"{dic['game_name_tw']}"] = {}
            play_time[f"{dic['game_name_tw']}"] = {}
            onlinetime[f"{dic['game_name_tw']}"] = {}
            uid_for_pt[f"{dic['game_name_tw']}"] = {}
            players_last[f"{dic['game_name_tw']}"] = {}
            for cur in get_currency:
                bets[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                wins[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                valid_bet[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                room_fee[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                rakes[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                income[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                players[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                add_player[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                lose_player[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                diff_player[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                add_rate[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                lose_rate[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                diff_rate[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                player_win[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                h5_rate[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                rounds[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                play_time[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                onlinetime[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                uid_for_pt[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                players_last[f"{dic['game_name_tw']}"][f"{cur}"] = 0

        for uid in add_player_uid:
            add_player[f"{uid['game_name_tw']}"][f"ALL"] += 1
            if uid['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                add_player[f"{uid['game_name_tw']}"][f"{uid['currency']}"]+=1
            elif uid['currency'] in ('IDR(K)','VND(K)'):
                if uid['currency'] == 'VND(K)':
                    add_player[f"{uid['game_name_tw']}"][f"VND"]+=1
                else:
                    add_player[f"{uid['game_name_tw']}"][f"IDR"] += 1
        
        for uid in lose_player_uid:
            lose_player[f"{uid['game_name_tw']}"][f"ALL"] += 1
            if uid['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                lose_player[f"{uid['game_name_tw']}"][f"{uid['currency']}"]+=1
            elif uid['currency'] in ('IDR(K)','VND(K)'):
                if uid['currency'] == 'VND(K)':
                    lose_player[f"{uid['game_name_tw']}"][f"VND"]+=1
                else:
                    lose_player[f"{uid['game_name_tw']}"][f"IDR"] += 1

        for pl in p_last:
            # print(pl)
            # players_last[f"{pl['game_name_tw']}"][f"ALL"] += 1
            # if pl['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
            #     players_last[f"{pl['game_name_tw']}"][f"{pl['currency']}"] += 1
            # elif pl['currency'] in ('IDR(K)', 'VND(K)'):
            #     if pl['currency'] == 'VND(K)':
            #         players_last[f"{pl['game_name_tw']}"][f"VND"] += 1
            #     else:
            #         players_last[f"{pl['game_name_tw']}"][f"IDR"] += 1
            players_last[f"{pl['game_name_tw']}"][f"{pl['currency']}"] = pl['players']

        for user in user_game:
            bets[f"{user['game_name_tw']}"][f"ALL"] += user['total_bet']
            wins[f"{user['game_name_tw']}"][f"ALL"] += user['total_win']
            valid_bet[f"{user['game_name_tw']}"][f"ALL"] += user['valid_bet']
            room_fee[f"{user['game_name_tw']}"][f"ALL"] += user['room_fee']
            rakes[f"{user['game_name_tw']}"][f"ALL"] += user['total_rake']
            income[f"{user['game_name_tw']}"][f"ALL"] += user['income']
            player_win[f"{user['game_name_tw']}"][f"ALL"] += user['player_win']
            rounds[f"{user['game_name_tw']}"][f"ALL"] += user['total_round']
            players[f"{user['game_name_tw']}"][f"ALL"] += 1
            onlinetime[f"{user['game_name_tw']}"][f"ALL"] = user['onlinetime']
            if user['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                
                bets[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['total_bet']
                wins[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['total_win']
                valid_bet[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['valid_bet']
                room_fee[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['room_fee']
                rakes[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['total_rake']
                income[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['income']
                player_win[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['player_win']
                rounds[f"{user['game_name_tw']}"][f"{user['currency']}"] += user['total_round']
                players[f"{user['game_name_tw']}"][f"{user['currency']}"] += 1
                onlinetime[f"{user['game_name_tw']}"][f"{user['currency']}"] = user['onlinetime']
            elif user['currency'] in ('VND(K)', 'IDR(K)'):
                if user['currency'] == 'VND(K)':
                    bets[f"{user['game_name_tw']}"][f"VND"] += user['total_bet']
                    wins[f"{user['game_name_tw']}"][f"VND"] += user['total_win']
                    valid_bet[f"{user['game_name_tw']}"][f"VND"] += user['valid_bet']
                    room_fee[f"{user['game_name_tw']}"][f"VND"] += user['room_fee']
                    rakes[f"{user['game_name_tw']}"][f"VND"] += user['total_rake']
                    income[f"{user['game_name_tw']}"][f"VND"] += user['income']
                    player_win[f"{user['game_name_tw']}"][f"VND"] += user['player_win']
                    rounds[f"{user['game_name_tw']}"][f"VND"] += user['total_round']
                    players[f"{user['game_name_tw']}"][f"VND"] += 1
                    onlinetime[f"{user['game_name_tw']}"][f"VND"] = user['onlinetime']
                else:
                    bets[f"{user['game_name_tw']}"][f"IDR"] += user['total_bet']
                    wins[f"{user['game_name_tw']}"][f"IDR"] += user['total_win']
                    valid_bet[f"{user['game_name_tw']}"][f"IDR"] += user['valid_bet']
                    room_fee[f"{user['game_name_tw']}"][f"IDR"] += user['room_fee']
                    rakes[f"{user['game_name_tw']}"][f"IDR"] += user['total_rake']
                    income[f"{user['game_name_tw']}"][f"IDR"] += user['income']
                    player_win[f"{user['game_name_tw']}"][f"IDR"] += user['player_win']
                    rounds[f"{user['game_name_tw']}"][f"IDR"] += user['total_round']
                    players[f"{user['game_name_tw']}"][f"IDR"] += 1
                    onlinetime[f"{user['game_name_tw']}"][f"IDR"] = user['onlinetime']
        for h5 in h5_round:
            h5_rate[f"{h5['game_name_tw']}"][f"ALL"] += h5['h5_round']
            if h5['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                h5_rate[f"{h5['game_name_tw']}"][f"{h5['currency']}"] += h5['h5_round']
            elif h5['currency'] in ('VND(K)', 'IDR(K)'):
                if h5['currency'] == 'VND(K)':
                    h5_rate[f"{h5['game_name_tw']}"][f"VND"] += h5['h5_round']
                else:
                    h5_rate[f"{h5['game_name_tw']}"][f"IDR"] += h5['h5_round']
        for pt in ptime:
            play_time[f"{pt['game_name_tw']}"][f"ALL"] += pt['play_time']
            uid_for_pt[f"{pt['game_name_tw']}"][f"ALL"] += 1
            if pt['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                play_time[f"{pt['game_name_tw']}"][f"{pt['currency']}"] += pt['play_time']
                uid_for_pt[f"{pt['game_name_tw']}"][f"{pt['currency']}"] += 1
            elif pt['currency'] in ('VND(K)', 'IDR(K)'):
                if pt['currency'] == 'VND(K)':
                    play_time[f"{pt['game_name_tw']}"][f"VND"] += pt['play_time']
                    uid_for_pt[f"{pt['game_name_tw']}"][f"VND"] += 1
                else:
                    play_time[f"{pt['game_name_tw']}"][f"IDR"] += pt['play_time']
                    uid_for_pt[f"{pt['game_name_tw']}"][f"IDR"] += 1
        qqe = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : time used for query {(qqe-qqs)/60} minutes")
        
        for idx, cur in enumerate(get_currency):
            querys = time.time()
            ans = {}
            for dic in gid_set:
                if bets[f"{dic['game_name_tw']}"][f'{cur}'] > 0:
                    ans[f"{dic['game_name_tw']}"] = {
                        '序號':-1, '遊戲名稱':dic['game_name_tw'], '碼量':bets[f"{dic['game_name_tw']}"][f'{cur}'],
                        '吐錢':wins[f"{dic['game_name_tw']}"][f'{cur}'], '有效投注':valid_bet[f"{dic['game_name_tw']}"][f'{cur}'],
                        '開房費':room_fee[f"{dic['game_name_tw']}"][f'{cur}'], '抽水錢':rakes[f"{dic['game_name_tw']}"][f'{cur}'],
                        '盈利':income[f"{dic['game_name_tw']}"][f'{cur}'], '會員數':players[f"{dic['game_name_tw']}"][f'{cur}'],
                        '新增會員數':add_player[f"{dic['game_name_tw']}"][f'{cur}'], '流失會員數':lose_player[f"{dic['game_name_tw']}"][f'{cur}'],
                        '會員增減數':add_player[f"{dic['game_name_tw']}"][f'{cur}']-lose_player[f"{dic['game_name_tw']}"][f'{cur}'],
                        '新增率':add_player[f"{dic['game_name_tw']}"][f'{cur}']/(players_last[f"{dic['game_name_tw']}"][f'{cur}'])*100 if players_last[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 100,
                        '流失率':lose_player[f"{dic['game_name_tw']}"][f'{cur}']/(players_last[f"{dic['game_name_tw']}"][f'{cur}'])*100 if players_last[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 0,
                        '增減率':(add_player[f"{dic['game_name_tw']}"][f'{cur}']-lose_player[f"{dic['game_name_tw']}"][f'{cur}'])/(players_last[f"{dic['game_name_tw']}"][f'{cur}'])*100 if players_last[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 100,
                        '殺率':(1-player_win[f"{dic['game_name_tw']}"][f'{cur}']/bets[f"{dic['game_name_tw']}"][f'{cur}'])*100,
                        '手機佔比':h5_rate[f"{dic['game_name_tw']}"][f'{cur}']/rounds[f"{dic['game_name_tw']}"][f'{cur}']*100, '場次' : rounds[f"{dic['game_name_tw']}"][f'{cur}'],
                        '會員留存時間(分鐘)':0 if dic['game_type'] in ('lotto', 'sport') else play_time[f"{dic['game_name_tw']}"][f'{cur}']/uid_for_pt[f"{dic['game_name_tw']}"][f'{cur}']/60,
                        '上線日期':onlinetime[f"{dic['game_name_tw']}"][f'{cur}']
                    }
                if dict_gid == 0:
                    dict_gid = dic['game_name_tw']
                    cols = list(ans[f"{dict_gid}"].keys())

            df = pd.DataFrame.from_dict(ans, orient='index', columns=cols)

            df = df.sort_values(by = ['碼量'], ascending=False)
            # print(df)
            if len(ans) > 0:
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
            print(f'{cur} used time : {time.time()-querys}')
    else:
        get_currency = ('ALL', 'CNY', 
        'IDR', 
        'KRW', 'THB', 'VND')
        if rep_date[5:7] in ("05", "07", "10", "12"):
            rep_day = 31
            rep_last = 30
        elif rep_date[5:7] in ("04", "06", "09", "11"):
            rep_day = 30
            rep_last = 31
        elif rep_date[5:7] in ("01", "08"):
            rep_day = 31
            rep_last = 31
        elif rep_date[5:7] == "03":
            rep_day = 31
            if int(rep_date[0:4]) % 4 != 0:
                rep_last = 28
            elif int(rep_date[0:4]) % 4 == 0 and int(rep_date[0:4]) % 100 != 0:
                rep_last = 29
            elif int(rep_date[0:4]) % 100 == 0 and int(rep_date[0:4]) % 400 != 0:
                rep_last = 28
            else:
                rep_last = 29
        else:
            rep_last = 31
            if int(rep_date[0:4]) % 4 != 0:
                rep_day = 28
            elif int(rep_date[0:4]) % 4 == 0 and int(rep_date[0:4]) % 100 != 0:
                rep_day = 29
            elif int(rep_date[0:4]) % 100 == 0 and int(rep_date[0:4]) % 400 != 0:
                rep_day = 28
            else:
                rep_day = 29
        rep_last_date = str(datetime.datetime.strptime(rep_date,"%Y-%m-%d %H:%M:%S")-datetime.timedelta(days=rep_last))
        rep_after_date = str(datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S")+datetime.timedelta(days=rep_day))
        dfs = []
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the gid set!")
        get_gid_set = """
            SELECT 
                gid, game_name_tw, game_type
            FROM
            MaReport.game_info
        """
        cursor.execute(get_gid_set)
        gid_set = cursor.fetchall()

        dict_gid = None

        bets = {} 
        wins = {}
        valid_bet = {} 
        room_fee = {}
        rakes = {}
        income = {}
        add_player = {}
        lose_player = {}
        diff_player = {}
        players_last = {}
        players = {}
        kill_rate = {}
        h5_rate = {}
        rounds = {}
        play_time = {} 
        onlinetime = {}
        game_day = {}

        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting player yesterday")
        p_last = get_last_month_players(rep_last_date, cursor)

        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting player yesterday done, time used : {t1-t2} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting daily report {rep_date} to {rep_after_date}")
        daily_report = get_daily_report(get_currency, rep_date, rep_day, cursor)
        t2= time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting daily report {rep_date} to {rep_after_date} done, time used : {t2-t1} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting game occur day")
        day_occur = get_day_occur(get_currency, rep_date, rep_day, cursor)
        t1 =time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting game occur day done, time used : {t1-t2} sec")
        
        for dic in gid_set:
            add_player[f"{dic['game_name_tw']}"] = {}
            lose_player[f"{dic['game_name_tw']}"] = {}
            diff_player[f"{dic['game_name_tw']}"] = {}
            players_last[f"{dic['game_name_tw']}"] = {}
            players[f"{dic['game_name_tw']}"] = {}
            game_day[f"{dic['game_name_tw']}"] = {}

            bets[f"{dic['game_name_tw']}"] = {}
            wins[f"{dic['game_name_tw']}"] = {}
            valid_bet[f"{dic['game_name_tw']}"] = {}
            room_fee[f"{dic['game_name_tw']}"] = {}
            rakes[f"{dic['game_name_tw']}"] = {}
            income[f"{dic['game_name_tw']}"] = {}
            kill_rate[f"{dic['game_name_tw']}"] = {}
            h5_rate[f"{dic['game_name_tw']}"] = {}
            rounds[f"{dic['game_name_tw']}"] = {}
            play_time[f"{dic['game_name_tw']}"] = {}
            onlinetime[f"{dic['game_name_tw']}"] = {}

            for cur in get_currency:
                add_player[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                lose_player[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                diff_player[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                players_last[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                players[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                game_day[f"{dic['game_name_tw']}"][f"{cur}"] = 0

                bets[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                wins[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                valid_bet[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                room_fee[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                rakes[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                income[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                kill_rate[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                h5_rate[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                rounds[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                play_time[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                onlinetime[f"{dic['game_name_tw']}"][f"{cur}"] = 0
            
                t1 = time.time()
            add_player_uid, lose_player_uid = get_add_lose_monthly(dic['gid'], dic['game_type'], rep_date, 1, rep_last_date, cursor)

            add_player[f"{dic['game_name_tw']}"]['ALL'] = len(add_player_uid)
            for ele in add_player_uid:
                if ele['currency'] in ('CNY', 'THB', 'KRW','VND', 'IDR'):
                    add_player[f"{dic['game_name_tw']}"][f"{ele['currency']}"] += 1
                    
                elif ele['currency'] in ('VND(K)', 'IDR(K)'):
                    if ele['currency'] == 'VND(K)':
                        add_player[f"{dic['game_name_tw']}"]['VND'] += 1
                    else:
                        add_player[f"{dic['game_name_tw']}"]['IDR'] += 1
                
            
            lose_player[f"{dic['game_name_tw']}"]['ALL'] = len(lose_player_uid)
            for ele in lose_player_uid:
                if ele['currency'] in ('CNY', 'THB', 'KRW','VND', 'IDR'):
                    
                    lose_player[f"{dic['game_name_tw']}"][f"{ele['currency']}"] += 1
                    
                elif ele['currency'] in ('VND(K)', 'IDR(K)'):
                    
                    if ele['currency'] == 'VND(K)':
                        lose_player[f"{dic['game_name_tw']}"]['VND'] += 1
                    else:
                        lose_player[f"{dic['game_name_tw']}"]['IDR'] += 1
                        
            t2 = time.time()
            logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting add & lose player {dic['gid']}, {dic['game_name_tw']} done, time used : {t2-t1} sec")
        
        for uid in add_player_uid:
            add_player[f"{uid['game_name_tw']}"][f"ALL"] += 1
            if uid['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                add_player[f"{uid['game_name_tw']}"][f"{uid['currency']}"]+=1
            elif uid['currency'] in ('IDR(K)','VND(K)'):
                if uid['currency'] == 'VND(K)':
                    add_player[f"{uid['game_name_tw']}"][f"VND"]+=1
                else:
                    add_player[f"{uid['game_name_tw']}"][f"IDR"] += 1
        
        for uid in lose_player_uid:
            lose_player[f"{uid['game_name_tw']}"][f"ALL"] += 1
            if uid['currency'] in ('CNY', 'KRW', 'THB', 'VND', 'IDR'):
                lose_player[f"{uid['game_name_tw']}"][f"{uid['currency']}"]+=1
            elif uid['currency'] in ('IDR(K)','VND(K)'):
                if uid['currency'] == 'VND(K)':
                    lose_player[f"{uid['game_name_tw']}"][f"VND"]+=1
                else:
                    lose_player[f"{uid['game_name_tw']}"][f"IDR"] += 1

        for pl in p_last:
            players_last[f"{pl['game_name_tw']}"][f"{pl['currency']}"] = pl['players']
        
        for do in day_occur:
            game_day[f"{do['game_name_tw']}"][f"{do['currency']}"] += 1

        for res in daily_report:
            bets[f"{res['game_name_tw']}"]['currency'] = res['bets']
            wins[f"{res['game_name_tw']}"]['currency'] = res['wins']
            valid_bet[f"{res['game_name_tw']}"]['currency'] = res['valid_bet']
            room_fee[f"{res['game_name_tw']}"]['currency'] = res['room_fee']
            rakes[f"{res['game_name_tw']}"]['currency'] = res['rakes']
            income[f"{res['game_name_tw']}"]['currency'] = res['income']
            kill_rate[f"{res['game_name_tw']}"]['currency'] = res['player_win']
            h5_rate[f"{res['game_name_tw']}"]['currency'] = res['h5_round']
            rounds[f"{res['game_name_tw']}"]['currency'] = res['rounds']
            play_time[f"{res['game_name_tw']}"]['currency'] = res['play_time']
            onlinetime[f"{res['game_name_tw']}"]['currency'] = res['onlinetime']

        for idx, cur in enumerate(get_currency):
            ans = {}
            for dic in gid_set:
                diff_player[f"{dic['game_name_tw']}"][f'{cur}'] = add_player[f"{dic['game_name_tw']}"][f'{cur}']-lose_player[f"{dic['game_name_tw']}"][f'{cur}']
                players[f"{dic['game_name_tw']}"][f'{cur}'] = diff_player[f"{dic['game_name_tw']}"][f'{cur}']+players_last[f"{dic['game_name_tw']}"][f'{cur}']
                if bets[f"{dic['game_name_tw']}"][f'{cur}'] > 0:
                    
                    ans[f"{dic['game_name_tw']}"] = {
                        '序號' : -1, '遊戲名稱' : dic['game_name_tw'], '碼量' : float(str(bets[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '吐錢' : float(str(wins[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '有效投注' : float(str(valid_bet[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '開房費' : float(str(room_fee[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '抽水錢' : float(str(rakes[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '盈利' : float(str(income[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '會員數' : float(str(players[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '新增會員數' : float(str(add_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '流失會員數' : float(str(lose_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '會員增減數' : float(str(diff_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '新增率' : float(str(add_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',',''))/float(str(players_last[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '流失率' : float(str(lose_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',',''))/float(str(players_last[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '增減率' : float(str(diff_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',',''))/float(str(players_last[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '殺率' : (1-float(str(kill_rate[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')))/float(str(bets[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '手機佔比' : float(str(h5_rate[f"{dic['game_name_tw']}"][f'{cur}']).replace(',',''))/float(str(rounds[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '場次' : float(str(rounds[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '會員留存時間(分鐘)' : float(str(play_time[f"{dic['game_name_tw']}"][f'{cur}']/game_day[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '上線日期' : str(onlinetime[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')
                    } 
                    if dict_gid == None:

                        dict_gid = ans['遊戲名稱']
                        cols = list(ans[f'{dict_gid}'].keys())
            df = pd.DataFrame.from_dict(ans, orient='index', columns = cols)

            df = df.sort_values(by = ['碼量'], ascending=False)
            print(df)

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
            dfs.append(df)

    cursor.close()
    connection.close()
    
    codee = time.time()
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : query done, time : {(codee-codes)/60} minutes")

    logger.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to import into excel')
    s = time.time()
    save_file = f"{filename[:10]}_report_by_game_daily.xlsx" if rep_type == 'daily' else f"{filename[:10]}_report_by_game_monthly.xlsx"
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