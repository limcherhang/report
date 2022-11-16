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
    "--path", default=None, type=str,
    help="path of all report_by_game_daily"
)
args = parser.parse_args()

def get_user_game(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):
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

def get_add_lose(gid : int , game_type : str, report_date : str, report_day : int, report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor) -> list:

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

def get_last_day_players(report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):

    query = f"""
        SELECT game_name_tw, players, currency 
        FROM MaReport.report_by_game_daily AS rep
        JOIN MaReport.game_info ON game_info.gid=rep.gid
        WHERE `date` = '{report_last_date}'
    """

    Mycursor.execute(query)
    result = Mycursor.fetchall()

    return result

def get_last_month_players(currency : str, gid : int, report_last_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):

    get_players = f"""
        SELECT players FROM MaReport.report_by_game_monthly
        WHERE `date`='{report_last_date}' AND currency='{currency}' AND gid = {gid}
    """
    Mycursor.execute(get_players)
    result = Mycursor.fetchone()

    players = result['players'] if result != None else 0

    return players

def get_day_occur(currency : str, game_name_tw : str, report_date : str, report_day : int, Mycursor : mysql.connector.cursor.MySQLCursor):
    query = f"""
        SELECT * FROM MaReport.report_by_game_daily AS rep
        JOIN MaReport.game_info ON rep.gid=game_info.gid
        WHERE `date`>= '{report_date}' AND `date`<DATE_ADD('{report_date}', INTERVAL {report_day} DAY) 
        AND currency ='{currency}' AND game_name_tw = '{game_name_tw}'
    """
    Mycursor.execute(query)
    day = len(Mycursor.fetchall())

    return day


if __name__ == '__main__':
    total_s = time.time()
    rep_date = args.report_date
    rep_type = args.report_type
    filename = rep_date
    log_folder = "log"
    try:
        os.mkdir(log_folder)
    except FileExistsError:
        pass
    logger = logging.getLogger(__name__)    
    logfile = f"./{log_folder}/{filename[0:10]}_report_by_game_daily.log" if rep_type == "daily" else f"./{log_folder}/{filename[0:10]}_report_by_game_monthly.log"
    try:
        if rep_type == 'daily':
            os.remove(f'./{log_folder}/{filename[0:10]}_report_by_game_daily.log')
        else:
            os.remove(f'./{log_folder}/{filename[0:10]}_report_by_game_monthly.log')
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

    if rep_type == 'monthly':
        if rep_date[8:10] != '01':
            rep_date[8:10] = '01'
            logger.warning(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : your report date is not in the first day of the month, we force change it!")

    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : connecting the cursor")
    cursor = connection.cursor(buffered=True, dictionary=True)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : executing query")
    cursor.execute("SET time_zone = '+00:00';")

    # currency_query = """
    #     SELECT short_name FROM cypress.fx_rate
    #     WHERE short_name NOT IN ('IDR(K)', 'INR(0.01)', 'KHR(Moha)', 'MMK(100)', 'MMK(K)', 'MMKPI', 'USD(0.1)', 'USDT(0.1)', 'VND(K)');
    # """
    # cursor.execute(currency_query)

    get_currency = ['ALL' ,
        'CNY', 
        'KRW', 
        'THB', 
        'VND',
        'IDR'
    ]
    if rep_type == 'daily':
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
        players_last = {}
        uid_for_pt = {}

        qqs = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user_game")
        t1 = time.time()
        user_game = get_user_game(rep_date, cursor)
        
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user_game done, time used : {t2-t1} sec")

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting players yesterday")
        p_last = get_last_day_players(rep_last_date, cursor)
        
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting players yesterday, time used : {t1-t2} sec")
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round")
        
        h5_round = get_h5_round(rep_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round done, time.used : {t2-t1} sec")
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user total play time")
        ptime = get_user_play_time(rep_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user total play time done , time used : {t1-t2} sec")
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
            players_last[f"{dic['game_name_tw']}"] = {}
            uid_for_pt[f"{dic['game_name_tw']}"] = {}
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
                players_last[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                uid_for_pt[f"{dic['game_name_tw']}"][f"{cur}"] = 0
            
            add_player_uid, lose_player_uid = get_add_lose(dic['gid'], dic['game_type'], rep_date, 1, rep_last_date, cursor)

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
        for pl in p_last:
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
                        '新增率':add_player[f"{dic['game_name_tw']}"][f'{cur}']/players_last[f"{dic['game_name_tw']}"][f'{cur}']*100 if players_last[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 100,
                        '流失率':lose_player[f"{dic['game_name_tw']}"][f'{cur}']/players_last[f"{dic['game_name_tw']}"][f'{cur}']*100 if players_last[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 0,
                        '增減率':(add_player[f"{dic['game_name_tw']}"][f'{cur}']-lose_player[f"{dic['game_name_tw']}"][f'{cur}'])/players_last[f"{dic['game_name_tw']}"][f'{cur}']*100 if players_last[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 100,
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
            print(df)
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
        get_gid_set = """
            SELECT 
                gid, game_name_tw, game_type
            FROM
            MaReport.game_info
        """
        cursor.execute(get_gid_set)
        gid_set = cursor.fetchall()
        dfs = []
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the gid set!")
        # cursor.execute(get_gid_set)

        # gid_set = {}
        # for ele in cursor.fetchall():
            # gid_set[f"{ele['gid']}"] = ele
        
        path_of_all_daily = args.path
        try:
            path = os.listdir(path_of_all_daily)
        except:
            logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : path not found")

        dict_gid = None
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to fetch date")
        s = time.time()
        add_player = {}
        lose_player = {}
        diff_player = {}
        players_last = {}
        players = {}
        game_day = {}

        for dic in gid_set:
            qqs = time.time()
            add_player[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
            lose_player[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
            diff_player[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
            players_last[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
            players[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
            game_day[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}

            add_player_uid, lose_player_uid = get_add_lose(dic['gid'], dic['game_type'], rep_date, rep_day, rep_last_date, cursor)

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
                    
            # print(dic['game_name_tw'], "add_player", add_player)
            # print(dic['game_name_tw'], "lose_player", lose_player)
            for c in get_currency:
                diff_player[f"{dic['game_name_tw']}"][f"{c}"] = add_player[f"{dic['game_name_tw']}"][f"{c}"] - lose_player[f"{dic['game_name_tw']}"][f"{c}"]
                players_last[f"{dic['game_name_tw']}"][f"{c}"] = get_last_month_players(c, dic['gid'], rep_last_date, cursor)# 想辦法改善
                players[f"{dic['game_name_tw']}"][f"{c}"] = diff_player[f"{dic['game_name_tw']}"][f"{c}"] + players_last[f"{dic['game_name_tw']}"][f"{c}"]
                game_day[f"{dic['game_name_tw']}"][f"{c}"] = get_day_occur(c, dic['game_name_tw'], rep_date, rep_day, cursor)# 想辦法改善
            # print(dic['game_name_tw'], "diff_player", diff_player)
            # print(dic['game_name_tw'], "players_last", players_last)
            # print(dic['game_name_tw'], "players", players)
            qqe = time.time()
            logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {dic['gid']} {dic['game_name_tw']} add&lose&diff&players used time : {(qqe-qqs)} sec")
    

        for idx, cur in enumerate(get_currency):
            ans = {}
            for idx2, file in enumerate(path):
                logger.info(f"loading {path_of_all_daily}/{file}, currency={cur}")
                qs = time.time()
                df_read = pd.read_excel(path_of_all_daily+"/"+file, sheet_name=cur)
                dict_df = df_read.to_dict('records')
                for dic in dict_df:
                    try:
                        ans[f"{dic['遊戲名稱']}"]['碼量'] += float(str(dic['碼量']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['吐錢'] += float(str(dic['吐錢']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['有效投注'] += float(str(dic['有效投注']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['開房費'] += float(str(dic['開房費']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['抽水錢'] += float(str(dic['抽水錢']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['盈利'] += float(str(dic['盈利']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['殺率'] += (1-float(str(dic['殺率']).replace(",",""))/100)*float(str(dic['碼量']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['手機佔比'] += float(str(dic['手機佔比']).replace(",",""))*float(str(dic['場次']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['場次'] += float(str(dic['場次']).replace(",",""))
                        ans[f"{dic['遊戲名稱']}"]['會員留存時間(分鐘)'] += \
                            float(str(dic['會員留存時間(分鐘)']).replace(",",""))/game_day[f"{dic['遊戲名稱']}"][f"{cur}"]
                            
                    except KeyError:
                        ans[f"{dic['遊戲名稱']}"] = {
                            '序號':-1, '遊戲名稱' : dic['遊戲名稱'], '碼量' : float(str(dic['碼量']).replace(",","")), '吐錢' : float(str(dic['吐錢']).replace(",","")),
                            '有效投注' : float(str(dic['有效投注']).replace(",","")), '開房費' : float(str(dic['開房費']).replace(",","")), 
                            '抽水錢' : float(str(dic['抽水錢']).replace(",","")), '盈利' : float(str(dic['盈利']).replace(",","")), 
                            '會員數' : float(str(players[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(",","")),
                            '新增會員數' : float(str(add_player[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '流失會員數' : float(str(lose_player[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '會員增減數' : float(str(diff_player[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')),
                            '新增率' : float(str(add_player[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))/float(str(players_last[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))*100 if players_last[f"{dic['遊戲名稱']}"][f"{cur}"] > 0 else 100, 
                            '流失率' : float(str(lose_player[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))/float(str(players_last[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))*100 if players_last[f"{dic['遊戲名稱']}"][f"{cur}"] > 0 else 0,
                            '增減率' : float(str(diff_player[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))/float(str(players_last[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))*100 if players_last[f"{dic['遊戲名稱']}"][f"{cur}"] > 0 else 100, 
                            '殺率' : (1-float(str(dic['殺率']).replace(",",""))/100)*float(str(dic['碼量']).replace(",","")),
                            '手機佔比' : float(str(dic['手機佔比']).replace(",",""))*float(str(dic['場次']).replace(",","")), '場次' : float(str(dic['場次']).replace(",","")),
                            '會員留存時間(分鐘)' : float(str(dic['會員留存時間(分鐘)']).replace(",",""))/game_day[f"{dic['遊戲名稱']}"][f"{cur}"], 
                            '上線日期' : dic['上線日期']
                        }
                    # print(game_day[f"{dic['遊戲名稱']}"][f'{cur}'], ans[f"{dic['遊戲名稱']}"]['會員留存時間(分鐘)'])
                    if dict_gid == None:
                        dict_gid = dic['遊戲名稱']
                        cols = list(ans[f'{dict_gid}'].keys())
                qe = time.time()
                logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {path_of_all_daily}/{file}, currency={cur} done, time used : {(qe-qs)}")
            
            # q = f"""
            #     SELECT COUNT(userid) AS p
            #     FROM MaReport.user_gametoken_log
            #     WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL {rep_day} DAY)
            # """
            # cursor.execute(q)
            # p = cursor.fetchone()['p']
            df = pd.DataFrame.from_dict(ans, orient='index', columns=cols)

            df['殺率'] = (1-df['殺率']/df['碼量'])*100
            df['手機佔比'] = df['手機佔比']/df['場次']
            df['盈利'] = df['盈利'] - df['抽水錢']
            # df['會員留存時間(分鐘)'] = df['會員留存時間(分鐘)']/game_day[f"{df['遊戲名稱']}"][f"{cur}"]

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
            # df.loc[:, 'bets_first'] = df['bets_first'].map('{:,.2f}'.format)
            # df.loc[:, 'players_first'] = df['players_first'].map('{:,.0f}'.format)
            # df.loc[:, 'play_time_first'] = df['play_time_first'].map('{:,.2f}'.format)
            # df.loc[:, 'play_time_nonfirst'] = df['play_time_nonfirst'].map('{:,.2f}'.format)

            df['序號'] = range(1, len(df)+1)

            logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {cur} done")
            dfs.append(df)

    cursor.close()
    connection.close()
    e = time.time()
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : query done, time : {(e-s)/60} minutes")

    logger.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to import into excel')
    s = time.time()
    save_file = f"{filename[:10]}_report_by_game_daily.xlsx" if rep_type == 'daily' else f"./{filename[:10]}_report_by_game_monthly.xlsx"
    with pd.ExcelWriter(save_file,engine='xlsxwriter') as writer:
        for cur, df in zip(get_currency, dfs):
            df.to_excel(writer,sheet_name=f'{cur}', index=False)
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

    total_e = time.time()
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : code time used : {(total_e-total_s)/60} minutes")