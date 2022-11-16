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
from report_by_game_daily import get_last_month_players, get_day_occur, get_user_game, get_add_lose_daily, get_add_lose_monthly, get_user_play_time, get_last_day_players, get_h5_round

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
    "--path", default="oct_report_by_game_daily", type=str,
    help="path of all report_by_game_daily"
)
args = parser.parse_args()

def get_ans_report_by_game_daily(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor) -> list:
    query = f"""
        SELECT 
            `no`, game_name_tw, bets, wins, valid_bet, room_fee, rakes, income,
            players, add_players, lose_players, diff_players, add_rate,
            lose_rate, diff_rate, kill_rate, h5_rate, rounds, play_time, rgd.onlinetime, currency
        FROM MaReport.report_by_game_daily AS rgd 
        JOIN MaReport.game_info ON rgd.gid = game_info.gid
        WHERE `date`='{report_date}'
        ORDER BY `no`
    """
    Mycursor.execute(query)

    return Mycursor.fetchall()

def report_by_game_monthly(currency : str, report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor) -> list:
    query = f"""
        SELECT 
            game_name_tw, bets, wins, valid_bet, room_fee, rakes, income, players, add_players, lose_players, diff_players,
            add_rate, lose_rate, diff_rate, kill_rate, h5_rate, rounds, play_time
        FROM
        MaReport.report_by_game_monthly AS rep
        JOIN MaReport.game_info ON game_info.gid=rep.gid
        WHERE `date` = '{report_date}' AND currency = '{currency}'
    """
    Mycursor.execute(query)
    result = Mycursor.fetchall()

    return result

if __name__ == '__main__':
    rep_date = args.report_date
    day_type = args.report_type    
    filename = rep_date

    logger = logging.getLogger(__name__)
    logfile = f"{filename[0:10]}_report_by_game_daily_vs_ans.log" if day_type == 'daily' else f"{filename[0:10]}_report_by_game_monthly_vs_ans.log"
    try:
        os.remove(f'{filename[0:10]}_report_by_game_daily_vs_ans.log' if day_type == 'daily' else f"{filename[0:10]}_report_by_game_monthly_vs_ans.log")
    except:
        logger.warning(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot remove {filename[0:10]}_report_by_game_daily_vs_ans.log!')

    logging.basicConfig(filename=logfile, level=logging.INFO, encoding='utf-8')

    #################
    # connect mysql #
    #################
    try:
        connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                    host='10.100.8.166')
    except connector.Error as e:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Error: Could not make connection to the MySQL database")
        raise e

    if len(rep_date) != 19:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")

    if day_type != 'daily' and rep_date[8:10] != '01':
        logger.error(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot find from Mareport')
        raise ValueError
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : connecting the cursor")
    cursor = connection.cursor(buffered=True, dictionary=True)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : executing query")
    cursor.execute("SET time_zone = '+00:00';")

    # currency_query = """
    #     SELECT short_name FROM cypress.fx_rate
    #     WHERE short_name NOT IN ('IDR(K)', 'INR(0.01)', 'KHR(Moha)', 'MMK(100)', 'MMK(K)', 'MMKPI', 'USD(0.1)', 'USDT(0.1)', 'VND(K)');
    # """
    # cursor.execute(currency_query)

    get_currency = [
        'ALL',
        'CNY', 
        'IDR', 
        'KRW', 
        'THB', 
        'VND',
    ]

    get_gid_set = f"""
    SELECT 
        game_info.gid, game_type, game_code, game_name_tw
    FROM
    MaReport.game_info
    JOIN
    (
        SELECT DISTINCT gid FROM
        cypress.statistic_user_by_game
        WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL 1 DAY)
        UNION
        SELECT DISTINCT gid FROM
        cypress.statistic_user_by_lottogame
        WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL 1 DAY)
        UNION
        SELECT DISTINCT gid FROM
        cypress.statistic_user_by_tablegame
        WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL 1 DAY)
    ) AS stat
    ON game_info.gid = stat.gid
    """
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the gid set!")
    cursor.execute(get_gid_set)

    gid_set = {}
    for ele in cursor.fetchall():
        gid_set[f"{ele['gid']}"]= ele

    dfs = []
    dict_gid = 0
    times = 0
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to fetch date")
    s = time.time()
    if day_type == 'daily':
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

        bets_Ma = {}
        wins_Ma = {}
        valid_bet_Ma = {}
        room_fee_Ma = {}
        rakes_Ma = {}
        income_Ma = {}
        players_Ma = {}
        add_player_Ma = {}
        lose_player_Ma = {}
        diff_player_Ma = {}
        add_rate_Ma = {}
        lose_rate_Ma = {}
        diff_rate_Ma = {}
        kill_rate_Ma = {}
        h5_rate_Ma = {}
        rounds_Ma = {}
        play_time_Ma = {}

        qqs = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user_game")
        t1 = time.time()
        user_game = get_user_game(rep_date, cursor)
        
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user_game done, time used : {t2-t1} sec")
        
        t1 = time.time()
        
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round")
        
        h5_round = get_h5_round(rep_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting h5 round done, time used : {t2-t1} sec")
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user total play time")
        ptime = get_user_play_time(rep_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting user total play time done, time used : {t1-t2} sec")
        report_ans = get_ans_report_by_game_daily(rep_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting ans report_by_game_daily {rep_date}, time used : {t2-t1} sec")
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting add & lose player")
        add_player_uid, lose_player_uid = get_add_lose_daily(rep_date, rep_last_date, cursor)
        t1 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting add & lose player done, time used : {t1-t2} sec")

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

            bets_Ma[f"{dic['game_name_tw']}"] = {}
            wins_Ma[f"{dic['game_name_tw']}"] = {}
            valid_bet_Ma[f"{dic['game_name_tw']}"] = {}
            room_fee_Ma[f"{dic['game_name_tw']}"] = {}
            rakes_Ma[f"{dic['game_name_tw']}"] = {}
            income_Ma[f"{dic['game_name_tw']}"] = {}
            players_Ma[f"{dic['game_name_tw']}"] = {}
            add_player_Ma[f"{dic['game_name_tw']}"] = {}
            lose_player_Ma[f"{dic['game_name_tw']}"] = {}
            diff_player_Ma[f"{dic['game_name_tw']}"] = {}
            add_rate_Ma[f"{dic['game_name_tw']}"] = {}
            lose_rate_Ma[f"{dic['game_name_tw']}"] = {}
            diff_rate_Ma[f"{dic['game_name_tw']}"] = {}
            kill_rate_Ma[f"{dic['game_name_tw']}"] = {}
            h5_rate_Ma[f"{dic['game_name_tw']}"] = {}
            rounds_Ma[f"{dic['game_name_tw']}"] = {}
            play_time_Ma[f"{dic['game_name_tw']}"] = {}
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

                bets_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                wins_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                valid_bet_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                room_fee_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                rakes_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                income_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                players_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                add_player_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                lose_player_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                diff_player_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                add_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                lose_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                diff_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                kill_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                h5_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                rounds_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0
                play_time_Ma[f"{dic['game_name_tw']}"][f"{cur}"] = 0

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

        # `no`, game_name_tw, bets, wins, valid_bet, room_fee, rakes, income,
        # players, add_players, lose_players, diff_players, add_rate,
        # lose_rate, diff_rate, kill_rate, h5_rate, rounds, play_time, rgd.onlinetime, currency

        for an in report_ans:
            if an['currency'] in get_currency:
                bets_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['bets']
                wins_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['wins']
                valid_bet_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['valid_bet']
                room_fee_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['room_fee']
                rakes_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['rakes']
                income_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['income']
                players_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['players']
                add_player_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['add_players']
                lose_player_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['lose_players']
                diff_player_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['diff_players']
                add_rate_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['add_rate']
                lose_rate_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['lose_rate']
                diff_rate_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['diff_rate']
                kill_rate_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['kill_rate']
                h5_rate_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['h5_rate']
                rounds_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['rounds']
                play_time_Ma[f"{an['game_name_tw']}"][f"{an['currency']}"] += an['play_time']

        qqe = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : time used for query {(qqe-qqs)/60} minutes")
        
        for idx, cur in enumerate(get_currency):
            querys = time.time()
            ans = {}
            for dic in gid_set:
                if bets[f"{dic['game_name_tw']}"][f'{cur}'] > 0:
                    ans[f"{dic['game_name_tw']}"] = {
                        '序號':-1, '遊戲名稱':dic['game_name_tw'], 
                        '碼量':float(str(bets[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '吐錢':float(str(wins[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')), 
                        '有效投注':float(str(valid_bet[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '開房費':float(str(room_fee[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')), 
                        '抽水錢':float(str(rakes[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '盈利':float(str(income[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')), 
                        '會員數':float(str(players[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '新增會員數':float(str(add_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')), 
                        '流失會員數':float(str(lose_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '會員增減數':float(str(add_player[f"{dic['game_name_tw']}"][f'{cur}']-lose_player[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '新增率':float(str(add_player[f"{dic['game_name_tw']}"][f'{cur}']/(players[f"{dic['game_name_tw']}"][f'{cur}']-diff_player[f"{dic['game_name_tw']}"][f'{cur}'])*100).replace(',','')) if players[f"{dic['game_name_tw']}"][f'{cur}']-diff_player[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 100,
                        '流失率':float(str(lose_player[f"{dic['game_name_tw']}"][f'{cur}']/(players[f"{dic['game_name_tw']}"][f'{cur}']-diff_player[f"{dic['game_name_tw']}"][f'{cur}'])*100).replace(',','')) if players[f"{dic['game_name_tw']}"][f'{cur}']-diff_player[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 0,
                        '增減率':float(str((add_player[f"{dic['game_name_tw']}"][f'{cur}']-lose_player[f"{dic['game_name_tw']}"][f'{cur}'])/(players[f"{dic['game_name_tw']}"][f'{cur}']-diff_player[f"{dic['game_name_tw']}"][f'{cur}'])*100).replace(',','')) if players[f"{dic['game_name_tw']}"][f'{cur}']-diff_player[f"{dic['game_name_tw']}"][f'{cur}'] > 0 else 100,
                        '殺率':float(str((1-player_win[f"{dic['game_name_tw']}"][f'{cur}']/bets[f"{dic['game_name_tw']}"][f'{cur}'])*100).replace(',','')),
                        '手機佔比':float(str(h5_rate[f"{dic['game_name_tw']}"][f'{cur}']/rounds[f"{dic['game_name_tw']}"][f'{cur}']*100).replace(',','')), 
                        '場次' : float(str(rounds[f"{dic['game_name_tw']}"][f'{cur}']).replace(',','')),
                        '會員留存時間(分鐘)':0 if dic['game_type'] in ('lotto', 'sport') else float(str(play_time[f"{dic['game_name_tw']}"][f'{cur}']/uid_for_pt[f"{dic['game_name_tw']}"][f'{cur}']/60).replace(',','')),
                        '上線日期':onlinetime[f"{dic['game_name_tw']}"][f'{cur}'], 
                        '碼量(Ma)' : float(str(bets_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '吐錢(Ma)' : float(str(wins_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '有效投注(Ma)' : float(str(valid_bet_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')),
                        '開房費(Ma)' : float(str(room_fee_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '抽水錢(Ma)' : float(str(rakes_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '盈利(Ma)' : float(str(income_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')),
                        '會員數(Ma)' : float(str(players_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '新增會員數(Ma)' : float(str(add_player_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '流失會員數(Ma)' : float(str(lose_player_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')),
                        '會員增減數(Ma)' : float(str(diff_player_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '新增率(Ma)' : float(str(add_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '流失率(Ma)' : float(str(lose_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')),
                        '增減率(Ma)' : float(str(diff_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '殺率(Ma)' : float(str(kill_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '手機佔比(Ma)' : float(str(h5_rate_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')),
                        '場次(Ma)' : float(str(rounds_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',','')), 
                        '會員留存時間(分鐘)(Ma)' : float(str(play_time_Ma[f"{dic['game_name_tw']}"][f"{cur}"]).replace(',',''))
                    }
                if dict_gid == 0:
                    dict_gid = dic['game_name_tw']
                    cols = list(ans[f"{dict_gid}"].keys())

            df = pd.DataFrame.from_dict(ans, orient='index', columns=cols)

            df = df.sort_values(by = ['碼量'], ascending=False)
            df['碼量差'] = round(df['碼量'] - df['碼量(Ma)'], 2)
            df['吐錢差'] = round(df['吐錢'] - df['吐錢(Ma)'], 2)
            df['有效投注差'] = round(df['有效投注'] - df['有效投注(Ma)'], 2)
            df['開房費差'] = round(df['開房費'] - df['開房費(Ma)'], 2)
            df['抽水錢差'] = round(df['抽水錢'] - df['抽水錢(Ma)'], 2)
            df['盈利差'] = round(df['盈利'] - df['盈利(Ma)'], 2)
            df['會員數差'] = round(df['會員數'] - df['會員數(Ma)'], 2)
            df['新增會員數差'] = round(df['新增會員數'] - df['新增會員數(Ma)'], 0)
            df['流失會員數差'] = round(df['流失會員數'] - df['流失會員數(Ma)'], 0)
            df['會員增減數差'] = round(df['會員增減數'] - df['會員增減數(Ma)'], 0)
            df['新增率差'] = round(df['新增率'] - df['新增率(Ma)'], 2)
            df['流失率差'] = round(df['流失率'] - df['流失率(Ma)'], 2)
            df['增減率差'] = round(df['增減率'] - df['增減率(Ma)'], 2)
            df['殺率差'] = round(df['殺率'] - df['殺率(Ma)'], 2)
            df['手機佔比差'] = round(df['手機佔比'] - df['手機佔比(Ma)'], 2)
            df['場次差'] = round(df['場次'] - df['場次(Ma)'], 0)
            df['會員留存時間(分鐘)差'] = round(df['會員留存時間(分鐘)'] - df['會員留存時間(分鐘)(Ma)'], 2)

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
                df.loc[:, '碼量(Ma)'] = df['碼量(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '吐錢(Ma)'] = df['吐錢(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '有效投注(Ma)'] = df['有效投注(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '開房費(Ma)'] = df['開房費(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '抽水錢(Ma)'] = df['抽水錢(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '盈利(Ma)'] = df['盈利(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '會員數(Ma)'] = df['會員數(Ma)'].map('{:,.0f}'.format)
                df.loc[:, '新增會員數(Ma)'] = df['新增會員數(Ma)'].map('{:,.0f}'.format)
                df.loc[:, '流失會員數(Ma)'] = df['流失會員數(Ma)'].map('{:,.0f}'.format)
                df.loc[:, '會員增減數(Ma)'] = df['會員增減數(Ma)'].map('{:,.0f}'.format)
                df.loc[:, '新增率(Ma)'] = df['新增率(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '流失率(Ma)'] = df['流失率(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '增減率(Ma)'] = df['增減率(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '殺率(Ma)'] = df['殺率(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '手機佔比(Ma)'] = df['手機佔比(Ma)'].map('{:,.2f}'.format)
                df.loc[:, '場次(Ma)'] = df['場次(Ma)'].map('{:,.0f}'.format)
                df.loc[:, '會員留存時間(分鐘)(Ma)'] = df['會員留存時間(分鐘)(Ma)'].map('{:,.2f}'.format)
            
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

        bets_Ma = {}
        wins_Ma = {}
        valid_bet_Ma = {}
        room_fee_Ma = {}
        rakes_Ma = {}
        income_Ma = {}
        players_Ma = {}
        add_player_Ma = {}
        lose_player_Ma = {}
        diff_player_Ma = {}
        add_rate_Ma = {}
        lose_rate_Ma = {}
        diff_rate_Ma = {}
        kill_rate_Ma = {}
        h5_rate_Ma = {}
        rounds_Ma = {}
        play_time_Ma = {}

        for idx, cur in enumerate(get_currency):
            ans = {}
            if idx == 0:
                for dic in gid_set:
                    qqs = time.time()
                    add_player[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    lose_player[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    diff_player[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    players_last[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    players[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    game_day[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}

                    bets_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    wins_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    valid_bet_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    room_fee_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    rakes_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    income_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    players_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    add_player_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    lose_player_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    diff_player_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    add_rate_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    lose_rate_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    diff_rate_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    kill_rate_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    h5_rate_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    rounds_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}
                    play_time_Ma[f"{dic['game_name_tw']}"] = {'ALL' : 0, 'CNY' : 0, 'KRW' : 0, 'THB' : 0, 'VND' : 0, 'IDR' : 0}

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
                for c in get_currency:
                    result = report_by_game_monthly(c, rep_date, cursor)
                    for res in result:
                        bets_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['bets']
                        wins_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['wins']
                        valid_bet_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['valid_bet']
                        room_fee_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['room_fee']
                        rakes_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['rakes']
                        income_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['income']
                        players_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['players']
                        add_player_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['add_players']
                        lose_player_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['lose_players']
                        diff_player_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['diff_players']
                        add_rate_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['add_rate']
                        lose_rate_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['lose_rate']
                        diff_rate_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['diff_rate']
                        kill_rate_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['kill_rate']
                        h5_rate_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['h5_rate']
                        rounds_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['rounds']
                        play_time_Ma[f"{res['game_name_tw']}"][f"{c}"] = res['play_time']
                            
                    
            
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
                            '上線日期' : dic['上線日期'], 
                            '碼量(Ma)' : float(str(bets_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '吐錢(Ma)' : float(str(wins_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '有效投注(Ma)' : float(str(valid_bet_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')),
                            '開房費(Ma)' : float(str(room_fee_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '抽水錢(Ma)' : float(str(rakes_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '盈利(Ma)' : float(str(income_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')),
                            '會員數(Ma)' : float(str(players_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '新增會員數(Ma)' : float(str(add_player_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '流失會員數(Ma)' : float(str(lose_player_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')),
                            '會員增減數(Ma)' : float(str(diff_player_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '新增率(Ma)' : float(str(add_rate_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '流失率(Ma)' : float(str(lose_rate_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')),
                            '增減率(Ma)' : float(str(diff_rate_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '殺率(Ma)' : float(str(kill_rate_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '手機佔比(Ma)' : float(str(h5_rate_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')),
                            '場次(Ma)' : float(str(rounds_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',','')), 
                            '會員留存時間(分鐘)(Ma)' : float(str(play_time_Ma[f"{dic['遊戲名稱']}"][f"{cur}"]).replace(',',''))
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
            df['碼量差'] = round(df['碼量'] - df['碼量(Ma)'], 2)
            df['吐錢差'] = round(df['吐錢'] - df['吐錢(Ma)'], 2)
            df['有效投注差'] = round(df['有效投注'] - df['有效投注(Ma)'], 2)
            df['開房費差'] = round(df['開房費'] - df['開房費(Ma)'], 2)
            df['抽水錢差'] = round(df['抽水錢'] - df['抽水錢(Ma)'], 2)
            df['盈利差'] = round(df['盈利'] - df['盈利(Ma)'], 2)
            df['會員數差'] = round(df['會員數'] - df['會員數(Ma)'], 2)
            df['新增會員數差'] = round(df['新增會員數'] - df['新增會員數(Ma)'], 2)
            df['流失會員數差'] = round(df['流失會員數'] - df['流失會員數(Ma)'], 2)
            df['會員增減數差'] = round(df['會員增減數'] - df['會員增減數(Ma)'], 2)
            df['新增率差'] = round(df['新增率'] - df['新增率(Ma)'], 2)
            df['流失率差'] = round(df['流失率'] - df['流失率(Ma)'], 2)
            df['增減率差'] = round(df['增減率'] - df['增減率(Ma)'], 2)
            df['殺率差'] = round(df['殺率'] - df['殺率(Ma)'], 2)
            df['手機佔比差'] = round(df['手機佔比'] - df['手機佔比(Ma)'], 2)
            df['會員留存時間(分鐘)差'] = round(df['會員留存時間(分鐘)'] - df['會員留存時間(分鐘)(Ma)'], 2)

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
            df.loc[:, '碼量(Ma)'] = df['碼量(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '吐錢(Ma)'] = df['吐錢(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '有效投注(Ma)'] = df['有效投注(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '開房費(Ma)'] = df['開房費(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '抽水錢(Ma)'] = df['抽水錢(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '盈利(Ma)'] = df['盈利(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '新增率(Ma)'] = df['新增率(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '流失率(Ma)'] = df['流失率(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '增減率(Ma)'] = df['增減率(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '殺率(Ma)'] = df['殺率(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '手機佔比(Ma)'] = df['手機佔比(Ma)'].map('{:,.2f}'.format)
            df.loc[:, '場次(Ma)'] = df['場次(Ma)'].map('{:,.0f}'.format)
            df.loc[:, '會員留存時間(分鐘)(Ma)'] = df['會員留存時間(分鐘)(Ma)'].map('{:,.2f}'.format) 

            df['序號'] = range(1, len(df)+1)

            logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {cur} done")
            dfs.append(df)
            

    cursor.close()
    connection.close()
    logging.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : query time used(not included ans) : {times/60}')
    e = time.time()
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : query done, time : {(e-s)/60} minutes")

    logger.info(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to import into excel')
    s = time.time()
    save_file = f"{filename[:10]}_report_by_game_daily_vs_ans.xlsx" if day_type == 'daily' else f"{filename[:10]}_report_by_game_monthly_vs_ans.xlsx"
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