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
from report_by_owner_daily import get_user_owner, get_last_day_players, get_h5_round, get_user_play_time, get_add_lose_daily

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

def get_ans_report_by_owner_daily(report_date : str, Mycursor : mysql.connector.cursor.MySQLCursor):

    query = f"""
        SELECT 
            owner_info.id AS `owner`, bets, wins, valid_bet, room_fee, rakes, income,
            players, add_players, lose_players, diff_players, add_rate, 
            lose_rate, diff_rate, kill_rate, h5_rate, rounds, play_time, currency
        FROM
        MaReport.report_by_owner_daily AS rep
        JOIN MaReport.owner_info ON owner_info.`owner` = rep.`owner`
        WHERE `date`='{report_date}' AND currency IN ('ALL', 'CNY', 'IDR', 'VND', 'THB', 'KRW')
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
    logfile = f"./{log_folder}/{filename[:10]}_report_by_owner_daily_vs_ans.log" if rep_type == 'daily' else f"./{log_folder}/{filename[:10]}_report_by_owner_monthly_vs_ans.log"
    try:
        os.remove(logfile)
    except:
        logger.warning(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot remove {logfile}!')

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
            if cur['display_currency'] in ('ALL', 'CNY', 'KRW', 'THB', 'VND', 'IDR'):
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

        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting answer from MaReport")
        ans_Ma = get_ans_report_by_owner_daily(rep_date, cursor)
        t2 = time.time()
        logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : getting answer from MaReport done, time used : {t2-t1} sec")

        ans = {}
        tmp = {}
        for cur in get_currency:
            ans[f'{cur}'] = {}
            tmp[f'{cur}'] = {}
            for dic in owner_set: # {`account`:, currency:, onlinetime:}
                ans[f'{cur}'][f"{dic['account']}"] = {}
                tmp[f'{cur}'][f"{dic['account']}"] = {}
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = -1
                ans[f'{cur}'][f"{dic['account']}"]['??????????????????'] = dic['owner']
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['????????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['???????????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['???????????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['???????????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['????????????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????????????????(??????)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['????????????'] = dic['onlinetime']
                tmp[f'{cur}'][f"{dic['account']}"]['???????????????'] = 0
                tmp[f"{cur}"][f"{dic['account']}"]['???????????????'] = 0

                ans[f'{cur}'][f"{dic['account']}"]['??????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['????????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['???????????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['???????????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['???????????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['????????????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????(Ma)'] = 0
                ans[f'{cur}'][f"{dic['account']}"]['??????????????????(??????)(Ma)'] = 0

                if dict_owner == 0:
                    dict_owner = 1
                    cols = ans[f'{cur}'][f"{dic['account']}"].keys()

        for user in user_owner:
            ans[f"ALL"][f"{user['account']}"]['??????'] += float(str(user['total_bet']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['??????'] += float(str(user['total_win']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['????????????'] += float(str(user['valid_bet']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['?????????'] += float(str(user['room_fee']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['?????????'] += float(str(user['total_rake']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['??????'] += float(str(user['income']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['?????????'] += float(str(user['players']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['??????'] += float(str(user['player_win']).replace(',',''))
            ans[f"ALL"][f"{user['account']}"]['??????'] += float(str(user['total_round']).replace(',',''))
        
            for cur in get_currency:
                if user['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{user['account']}"]['??????'] += float(str(user['total_bet']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['??????'] += float(str(user['total_win']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['????????????'] += float(str(user['valid_bet']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['?????????'] += float(str(user['room_fee']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['?????????'] += float(str(user['total_rake']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['??????'] += float(str(user['income']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['?????????'] += float(str(user['players']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['??????'] += float(str(user['player_win']).replace(',',''))
                    ans[f"{cur}"][f"{user['account']}"]['??????'] += float(str(user['total_round']).replace(',',''))
                    break

#       rep.`account`, rep.uid, rep.currency

        for uid in add_player_uid:
            ans[f"ALL"][f"{uid['account']}"]['???????????????'] += 1
            
            for cur in get_currency:
                if uid['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{uid['account']}"]['???????????????'] += 1
                    break

        for uid in lose_player_uid:
            ans[f"ALL"][f"{uid['account']}"]['???????????????'] += 1
        
            for cur in get_currency:
                if uid['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{uid['account']}"]['???????????????'] += 1
                    break
        for pl in player_last_uid:
            tmp[f'ALL'][f"{pl['account']}"]['???????????????'] += 1
            for cur in get_currency:
                if pl['currency'] in get_currency[cur]:
                    tmp[f'{cur}'][f"{pl['account']}"]['???????????????'] += 1
                    break

        for h5 in h5_round:
            ans[f"ALL"][f"{h5['account']}"]['????????????'] += float(str(h5['h5_round']).replace(',',''))
            for cur in get_currency:
                if h5['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{h5['account']}"]['????????????'] += float(str(h5['h5_round']).replace(',',''))
                    break

        for pt in ptime:
            ans[f"ALL"][f"{pt['account']}"]['??????????????????(??????)'] += float(str(pt['play_time']).replace(',',''))
            tmp[f"ALL"][f"{pt['account']}"]['???????????????'] += 1
            for cur in get_currency:
                if pt['currency'] in get_currency[cur]:
                    ans[f"{cur}"][f"{pt['account']}"]['??????????????????(??????)'] += float(str(pt['play_time']).replace(',',''))
                    tmp[f"{cur}"][f"{pt['account']}"]['???????????????'] += 1
        
        for an in ans_Ma:
            ans[f"{an['currency']}"][f"{an['owner']}"]['??????(Ma)'] = an['bets']
            ans[f"{an['currency']}"][f"{an['owner']}"]['??????(Ma)'] = an['wins']
            ans[f"{an['currency']}"][f"{an['owner']}"]['????????????(Ma)'] = an['valid_bet']
            ans[f"{an['currency']}"][f"{an['owner']}"]['?????????(Ma)'] = an['room_fee']
            ans[f"{an['currency']}"][f"{an['owner']}"]['?????????(Ma)'] = an['rakes']
            ans[f"{an['currency']}"][f"{an['owner']}"]['??????(Ma)'] = an['income']
            ans[f"{an['currency']}"][f"{an['owner']}"]['?????????(Ma)'] = an['players']
            ans[f"{an['currency']}"][f"{an['owner']}"]['???????????????(Ma)'] = an['add_players']
            ans[f"{an['currency']}"][f"{an['owner']}"]['???????????????(Ma)'] = an['lose_players']
            ans[f"{an['currency']}"][f"{an['owner']}"]['???????????????(Ma)'] = an['diff_players']
            ans[f"{an['currency']}"][f"{an['owner']}"]['?????????(Ma)'] = an['add_rate']
            ans[f"{an['currency']}"][f"{an['owner']}"]['?????????(Ma)'] = an['lose_rate']
            ans[f"{an['currency']}"][f"{an['owner']}"]['?????????(Ma)'] = an['diff_rate']
            ans[f"{an['currency']}"][f"{an['owner']}"]['??????(Ma)'] = an['kill_rate']
            ans[f"{an['currency']}"][f"{an['owner']}"]['????????????(Ma)'] = an['h5_rate']
            ans[f"{an['currency']}"][f"{an['owner']}"]['??????(Ma)'] = an['rounds']
            ans[f"{an['currency']}"][f"{an['owner']}"]['??????????????????(??????)(Ma)'] = an['play_time']

        for cur in get_currency:
            for dic in owner_set:
                ans[f'{cur}'][f"{dic['account']}"]['???????????????'] = ans[f'{cur}'][f"{dic['account']}"]['???????????????'] - ans[f'{cur}'][f"{dic['account']}"]['???????????????']
                # tmp[f'{cur}'][f"{dic['account']}"]['???????????????'] = ans[f'{cur}'][f"{dic['account']}"]['?????????']-ans[f'{cur}'][f"{dic['account']}"]['???????????????']
                ans[f'{cur}'][f"{dic['account']}"]['??????????????????(??????)'] = ans[f'{cur}'][f"{dic['account']}"]['??????????????????(??????)']/tmp[f'{cur}'][f"{dic['account']}"]['???????????????']/60 if tmp[f'{cur}'][f"{dic['account']}"]['???????????????']> 0 else 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = ans[f'{cur}'][f"{dic['account']}"]['???????????????']/tmp[f'{cur}'][f"{dic['account']}"]['???????????????']*100 if tmp[f'{cur}'][f"{dic['account']}"]['???????????????']>0 else 100
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = ans[f'{cur}'][f"{dic['account']}"]['???????????????']/tmp[f'{cur}'][f"{dic['account']}"]['???????????????']*100 if tmp[f'{cur}'][f"{dic['account']}"]['???????????????']>0 else 0
                ans[f'{cur}'][f"{dic['account']}"]['?????????'] = ans[f'{cur}'][f"{dic['account']}"]['???????????????']/tmp[f'{cur}'][f"{dic['account']}"]['???????????????']*100 if tmp[f'{cur}'][f"{dic['account']}"]['???????????????']>0 else 100
                ans[f'{cur}'][f"{dic['account']}"]['??????'] = (1-ans[f'{cur}'][f"{dic['account']}"]['??????']/ans[f'{cur}'][f"{dic['account']}"]['??????'])*100 if ans[f'{cur}'][f"{dic['account']}"]['??????']*100 >0 else 0 
                ans[f'{cur}'][f"{dic['account']}"]['????????????'] = ans[f'{cur}'][f"{dic['account']}"]['????????????']/ans[f'{cur}'][f"{dic['account']}"]['??????']*100 if ans[f'{cur}'][f"{dic['account']}"]['??????'] > 0 else 0
                
        for cur in get_currency:
            df = pd.DataFrame.from_dict(ans[cur], orient='index', columns=cols)
            df = df[df['??????????????????(??????)'] > 0]
            df = df.sort_values(by = ['??????'], ascending=False)
            print(df)
            df['?????????'] = round(df['??????'], 2) - df['??????(Ma)']
            df['?????????'] = round(df['??????'], 2) - df['??????(Ma)']
            df['???????????????'] = round(df['????????????'], 2) - df['????????????(Ma)']
            df['????????????'] = round(df['?????????'], 2) - df['?????????(Ma)']
            df['????????????'] = round(df['?????????'], 2) - df['?????????(Ma)']
            df['?????????'] = round(df['??????'], 2) - df['??????(Ma)']
            df['????????????'] = round(df['?????????'], 2) - df['?????????(Ma)']
            df['??????????????????'] = round(df['???????????????'], 2) - df['???????????????(Ma)']
            df['??????????????????'] = round(df['???????????????'], 2) - df['???????????????(Ma)']
            df['??????????????????'] = round(df['???????????????'], 2) - df['???????????????(Ma)']
            df['????????????'] = round(df['?????????'], 2) - df['?????????(Ma)']
            df['????????????'] = round(df['?????????'], 2) - df['?????????(Ma)']
            df['????????????'] = round(df['?????????'], 2) - df['?????????(Ma)']
            df['?????????'] = round(df['??????'], 2) - df['??????(Ma)']
            df['???????????????'] = round(df['????????????'], 2) - df['????????????(Ma)']
            df['??????????????????(??????)???'] = round(df['??????????????????(??????)'], 2) - df['??????????????????(??????)(Ma)']
            if len(ans[f"{cur}"]) > 0:
                df.loc[:, '??????'] = df['??????'].map('{:,.2f}'.format)
                df.loc[:, '??????'] = df['??????'].map('{:,.2f}'.format)
                df.loc[:, '????????????'] = df['????????????'].map('{:,.2f}'.format)
                df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
                df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
                df.loc[:, '??????'] = df['??????'].map('{:,.2f}'.format)
                df.loc[:, '?????????'] = df['?????????'].map('{:,.0f}'.format)
                df.loc[:, '???????????????'] = df['???????????????'].map('{:,.0f}'.format)
                df.loc[:, '???????????????'] = df['???????????????'].map('{:,.0f}'.format)
                df.loc[:, '???????????????'] = df['???????????????'].map('{:,.0f}'.format)
                df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
                df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
                df.loc[:, '?????????'] = df['?????????'].map('{:,.2f}'.format)
                df.loc[:, '??????'] = df['??????'].map('{:,.2f}'.format)
                df.loc[:, '????????????'] = df['????????????'].map('{:,.2f}'.format)
                df.loc[:, '??????'] = df['??????'].map('{:,.0f}'.format)
                df.loc[:, '??????????????????(??????)'] = df['??????????????????(??????)'].map('{:,.2f}'.format)
                
            
            
            df['??????'] = range(1, len(df)+1)

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
    save_file = f"{filename[:10]}_report_by_owner_daily_vs_ans.xlsx" if rep_type == 'daily' else f"{filename[:10]}_report_by_owner_monthly_vs_ans.xlsx"
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