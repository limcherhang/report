import datetime
import xlsxwriter
import pandas as pd
import mysql
import mysql.connector as connector
import pymongo
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
    "--report_hour", default=24, type = int, 
    help="hour; format: 1 or 2 or ... 24"
)
args = parser.parse_args()

def report_rounds_hourly(ssid : str, game_code : str, game_type : str, report_time, collec : pymongo.collection.Collection, collec2 : pymongo.collection.Collection):
    report_time = datetime.datetime.strptime(report_time, "%Y-%m-%d %H:%M:%S")
    if game_type not in ("lotto", "sport"):
        query = collec.find({
            "gamecode":game_code, "parentid":ssid, "createtime":{"$gte":report_time, "$lt":report_time+datetime.timedelta(hours=1)}
        })
    else:
        query = collec2.find({
            "gamecode":game_code, "parentid":ssid, "createtime":{"$gte":report_time, "$lt":report_time+datetime.timedelta(hours=1)}
        })

    result = {}
    total_round = 0
    h5_round = 0
    pc_round = 0
    for i, ele in enumerate(query):
        total_round+=1
        if ele['platform'] == "mobile":
            h5_round += 1
        else:
            pc_round += 1
        result[f'{i}']['total_round'] = total_round
        result[f'{i}']['h5_round'] = h5_round
        result[f'{i}']['pc_round'] = pc_round
    return result # [total_round, h5_round, pc_round]

if __name__ == '__main__':
    rep_date = args.report_date
    hours = args.report_hour
    filename = rep_date

    logger = logging.getLogger(__name__)
    logfile = f"{filename[0:10]}_report_rounds_hourly_or_daily.log"
    try:
        os.remove(f'{logfile}')
    except:
        logger.warning(f'{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : cannot remove {logfile}!')
    
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

    ###################
    # connect mongodb #
    ###################
    try:
        myclient = pymongo.MongoClient(
                    host=["10.100.8.87","10.100.8.88","10.100.8.89"],
                    port=27017,
                    username="DAxHermes_aries",
                    password="AK4EyH7Nx96_GDwz",
                    authSource="admin",
                    connect=True,
                    serverSelectionTimeoutMS=3000,
                    replicaSet="pro_da_ana_rs",
                    read_preference=pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED,
                    w=1,
                )
        db = myclient.order
        collec = db.order
        collec2 = db.orderlotto
  
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Error: Could not make connection to the Mongo database")
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {e}")

    if len(rep_date)!=19:
        logger.error(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")

    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : connecting the cursor")
    cursor = connection.cursor(buffered=True, dictionary=True)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : executing query")
    cursor.execute("SET time_zone = '+00:00';")

    get_gid_set = get_gid_set = f"""
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
        WHERE `date`>='{rep_date}' AND `updated_time`<DATE_ADD('{rep_date}', INTERVAL 25 HOUR)
        UNION
        SELECT DISTINCT gid FROM
        cypress.statistic_user_by_tablegame
        WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL 1 DAY)
    ) AS stat
    ON game_info.gid = stat.gid
    """

    get_parent_id = f"""
        SELECT DISTINCT id AS pid, ssid FROM
        cypress.parent_list
        JOIN
        (
            SELECT DISTINCT pid FROM
            cypress.statistic_parent_by_game
            WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL 1 DAY)
            UNION
            SELECT DISTINCT pid FROM
            cypress.statistic_parent_by_lottogame
            WHERE `date`>='{rep_date}' AND `updated_time`<DATE_ADD('{rep_date}', INTERVAL 25 HOUR)
            UNION
            SELECT DISTINCT pid FROM
            cypress.statistic_parent_by_tablegame
            WHERE `date`>='{rep_date}' AND `date`<DATE_ADD('{rep_date}', INTERVAL 1 DAY)
        ) AS stat
        ON stat.pid=parent_list.id
    """

    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : Start to fetch all the gid set!")
    cursor.execute(get_gid_set)

    gid_set = {}
    for ele in cursor.fetchall():
        gid_set[f"{ele['gid']}"] = ele

    cursor.execute(get_parent_id)
    parent_set = {}
    for ele in cursor.fetchall():
        parent_set[f"{ele['pid']}"] = ele

    dfs = []
    dict_gid = 0
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : prepare to fetch date")
    s = time.time()
    for _ in range(hours):

        for idx, keys in enumerate(gid_set):
            ans = {}
            # print(gid_set[keys]['gid'], gid_set[keys]['game_code'], gid_set[keys]['game_type'], gid_set[keys]['game_name_tw'])#gid_set[idx][gid], game_type, game_code, game_name_tw)
            for idx1, keyss in enumerate(parent_set):
                # print(parent_set[keys]['pid'], parent_set[keys]['ssid'])
                result = report_rounds_hourly(parent_set[keyss]['ssid'], gid_set[keys]['game_code'], gid_set[keys]['game_type'], rep_date, collec, collec2)# [total_round, h5_round, pc_round]

                ans[f"{gid_set[keys]['gid']} {gid_set[keys]['gid']}"] = {
                    "date" : rep_date, "pid" : parent_set[keys]['pid'], "gid" : gid_set[keys]['gid'],
                    "total_round" : result['total_round'], "h5_round" : result['h5_round'], "pc_round" : result['pc_round']
                }

                if dict_gid == 0:
                    dict_gid = f"{gid_set[keys]['gid']} {gid_set[keys]['gid']}"
                    cols = list(ans[f"{dict_gid}"])
        df = pd.DataFrame.from_dict(ans, orient='index', colmns = cols)
        df.loc[:, 'total_round'] = df['total_round'].map('{:, .0f}'.format)
        df.loc[:, 'h5_round'] = df['h5_round'].map('{:, .0f}'.format)
        df.loc[:, 'pc_round'] = df['pc_round'].map('{:, .0f}'.format)

        dfs.append(df)

        rep_date = str(datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=1))

    save_file = f"{filename}_report_rounds_hourly.xlsx"
    with pd.ExcelWriter(save_file, engine = 'xlsxwriter') as writer:
        for h, df in zip(hours, dfs):
            df.to_excel(writer, sheet_name=f'{h}', index = False)
            worksheet = writer.sheet[f'{h}']

            for idx, col in enumerate(df):
                series = df[col]
                max_len = max(
                    series.astype(str).map(len).max(),
                    len(str(series.name))
                )+5
                worksheet.set_column(idx,idx,max_len)
    
    e = time.time()
    print("time used : ", (e-s)/60)
    logger.info(f"{str(datetime.datetime.now().astimezone(datetime.timezone(datetime.timedelta(hours=8))))} : {save_file} import complete, time : {e-s} sec")