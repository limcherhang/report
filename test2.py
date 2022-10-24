import datetime
import openpyxl
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

currency = ['ALL', 'CNY', 'THB', 'KRW', 'VND']

cursor = connection.cursor()

cursor.execute("SET time_zone = '+00:00';")

query = f"""
    SELECT 
        SUM(total_bet) AS total_bet, SUM(total_round) AS total_round, 1-SUM(total_bet-total_win)/SUM(total_bet)
    FROM
    (
        SELECT 
            total_bet/fx_rate.rate AS total_bet, total_round, total_win/fx_rate.rate AS total_win
        FROM
        cypress.statistic_user_by_game AS stat
        JOIN MaReport.game_info ON game_info.gid=stat.gid
        JOIN cypress.user_list ON stat.uid = user_list.id
        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
        WHERE `date` >= '2022-09-30 00:00:00' AND `date` < '2022-10-01 00:00:00' AND 
        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = 1 AND user_list.currency='VND' 
        UNION
        SELECT 
            total_bet/fx_rate.rate AS total_bet, total_round, total_win/fx_rate.rate AS total_win
        FROM
        cypress.statistic_user_by_game AS stat
        JOIN MaReport.game_info ON game_info.gid=stat.gid
        JOIN cypress.user_list ON stat.uid = user_list.id
        JOIN cypress.parent_list ON parent_list.id = user_list.parentid
        JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
        JOIN cypress.fx_rate ON user_list.currency = fx_rate.short_name
        WHERE `date` >= '2022-09-30 00:00:00' AND `date` < '2022-10-01 00:00:00' AND 
        parent_list.istestss = 0 AND owner_list.istestss = 0 AND stat.gid = 1 AND user_list.currency='VND(K)'
    ) AS tb
"""

cursor.execute(query)
result = cursor.fetchone()

print(result)