import datetime
from locale import currency
import openpyxl
import pandas as pd
import mysql
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

q1 = """
    SELECT date, no, gid, bets, wins, valid_bet, room_fee, rakes, income, currency
    FROM 
    MaReport.report_by_game_monthly 
    WHERE `date`>='2022-09-01' AND `date`<'2022-10-01'
    ORDER BY gid,currency
"""

q2 = """
    SELECT 
        date, no, gid, SUM(bets), SUM(wins),SUM(valid_bet), SUM(room_fee), SUM(rakes), SUM(income), currency
    FROM
    MaReport.report_by_game_daily
    WHERE `date`>='2022-09-01' AND `date`<'2022-10-01'
    GROUP BY gid, currency
    ORDER BY gid
"""

q3 = f"""
    SELECT * 
    FROM 
    (   
        {q1} 
    ) AS monthly
    JOIN
    (
        {q2}
    ) AS daily
    ON monthly.gid = daily.gid AND monthly.currency=daily.currency
"""
cursor = connection.cursor()
cursor.execute("SET time_zone = '+00:00';")

cursor.execute(q3)

result = cursor.fetchall()

ans = []

for res in result:
    ans.append([r for r in res])

df = pd.DataFrame(ans, columns=[
    "日期(月)","排名(月)","gid(月)","總碼量(月)","總吐錢(月)","總有效碼量(月)","總房費(月)","抽水錢(月)","收入(月)","幣別",
    "日期(日)","排名(日)","gid(日)","總碼量(日)","總吐錢(日)","總有效碼量(日)","總房費(日)","抽水錢(日)","收入(日)","幣別"
])

df['碼量差'] = df["總碼量(月)"]-df["總碼量(日)"]
df['吐錢差'] = df["總吐錢(月)"]-df["總吐錢(日)"]
df['有效碼量差'] = df["總有效碼量(月)"]-df["總有效碼量(日)"]
df['房費差'] = df["總房費(月)"]-df["總房費(日)"]
df['抽水差'] = df["抽水錢(月)"]-df["抽水錢(日)"]
df['收入差'] = df["收入(月)"]-df["收入(日)"]

df.to_excel('test.xlsx')