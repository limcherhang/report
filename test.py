import datetime
from locale import currency
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

try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                host='10.100.8.166')
except connector.Error as e:
    raise ConnectionError

cursor = connection.cursor(dictionary = True)
s = time.time()
cursor.execute("SET time_zone='+00:00'")
query = """
    SELECT rep.oid, rep.uid, user_list.currency FROM
    (
        SELECT oid, uid FROM
        (
            SELECT owner_list.id AS oid, uid FROM cypress.statistic_user_by_game AS stat
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            WHERE `date`>='2022-11-14' AND `date`<DATE_ADD('2022-11-14', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.id, uid
            UNION
            SELECT owner_list.id AS oid, uid FROM cypress.statistic_user_by_lottogame AS stat
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            WHERE `date`>='2022-11-14' AND `date`<DATE_ADD('2022-11-14', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.id, uid
            UNION
            SELECT owner_list.id AS oid, uid FROM cypress.statistic_user_by_tablegame AS stat
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            WHERE `date`>='2022-11-14' AND `date`<DATE_ADD('2022-11-14', INTERVAL 1 DAY) AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.id, uid
        ) AS stat
    ) AS rep
    LEFT JOIN
    (
        SELECT oid, uid FROM
        (
            SELECT owner_list.id AS oid, uid FROM cypress.statistic_user_by_game AS stat 
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            WHERE `date`>='2022-11-13' AND `date`<'2022-11-14' AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY  owner_list.id, uid
            UNION
            SELECT owner_list.id AS oid, uid FROM cypress.statistic_user_by_game AS stat 
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            WHERE `date`>='2022-11-13' AND `date`<'2022-11-14' AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY  owner_list.id, uid
            UNION
            SELECT owner_list.id AS oid, uid FROM cypress.statistic_user_by_game AS stat 
            JOIN cypress.user_list ON stat.uid = user_list.id
            JOIN cypress.parent_list ON parent_list.id = user_list.parentid
            JOIN cypress.parent_list AS owner_list ON owner_list.id = user_list.ownerid
            WHERE `date`>='2022-11-13' AND `date`<'2022-11-14' AND parent_list.istestss=0 AND owner_list.istestss=0
            GROUP BY owner_list.id, uid
        ) AS stat
    ) AS `last` ON rep.uid = `last`.uid AND rep.oid=`last`.oid
    JOIN cypress.user_list ON user_list.id=rep.uid
    JOIN cypress.parent_list ON parent_list.id=user_list.parentid
    JOIN cypress.parent_list AS owner_list ON owner_list.id=user_list.ownerid
    WHERE parent_list.istestss = 0 AND owner_list.istestss = 0 AND `last`.uid IS NULL
"""

cursor.execute(query)

result = cursor.fetchall()
e = time.time()
print(f'time used : {e-s} sec')
for res in result:
    print(res)

# print(f'time used : {e-s} sec')