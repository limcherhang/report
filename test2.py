import pandas as pd
import mysql
import mysql.connector as connector
from openpyxl.utils import get_column_letter
import numpy as np
from time import time
import datetime

#################
# connect mysql #
#################
try:
    connection = connector.connect(user='DAxHermes_aries', password='AK4EyH7Nx96_GDwz',
                                 host='10.100.8.166')
except connector.Error as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)

cursor = connection.cursor()

cursor.execute('SELECT * FROM MaReport.game_info;')

r1 = cursor.fetchall()
print(r1)

cursor.execute('SELECT 1;')
r2 = cursor.fetchall()
print(r2)