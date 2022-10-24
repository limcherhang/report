import pymongo
import datetime
import time
import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument(
    '--report_date', default='2022-09-01 00:00:00', type = str,
    help="date you want to report: format: 'YYYYY-mm-dd HH:MM:SS'"
)
# parser.add_argument(
#     "--report_day", default=1, type = int, 
#     help="day; format: 1 or 2 or ... or 31"
# )
args = parser.parse_args()

rep_date = args.report_date
if len(rep_date) != 19:
    print("date doesn't match format, Please quit this processing and check date format as 'xxxx-xx-xx xx:xx:xx'")

###################
# connect mongodb #
###################
username = "DAxHermes_aries"
passw = "AK4EyH7Nx96_GDwz"
host = ["10.100.8.87","10.100.8.88","10.100.8.89"]
port = "27017"
replicaset = "pro_da_ana_rs"
authentication_source = "admin"

uri = f"mongodb://{username}:{passw}@"
uri += f":{port},".join(host)
uri += f"/?replicaSet={replicaset}&authSource={authentication_source}"
# print(uri)
myclient = pymongo.MongoClient(uri) # port is default 27017

# call database 'order'
db = myclient.order

# call collection 'order'
collec = db.order

s = time.time()
cursor = collec.find({
    "bettime":{"$gte":datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S")}, "finaltime":{"$lt":datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S")+datetime.timedelta(hours=1)}
})
result_mongo = []
token = ()

for ele in cursor:
    if ele['gametoken'] in token:
        for i in range(len(token)):
            if ele['gametoken'] == token[i]:
                idx = i
                break
        if ele['bettime'].timestamp() < result_mongo[idx]['bettime'].timestamp():
            ele['bettime'] = result_mongo[idx]['bettime']
        if ele['finaltime'].timestamp() > result_mongo[idx]['finaltime'].timestamp():
            ele['finaltime'] = result_mongo[idx]['finaltime']
    else:
        token+=(ele['gametoken'],)
        result_mongo.append({
            "gametoken":ele['gametoken'],
            "userid":ele['playerid'],
            "game_code":ele['gamecode'],
            "bettime":ele['bettime'],
            "wintime":ele['finaltime']
        })

e = time.time()
print(e-s)

df = pd.DataFrame(result_mongo, columns=result_mongo[0].keys())

df.to_excel(f"{rep_date[:10]}_user_gametoken_log.xlsx")