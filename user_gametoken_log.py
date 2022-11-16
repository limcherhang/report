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

# username = "DAxHermes_aries"
# passw = "AK4EyH7Nx96_GDwz"
# host = ["10.100.8.87","10.100.8.88","10.100.8.89"]
# port = "27017"
# replicaset = "pro_da_ana_rs"
# authentication_source = "admin"

# uri = f"mongodb://{username}:{passw}@"
# uri += f":{port},".join(host)
# uri += f"/?replicaSet={replicaset}&authSource={authentication_source}"
# # print(uri)
# myclient = pymongo.MongoClient(uri) # port is default 27017

# call database 'order'
db = myclient.order

# call collection 'order'
collec = db.order

s = time.time()
cursor = collec.find({
    "bettime":{"$gte":datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S")}, "createtime":{"$lt":datetime.datetime.strptime(rep_date, "%Y-%m-%d %H:%M:%S")+datetime.timedelta(hours=1)}
})
e = time.time()
print('query done : ', (e-s)/60, ' min')

result_mongo = {}
# token = ()
s = time.time()
for i, ele in enumerate(cursor):
    print(i)
    try:
        print(result_mongo['{}'.format(ele['gametoken'])])
        if ele['bettime'].timestamp() < result_mongo['{}'.format(ele['gametoken'])]['bettime'].timestamp():
            result_mongo['{}'.format(ele['gametoken'])]['bettime'] = ele['bettime']
        if ele['createtime'].timestamp() > result_mongo['{}'.format(ele['gametoken'])]['createtime'].timestamp():
            result_mongo['{}'.format(ele['gametoken'])]['createtime'] = ele['createtime']
    except KeyError:
        result_mongo['{}'.format(ele['gametoken'])] = {
            'gametoken':ele['gametoken'],
            'userid':ele['playerid'],
            'game_code':ele['gamecode'],
            'bettime':ele['bettime'],
            'wintime':ele['createtime']
        }
    # if ele['gametoken'] in token:
    #     for i in range(len(token)):
    #         if ele['gametoken'] == token[i]:
    #             idx = i
    #             break
    #     if ele['bettime'].timestamp() < result_mongo[idx]['bettime'].timestamp():
    #         ele['bettime'] = result_mongo[idx]['bettime']
    #     if ele['finaltime'].timestamp() > result_mongo[idx]['finaltime'].timestamp():
    #         ele['finaltime'] = result_mongo[idx]['finaltime']
    # else:
    #     token+=(ele['gametoken'],)
    #     result_mongo.append({
    #         "gametoken":ele['gametoken'],
    #         "userid":ele['playerid'],
    #         "game_code":ele['gamecode'],
    #         "bettime":ele['bettime'],
    #         "wintime":ele['finaltime']
    #     })

e = time.time()
print(e-s)

df = pd.DataFrame.from_dict(
    result_mongo, orient='index', columns=['gametoken', 'userid', 'game_code', 'bettime', 'wintime']
)

df.to_excel(f"{rep_date[:10]}_user_gametoken_log.xlsx")