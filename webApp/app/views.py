#################################--------------#################################
# Author: Karthik Chaganti
# Descr: The file that reads from DB and populates the views in the website
#################################--------------#################################

from flask import render_template, request
from flask import jsonify
from app import app
from cassandra.cluster import Cluster
import random


# Connect to cassandra
cluster = Cluster(["ec2-34-198-185-77.compute-1.amazonaws.com"])
session = cluster.connect("stockportfolio")
session.default_fetch_size = None

def get_user_data(user):
    # Read user's data from cassandra dB
    hist_trades_live = session.execute("SELECT * FROM db_trades_live LIMIT 5")

    topK_traders = session.execute("SELECT * FROM db_user_portcount LIMIT 10")

    hist_trades = session.execute("SELECT tickername, tradequantity,tickerprice,total_val,tradetype,tradetime FROM db_trades_stream  WHERE userid= " + user + " ORDER BY tradetime DESC LIMIT 10")

    port_list   = session.execute("SELECT tickersector, tickername,tickerquant,tickervalue FROM db_user_portfolio WHERE userid=" + user + "LIMIT 10")

    port_list2   = session.execute("SELECT tickervalue,tickername, tickerquant,tickersector FROM db_user_portfolio WHERE userid=" + user)

    port_total  = session.execute("SELECT portfolio_count, portfolio_value FROM db_user_portcount WHERE userid=" + user)

    port_list_dict = map(lambda row: dict(zip(["tickervalue","tickername","tickerquant","tickersector"], row)), port_list2)

    lister = ['Consumer Discretionary ','Consumer Staples ','Financials ','Energy ','Health Care ','Industrials ','Information Technology ','Materials ','Real Estate ','Telecommunications Services ','Utilities ']

    sector_dict = {}

    for row in port_list_dict:
        if row['tickersector'] in sector_dict:
            val = sector_dict.get(row['tickersector'])
            val = val+row['tickervalue']
            sector_dict[row['tickersector']] = val
        else:
            val = row['tickervalue']
            sector_dict[row['tickersector']] = val

    statevals = []
    for key in lister:
        if key in sector_dict:
            valuer = sector_dict[key]
            ## print(valuer)
            statevals.append(valuer)
        else:
            valuer = 1
            statevals.append(valuer)





    sector_dict_chart_json =    """{
                                    "cols": [
                                          {"id":"","label":"Sector","pattern":"","type":"string"},
                                          {"id":"","label":"Value","pattern":"","type":"number"}
                                        ],
                                    "rows":
                                            [
                                          {"c":[{"v":"Consumer Discretionary","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Consumer Staples","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Financials","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Energy","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Utilities","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Health Care","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Industrials","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Information Technology","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Materials","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Real Estate","f":null},{"v":%d,"f":null}]},
                                          {"c":[{"v":"Telecommunications Services","f":null},{"v":%d,"f":null}]}
                                        ]
                                    }""" %  tuple(statevals)
    return [hist_trades,port_list,port_total,sector_dict_chart_json]

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/user1',methods=['GET'])
def get_user_cust():
    user=request.args.get("user")
    print(user)
    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(request.args.get("user"))

    return render_template("base.html", user=user, hist_trades = hist_trades, port_list = port_list, port_total = port_total)


@app.route('/slides')
def get_user_cust1():
    return render_template("slides.html")

@app.route('/firm')
def renderFirm():

    return render_template("firm.html")

@app.route('/user')
def get_user():

    #user=request.args.get("user")
    # usrlist for demonstration purposes!
    userList = ['c9f6f8cb-133c-49a3-94d6-c9067b8817fc',
    '111a68c3-5104-4a9f-8ae1-f45cdaca9fa1',
    '1781b41e-c033-44d7-aadc-3c6dfa811b54',
    '73e6e7c3-c2da-42f0-8e51-198329067dc3',
    '4ca0e6de-3904-4fbc-944b-28a610f26d48',
    'f839348d-b5af-4fd2-be05-9ccbe6624f96',
    '692a415f-e048-46ea-96a0-964a9a6444ff',
    '12deb35d-4da4-4c9b-8f40-8415302fea7b',
    '140020c3-69c9-4b45-b505-e53ccfa5b5a7',
    'f7c3b980-551a-44eb-be3d-30616a798ccc',
    '56efa34d-03bc-4fe4-b65d-7694019fbf25',
    'a26a0510-b0c9-4a89-a494-99dbe7ae2189',
    '6762b5e3-37ed-41fb-984e-f5db51ae807d',
    '535aec8b-e287-46c6-9c0e-47fc1e52ea31',
    'c6547a3a-8edb-4067-b30d-12d499ca786b',
    'e45003b0-4f91-42b5-af3d-a975015c8b1d',
    '8a5c35b6-574f-4268-9bfd-8e37998d1251',
    '7b3ed36e-68d0-4efa-acb4-91fdeabeb904',
    '703ea8f1-8147-4210-b315-3e81113226ab',
    '821779f7-f146-42fa-b867-f1668a669e16',
    '93d3292c-fc81-4d4c-ab71-0bc72e147ffc',
    'f073d073-8f95-4de1-a5c6-5594264f5ad7',
    '2ef99907-3608-4c34-8a86-050255e66553',
    '3ce524d0-5e81-4308-a8ee-9cf3576a2cd9',
    '792d8b5e-4d5e-4145-90f1-28f0d0e2414c',
    'd0c8942d-de14-4366-b953-8a64f8df7491',
    '60a88703-41cd-465b-947d-022c3a8e1351',
    'cad2c0d7-a117-4a2d-ae8a-644972e38c4f',
    'a6332940-1ebe-4626-ad55-2a722f1c508f']
    user = random.choice(userList)
    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(user)
    return render_template("base.html", user=user, hist_trades = hist_trades, port_list = port_list, port_total = port_total)

@app.route('/chartData')
def retrieve_chart_data():
    user=request.args.get("user")
    # print(user)
    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(user)
    return sector_dict_chart_json
