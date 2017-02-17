#################################--------------#################################
# Author: Karthik Chaganti
# Descr: The file that reads from DB and populates the views in the website
# Built using Python Flask Package.
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

# Top K users for demonstration
topK=['15bf9053-e523-4707-a016-41fc2f3d2b96',
'76e754db-5471-450e-bf48-a41bee28edd8',
'89194a36-0388-45c1-b9ab-1ae131624831',
'0ee774cf-3f1a-4075-993b-a91c9eb31e6c',
'9ceed831-0959-461a-a388-183c575ef933',
'33234cbf-cfbb-44b7-bcde-ebe1b04deb7c',
'76e754db-5471-450e-bf48-a41bee28edd8',
'1003e0eb-e1c2-40cc-bf1c-bb314602e0f5',
'429491c8-441c-4974-9669-c48a2d52280e',
'a8c4cd0c-a29c-457b-8a51-aa8950f4cdd2']

# Gets the sector wise proportions for the stocks
def getProportions(user):
    user = str(user)
    # Retrieve the stocks list of each individual
    port_list2   = session.execute("SELECT tickersector, tickername,tickerquant,tickervalue FROM db_user_portfolio WHERE userid=" + user)
    # Create a mapping for the stocks
    port_list_dict = map(lambda row: dict(zip(["tickersector","tickername","tickerquant","tickervalue"], row)), port_list2)
    # List of sectors
    lister = ['Consumer Discretionary ','Consumer Staples ','Financials ','Energy ','Health Care ','Industrials ','Information Technology ','Materials ','Real Estate ','Telecommunications Services ','Utilities ']

    sector_dict = {}

    # Read into the dictionary the portfolio count on each sector
    for row in port_list_dict:
        if row['tickersector'] in sector_dict:

            val = sector_dict.get(row['tickersector'])

            val = val+row['tickervalue']

            sector_dict[row['tickersector']] = val

        else:

            val = row['tickervalue']

            sector_dict[row['tickersector']] = val

    # Add the portfolio values onto each sector and save it in an array
    statevals = []

    for key in lister:

        if key in sector_dict:

            valuer = sector_dict[key]

            statevals.append(valuer)

        else:

            valuer = 1

            statevals.append(valuer)


    sector_dict_chart_json = appendChartData(statevals)

    return [sector_dict_chart_json,statevals]

# Create a chart in google chart json format
def appendChartData(statevals):

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

    return sector_dict_chart_json

# from cassandra get the data for a given user by reading from following tables
def get_user_data(user):

    hist_trades = session.execute("SELECT tickername, tradequantity,tickerprice,total_val,tradetype,tradetime FROM db_trades_stream  WHERE userid= " + user + " ORDER BY tradetime DESC LIMIT 10")

    port_list   = session.execute("SELECT tickersector, tickername,tickerquant,tickervalue FROM db_user_portfolio WHERE userid=" + user + "LIMIT 10")

    port_total  = session.execute("SELECT portfolio_count, portfolio_value FROM db_user_portcount WHERE userid=" + user)

    sector_dict_chart_json,statevals = getProportions(user)

    return [hist_trades,port_list,port_total,sector_dict_chart_json]

# routes to the html
@app.route('/')
@app.route('/index')
def index():

    return render_template("index.html")

@app.route('/user1',methods=['GET'])
def get_user_cust():

    user=request.args.get("user")

    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(request.args.get("user"))

    return render_template("base.html", user=user, hist_trades = hist_trades, port_list = port_list, port_total = port_total)


@app.route('/slides')
def get_user_cust1():

    return render_template("slides.html")

@app.route('/firmChart')
def ret_firmData():

    topK_traders = session.execute("SELECT * FROM db_user_dummycount LIMIT 10")

    total_trade_info = []

    for t in topK_traders:

        js,statevals = getProportions(t.userid)

        if not total_trade_info:

            total_trade_info = statevals

        else:

            for i in range(len(statevals)):

                total_trade_info[i] += statevals[i]

    firmData_json = appendChartData(total_trade_info)

    return firmData_json

@app.route('/firm')
def renderFirm():

    topK_info = []

    for user in topK:

        query_topK = session.execute("SELECT * FROM db_user_portcount WHERE userid=" + user)

        topK_info.append(query_topK)

    return render_template("firm.html", topK = topK_info)

@app.route('/user')
def get_user():

    user = random.choice(topK)

    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(user)

    return render_template("base.html", user=user, hist_trades = hist_trades, port_list = port_list, port_total = port_total)

@app.route('/chartData')
def retrieve_chart_data():

    user=request.args.get("user")

    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(user)

    return sector_dict_chart_json
