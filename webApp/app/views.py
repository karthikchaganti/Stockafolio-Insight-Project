#################################--------------#################################
# Author: Karthik Chaganti
# Descr: The file that reads from DB and populates the views in the website
#################################--------------#################################

from flask import render_template, request
from flask import jsonify
from app import app
from cassandra.cluster import Cluster


# Connect to cassandra
cluster = Cluster(["ec2-34-198-185-77.compute-1.amazonaws.com"])
session = cluster.connect("stockportfolio")
session.default_fetch_size = None

def get_user_data(user):

    # Read user's data from cassandra dB
    hist_trades = session.execute("SELECT tickername, tradequantity,tickerprice,total_val,tradetype,tradetime FROM db_trades_stream  WHERE userid= " + user + " ORDER BY tradetime DESC LIMIT 5")
    port_list   = session.execute("SELECT tickervalue,tickername, tickerquant,tickersector FROM db_user_portfolio WHERE userid=" + user)
    port_total  = session.execute("SELECT portfolio_count, portfolio_value FROM db_user_portcount WHERE userid=" + user)
    port_list_dict = map(lambda row: dict(zip(["tickervalue","tickername","tickerquant","tickersector"], row)), port_list)
    # # print(port_list_json)
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
    print(sector_dict)
    ###############################


    ############################
    statevals = []
    for key in lister:
        if key in sector_dict:
            valuer = sector_dict[key]
            ## print(valuer)
            statevals.append(valuer)
        else:
            valuer = 1
            statevals.append(valuer)

    print(statevals)
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

def get_user_json_data(user):

    hist_trades, port_list, port_total = get_user_data(user)
    hist_trades_json = [{"tickername":row.tickername, "tradequantity":row.tradequantity,"tickerprice":row.tickerprice,"total_val":row.total_val,"tradetype":row.tradetype,"tradetime":row.tradetime}for row in hist_trades]
    port_list_json   = [{"tickername":row.tickername,"tickerquant":tickerquant,"tickervalue":tickervalue} for row in port_list]
    port_total_json  = [{"portfolio_count":row.portfolio_count,"portfolio_value":row.portfolio_value} for row in port_total]
    # print(port_list_json)
    return jsonify(hist_trades = hist_trades_json,port_list=port_list_json,port_total=port_total_json)

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/user')
def get_user():
    user=request.args.get("user")
    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(request.args.get("user"))
    return render_template("base2.html", user=user, hist_trades = hist_trades, port_list = port_list, port_total = port_total)

@app.route('/retrieve_user_data')
def retrieve_user_data():
    return get_user_json_data(request.args.get("user"))

@app.route('/chartData')
def retrieve_chart_data():
    user=request.args.get("user")
    # print(user)
    hist_trades,port_list,port_total,sector_dict_chart_json = get_user_data(user)
    # print(sector_dict_chart_json)
    jj = '{"cols": [{"id":"","label":"Topping","pattern":"","type":"string"},{"id":"","label":"Slices","pattern":"","type":"number"}],"rows": [{"c":[{"v":"Mushrooms","f":null},{"v":3,"f":null}]},        {"c":[{"v":"Onions","f":null},{"v":1,"f":null}]},        {"c":[{"v":"Olives","f":null},{"v":1,"f":null}]},{"c":[{"v":"Zucchini","f":null},{"v":1,"f":null}]}, {"c":[{"v":"Pepperoni","f":null},{"v":2,"f":null}]}]}'
    y = 'karthik'
    return sector_dict_chart_json

@app.route('/eg')
def retrieve_chart_data_1():
    jj = '{"cols": [{"id":"","label":"Topping","pattern":"","type":"string"},{"id":"","label":"Slices","pattern":"","type":"number"}],"rows": [{"c":[{"v":"Mushrooms","f":null},{"v":3,"f":null}]},        {"c":[{"v":"Onions","f":null},{"v":1,"f":null}]},        {"c":[{"v":"Olives","f":null},{"v":1,"f":null}]},{"c":[{"v":"Zucchini","f":null},{"v":1,"f":null}]}, {"c":[{"v":"Pepperoni","f":null},{"v":2,"f":null}]}]}'
    return jj
