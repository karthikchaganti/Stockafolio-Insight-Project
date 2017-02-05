CREATE KEYSPACE stockportfolio WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE stockportfolio;


-- All the trades that happen every second. From here the users' latest k trades can be extracted onto the dashboard

CREATE TABLE db_trades_stream (userId uuid,
userName text,
tickerName text,
tickerSector text,
tickerPrice float,
tradeQuantity bigint,
total_val double,
tradeTime timestamp,
tradeType text,
PRIMARY KEY(userId,tickerName,tradeTime)) WITH CLUSTERING ORDER BY (tradeTime DESC);


-- User vs Company Portfolio: Contains user id , company stock name, the total no.of. shares and the value along with its sector
-- userid, tickername, quantity, total value, sector
-- 123456, GOOG, 254 shares, 1100, information technology
-- 123456, AAPL, 135 shares, 1256, information technology

CREATE TABLE db_user_portfolio (userId uuid,
tickerName text,
tickerQuant bigint,
tickerValue double,
PRIMARY KEY(userId,tickerName));

CREATE TABLE db_user_portCount (userId uuid,
portfolio_count bigint,
portfolio_value double,
PRIMARY KEY(userId));

CREATE TABLE db_user_sector (userId uuid,sec_prop float,
  tickerSector text, PRIMARY KEY(userId,sec_prop,tickerSector))


--CREATE TABLE stock_counts_batch (user text, company text, stock_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY (user, company));
--CREATE TABLE stock_counts_rts1 (user text, company text, stock_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY (user, company));

--CREATE TABLE stock_totals_batch (user text, portfolio_total int, PRIMARY KEY (user));
--CREATE TABLE stock_totals_rts1 (user text, portfolio_total int, PRIMARY KEY (user));
--CREATE TABLE stock_totals_rts2 (user text, portfolio_total int, PRIMARY KEY (user));
--CREATE TABLE stock_totals_web (user text, portfolio_total int, PRIMARY KEY (user));

--a table of the most recent trades for the user
--CREATE TABLE trade_history (user text, company text, num_stock int, tradetime timestamp, PRIMARY KEY (user, tradetime, company)) WITH CLUSTERING ORDER BY (tradetime DESC);

--news
--CREATE TABLE news (company text, summary text, author text, source text, newstime timestamp, newsoutlet text, PRIMARY KEY ((company), newstime), ) WITH CLUSTERING ORDER BY (newstime DESC);
