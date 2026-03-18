CREATE KEYSPACE IF NOT EXISTS market_data WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 3
};

USE market_data;

CREATE TABLE IF NOT EXISTS ticks (
    symbol     text,
    trade_date date,
    trade_time timestamp,
    price      decimal,
    volume     bigint,
    exchange   text,
    PRIMARY KEY ((symbol, trade_date), trade_time)
) WITH CLUSTERING ORDER BY (trade_time DESC);

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol      text,
    candle_date date,
    candle_time timestamp,
    open_price  decimal,
    high_price  decimal,
    low_price   decimal,
    close_price decimal,
    volume      bigint,
    trade_count int,
    PRIMARY KEY ((symbol, candle_date), candle_time)
) WITH CLUSTERING ORDER BY (candle_time DESC);

CREATE TABLE IF NOT EXISTS daily_summary (
    symbol       text,
    trade_date   date,
    open_price   decimal,
    high_price   decimal,
    low_price    decimal,
    close_price  decimal,
    total_volume bigint,
    trade_count  int,
    vwap         decimal,
    PRIMARY KEY (symbol, trade_date)
) WITH CLUSTERING ORDER BY (trade_date DESC);
