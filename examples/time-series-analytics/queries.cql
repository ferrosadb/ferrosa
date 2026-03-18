USE market_data;

-- Get the latest trades for a stock
SELECT trade_time, price, volume, exchange
FROM ticks
WHERE symbol = 'AAPL'
  AND trade_date = '2024-06-15'
LIMIT 10;

-- Price history for a time window
SELECT trade_time, price, volume, exchange
FROM ticks
WHERE symbol = 'AAPL'
  AND trade_date = '2024-06-15'
  AND trade_time >= '2024-06-15 09:30:00'
  AND trade_time <= '2024-06-15 09:32:00';

-- OHLC candles for charting
SELECT candle_time, open_price, high_price, low_price, close_price, volume
FROM candles_1m
WHERE symbol = 'AAPL'
  AND candle_date = '2024-06-15';

-- Compare candles across stocks: AAPL
SELECT candle_time, open_price, close_price, volume
FROM candles_1m
WHERE symbol = 'AAPL'
  AND candle_date = '2024-06-15';

-- Compare candles across stocks: MSFT
SELECT candle_time, open_price, close_price, volume
FROM candles_1m
WHERE symbol = 'MSFT'
  AND candle_date = '2024-06-15';

-- Volume analysis for MSFT
SELECT candle_time, volume, trade_count
FROM candles_1m
WHERE symbol = 'MSFT'
  AND candle_date = '2024-06-15';

-- Multi-day price history
SELECT trade_date, open_price, close_price, total_volume, vwap
FROM daily_summary
WHERE symbol = 'AAPL';
