USE market_data;

-- AAPL ticks
INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:30:00.000', 195.25, 15000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:30:15.230', 195.30, 8500, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:30:32.100', 195.18, 12000, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:30:45.500', 195.42, 20000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:31:02.800', 195.50, 9200, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:31:18.450', 195.38, 11000, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:31:33.920', 195.55, 7800, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:31:50.100', 195.60, 14500, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:32:05.330', 195.48, 6300, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:32:22.750', 195.65, 18000, 'NASDAQ');

-- GOOGL ticks
INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:30:00.000', 178.90, 22000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:30:20.400', 178.85, 13500, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:30:41.800', 179.10, 9800, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:31:05.200', 179.25, 17000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:31:28.600', 179.05, 11200, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:31:50.900', 179.30, 8400, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:32:10.350', 179.45, 15600, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:32:35.100', 179.50, 19000, 'NASDAQ');

-- MSFT ticks
INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:30:00.000', 442.10, 25000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:30:18.700', 442.25, 12000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:30:35.400', 442.00, 16500, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:30:52.100', 442.50, 19800, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:31:10.600', 442.75, 8900, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:31:30.200', 442.60, 14200, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:31:48.800', 442.80, 21000, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:32:05.500', 443.00, 17500, 'NASDAQ');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:32:22.300', 442.90, 10600, 'NYSE');

INSERT INTO ticks (symbol, trade_date, trade_time, price, volume, exchange)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:32:40.900', 443.15, 23000, 'NASDAQ');

-- AAPL 1-minute candles
INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:30:00', 195.25, 195.42, 195.18, 195.42, 55500, 4);

INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:31:00', 195.50, 195.60, 195.38, 195.60, 33500, 3);

INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('AAPL', '2024-06-15', '2024-06-15 09:32:00', 195.48, 195.65, 195.48, 195.65, 24300, 2);

-- GOOGL 1-minute candles
INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:30:00', 178.90, 179.10, 178.85, 179.10, 45300, 3);

INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:31:00', 179.25, 179.30, 179.05, 179.30, 36600, 3);

INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('GOOGL', '2024-06-15', '2024-06-15 09:32:00', 179.45, 179.50, 179.45, 179.50, 34600, 2);

-- MSFT 1-minute candles
INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:30:00', 442.10, 442.50, 442.00, 442.50, 73300, 4);

INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:31:00', 442.75, 442.80, 442.60, 442.80, 44100, 3);

INSERT INTO candles_1m (symbol, candle_date, candle_time, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES ('MSFT', '2024-06-15', '2024-06-15 09:32:00', 443.00, 443.15, 442.90, 443.15, 51100, 3);

-- Daily summaries
INSERT INTO daily_summary (symbol, trade_date, open_price, high_price, low_price, close_price, total_volume, trade_count, vwap)
VALUES ('AAPL', '2024-06-14', 194.50, 196.20, 194.10, 195.80, 45200000, 128500, 195.42);

INSERT INTO daily_summary (symbol, trade_date, open_price, high_price, low_price, close_price, total_volume, trade_count, vwap)
VALUES ('AAPL', '2024-06-13', 193.80, 195.00, 193.20, 194.50, 38700000, 112000, 194.15);

INSERT INTO daily_summary (symbol, trade_date, open_price, high_price, low_price, close_price, total_volume, trade_count, vwap)
VALUES ('GOOGL', '2024-06-14', 177.50, 179.30, 177.10, 178.90, 28400000, 95200, 178.35);

INSERT INTO daily_summary (symbol, trade_date, open_price, high_price, low_price, close_price, total_volume, trade_count, vwap)
VALUES ('MSFT', '2024-06-14', 440.00, 443.50, 439.80, 442.10, 32100000, 108000, 441.85);
