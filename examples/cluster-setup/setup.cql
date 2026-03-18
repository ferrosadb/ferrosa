-- Verify cluster is responsive
SELECT cluster_name, data_center FROM system.local;
SELECT * FROM system.peers;

-- Create test keyspace and verify replication
CREATE KEYSPACE IF NOT EXISTS setup_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE IF NOT EXISTS setup_test.hello (id int PRIMARY KEY, message text);
INSERT INTO setup_test.hello (id, message) VALUES (1, 'Hello from Ferrosa!');
SELECT * FROM setup_test.hello;
