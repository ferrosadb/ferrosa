/**
 * Cassandra SSTable fixture generator.
 *
 * Uses Cassandra's CQLSSTableWriter to produce BTI-format SSTables that
 * ferrosa's reader must parse correctly. The generated SSTables serve as
 * the ground truth for cross-compatibility testing.
 *
 * Schema: CREATE TABLE test.compat (pk text, ck int, val text, ttl_val text, PRIMARY KEY (pk, ck))
 *
 * Build:
 *   cd cassandra && ant build
 *   javac -cp "cassandra/build/classes/main:cassandra/lib/*" tests/sstable-compat/CassandraSSTableWriter.java
 *
 * Run:
 *   java -cp "cassandra/build/classes/main:cassandra/lib/*:tests/sstable-compat" \
 *     CassandraSSTableWriter ferrosa-sstable/tests/fixtures/cassandra_generated
 */

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

public class CassandraSSTableWriter {

    public static void main(String[] args) throws Exception {
        String outputDir = args.length > 0 ? args[0] : "/tmp/cassandra_sstable_fixtures";
        new File(outputDir).mkdirs();

        String schema = "CREATE TABLE test.compat ("
                + "pk text, "
                + "ck int, "
                + "val text, "
                + "PRIMARY KEY (pk, ck)"
                + ")";

        String insert = "INSERT INTO test.compat (pk, ck, val) VALUES (?, ?, ?)";
        String insertTTL = "INSERT INTO test.compat (pk, ck, val) VALUES (?, ?, ?) USING TTL ?";

        // === Fixture 1: Normal cells ===
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir + "/normal_cells")
                .forTable(schema)
                .using(insert)
                .build()) {

            writer.addRow("alpha", 1, "hello");
            writer.addRow("alpha", 2, "world");
            writer.addRow("bravo", 1, "foo");
            writer.addRow("charlie", 1, "bar");
            writer.addRow("charlie", 2, "baz");
        }
        System.out.println("Generated: normal_cells");

        // === Fixture 2: Expiring cells (TTL) ===
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir + "/ttl_cells")
                .forTable(schema)
                .using(insertTTL)
                .build()) {

            writer.addRow("ttl_pk", 1, "expires_soon", 60);      // 60 second TTL
            writer.addRow("ttl_pk", 2, "expires_later", 3600);    // 1 hour TTL
            writer.addRow("ttl_pk", 3, "expires_much_later", 86400); // 1 day TTL
        }
        System.out.println("Generated: ttl_cells");

        // === Fixture 3: Empty and null values ===
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir + "/edge_cases")
                .forTable(schema)
                .using(insert)
                .build()) {

            writer.addRow("empty", 1, "");         // empty string
            writer.addRow("empty", 2, (String)null); // null value
            writer.addRow("long_key_" + "x".repeat(200), 1, "long partition key");
        }
        System.out.println("Generated: edge_cases");

        // === Fixture 4: Many partitions (stress) ===
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir + "/many_partitions")
                .forTable(schema)
                .using(insert)
                .build()) {

            for (int i = 0; i < 100; i++) {
                writer.addRow("pk_" + i, 1, "value_" + i);
            }
        }
        System.out.println("Generated: many_partitions");

        // === Fixture 5: Wide partition (many rows per PK) ===
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir + "/wide_partition")
                .forTable(schema)
                .using(insert)
                .build()) {

            for (int i = 0; i < 500; i++) {
                writer.addRow("wide", i, "row_" + i);
            }
        }
        System.out.println("Generated: wide_partition");

        System.out.println("All fixtures generated in: " + outputDir);
    }
}
