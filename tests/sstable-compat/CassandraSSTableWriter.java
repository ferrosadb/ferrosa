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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

        // === Fixture 6: Non-frozen collections (complex columns) ===
        // Exercises Cassandra's complex-column on-disk layout: each collection
        // element is its own cell sharing the column's storage index, keyed by a
        // cell path (list -> TimeUUID, set -> element, map -> key). This is the
        // ground-truth fixture for ferrosa's complex-column reader.
        String collSchema = "CREATE TABLE test.collections ("
                + "pk text, "
                + "l list<int>, "
                + "s set<text>, "
                + "m map<text,int>, "
                + "PRIMARY KEY (pk)"
                + ")";
        String collInsert =
                "INSERT INTO test.collections (pk, l, s, m) VALUES (?, ?, ?, ?)";
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDir + "/collections")
                .forTable(collSchema)
                .using(collInsert)
                .build()) {

            List<Integer> list = Arrays.asList(10, 20, 30);
            Set<String> set = new LinkedHashSet<>(Arrays.asList("a", "b", "c"));
            Map<String, Integer> map = new LinkedHashMap<>();
            map.put("k1", 1);
            map.put("k2", 2);
            writer.addRow("row1", list, set, map);
        }
        System.out.println("Generated: collections");

        System.out.println("All fixtures generated in: " + outputDir);
    }
}
