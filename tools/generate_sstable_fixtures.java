// SSTable fixture generator for ferrosa-sstable compatibility tests.
//
// Uses Cassandra's CQLSSTableWriter to produce BTI-format SSTables that serve
// as ground-truth fixtures for the Rust reader.
//
// Requires the Cassandra classpath.  Compile and run from the repo root:
//
//   cd cassandra && ant build && cd ..
//   javac -cp cassandra/build/classes/main:cassandra/lib/* \
//         tools/generate_sstable_fixtures.java -d tools/
//   java -cp tools:cassandra/build/classes/main:cassandra/lib/* \
//        generate_sstable_fixtures
//
// Outputs:
//   ferrosa-sstable/tests/fixtures/multi_partition/
//   ferrosa-sstable/tests/fixtures/single_partition/
//   ferrosa-sstable/tests/fixtures/wide_partition/
//   ferrosa-sstable/tests/fixtures/empty_table/
//
// Each directory receives all BTI component files renamed to a canonical
// prefix: "nb-1-bti-" (e.g. nb-1-bti-Data.db, nb-1-bti-Partitions.db, ...).

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;

public class generate_sstable_fixtures {

    // Base output path (relative to repo root).
    private static final String FIXTURE_BASE = "ferrosa-sstable/tests/fixtures";

    // We strip the Cassandra prefix and rename files to bare component names
    // (e.g. "Data.db", "Partitions.db") for simpler Rust test access.

    // BTI component suffixes we expect from Cassandra's writer.
    private static final String[] COMPONENT_SUFFIXES = {
        "Data.db",
        "Partitions.db",
        "Rows.db",
        "Filter.db",
        "Statistics.db",
        "CompressionInfo.db",
        "TOC.txt",
        // Uncompressed SSTables use CRC.db instead of CompressionInfo.db.
        // CQLSSTableWriter defaults to LZ4 compression, so we expect
        // CompressionInfo.db.  Include CRC.db here as a fallback.
        "CRC.db",
    };

    // ---------------------------------------------------------------
    // Schema shared by all fixtures
    // ---------------------------------------------------------------

    private static final String SCHEMA =
        "CREATE TABLE test.fixture ("
        + "pk text, "
        + "ck int, "
        + "val text, "
        + "PRIMARY KEY (pk, ck)"
        + ")";

    private static final String INSERT =
        "INSERT INTO test.fixture (pk, ck, val) VALUES (?, ?, ?)";

    // ---------------------------------------------------------------
    // Main
    // ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        System.out.println("Generating BTI SSTable fixtures ...");

        generateMultiPartition();
        generateSinglePartition();
        generateWidePartition();
        generateEmptyTable();

        System.out.println("Done.  Fixtures written to " + FIXTURE_BASE + "/");
    }

    // ---------------------------------------------------------------
    // Fixture: multi_partition
    //   ~5 partitions, each with 2-3 rows, simple text values.
    // ---------------------------------------------------------------

    private static void generateMultiPartition() throws Exception {
        String name = "multi_partition";
        File dir = prepareDir(name);

        CQLSSTableWriter writer = newBtiWriter(dir);
        writer.addRow("alpha", 1, "hello");
        writer.addRow("alpha", 2, "world");
        writer.addRow("bravo", 1, "foo");
        writer.addRow("bravo", 2, "bar");
        writer.addRow("bravo", 3, "baz");
        writer.addRow("charlie", 1, "one");
        writer.addRow("charlie", 2, "two");
        writer.addRow("delta", 1, "only-row");
        writer.addRow("echo", 1, "first");
        writer.addRow("echo", 2, "second");
        writer.addRow("echo", 3, "third");
        writer.close();

        renameComponents(dir);
        System.out.println("  " + name + " -> " + dir);
    }

    // ---------------------------------------------------------------
    // Fixture: single_partition
    //   1 partition, 1 row.  Tests the DataDirect / negative-idxpos
    //   code path where no row-index block is emitted.
    // ---------------------------------------------------------------

    private static void generateSinglePartition() throws Exception {
        String name = "single_partition";
        File dir = prepareDir(name);

        CQLSSTableWriter writer = newBtiWriter(dir);
        writer.addRow("only-pk", 1, "only-value");
        writer.close();

        renameComponents(dir);
        System.out.println("  " + name + " -> " + dir);
    }

    // ---------------------------------------------------------------
    // Fixture: wide_partition
    //   1 partition with 50+ clustering rows.  Exercises the row
    //   index trie (Rows.db) and potentially multiple index blocks.
    // ---------------------------------------------------------------

    private static void generateWidePartition() throws Exception {
        String name = "wide_partition";
        File dir = prepareDir(name);

        CQLSSTableWriter writer = newBtiWriter(dir);
        for (int ck = 0; ck < 64; ck++) {
            writer.addRow("wide-pk", ck, "val-" + ck);
        }
        writer.close();

        renameComponents(dir);
        System.out.println("  " + name + " -> " + dir);
    }

    // ---------------------------------------------------------------
    // Fixture: empty_table
    //   Table with schema but no data rows.  Edge case for parsers.
    // ---------------------------------------------------------------

    private static void generateEmptyTable() throws Exception {
        String name = "empty_table";
        File dir = prepareDir(name);

        CQLSSTableWriter writer = newBtiWriter(dir);
        // Write nothing — just open and close.
        writer.close();

        // CQLSSTableWriter may not emit any files when zero rows are written.
        // If that happens, we note it and skip the rename step.
        File[] remaining = dir.listFiles();
        if (remaining == null || remaining.length == 0) {
            System.out.println("  " + name + " -> (no files produced — "
                + "writer does not emit an SSTable for empty data; "
                + "create a minimal fixture by hand or skip this test)");
            return;
        }

        renameComponents(dir);
        System.out.println("  " + name + " -> " + dir);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Create a CQLSSTableWriter configured for BTI format with Murmur3
     * partitioner (the Cassandra default).
     *
     * NOTE: CQLSSTableWriter's static initializer calls
     * DatabaseDescriptor.clientInitialization() and sets up Murmur3Partitioner,
     * so we do not need explicit init here.
     */
    private static CQLSSTableWriter newBtiWriter(File dir) throws Exception {
        // Obtain a BtiFormat instance via the public factory.
        SSTableFormat<?, ?> btiFormat =
            new BtiFormat.BtiFormatFactory().getInstance(Collections.emptyMap());

        return CQLSSTableWriter.builder()
            .inDirectory(dir.getAbsolutePath())
            .forTable(SCHEMA)
            .using(INSERT)
            .withFormat(btiFormat)
            .build();
    }

    /**
     * Prepare (create / clean) the output directory for a fixture.
     */
    private static File prepareDir(String fixtureName) throws IOException {
        File dir = new File(FIXTURE_BASE, fixtureName);
        if (dir.exists()) {
            // Remove any stale files from previous runs.
            for (File f : dir.listFiles()) {
                f.delete();
            }
        } else {
            dir.mkdirs();
        }
        return dir;
    }

    /**
     * Rename Cassandra-generated SSTable component files to bare names.
     *
     * Cassandra writes files like:
     *   {version}-{id}-{format}-{Component}
     * e.g.  "nb-1-bti-Data.db"
     *
     * We strip the prefix entirely, renaming to just "Data.db", "Partitions.db",
     * etc. so the Rust tests can load components by simple name.
     *
     * After renaming, we rewrite TOC.txt to list the bare filenames.
     */
    private static void renameComponents(File dir) throws IOException {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }

        // Discover the prefix Cassandra used by examining any file.
        // Files follow the pattern: {prefix}-{ComponentSuffix}
        String detectedPrefix = null;
        for (File f : files) {
            String fname = f.getName();
            for (String suffix : COMPONENT_SUFFIXES) {
                if (fname.endsWith(suffix)) {
                    String candidate = fname.substring(0, fname.length() - suffix.length() - 1);
                    if (detectedPrefix == null) {
                        detectedPrefix = candidate;
                    }
                    break;
                }
            }
            if (detectedPrefix != null) break;
        }

        if (detectedPrefix == null) {
            System.err.println("WARNING: could not detect SSTable prefix in " + dir);
            System.err.println("  Files present:");
            for (File f : files) {
                System.err.println("    " + f.getName());
            }
            return;
        }

        // Rename each component file to its bare suffix.
        List<String> newTocEntries = new ArrayList<>();
        String prefixWithSep = detectedPrefix + "-";
        for (File f : files) {
            String fname = f.getName();
            if (!fname.startsWith(prefixWithSep)) {
                continue;
            }
            // Strip the prefix: "nb-1-bti-Data.db" -> "Data.db"
            String bareName = fname.substring(prefixWithSep.length());
            File dest = new File(dir, bareName);
            f.renameTo(dest);

            if (!bareName.equals("TOC.txt")) {
                newTocEntries.add(bareName);
            }
        }

        // Rewrite TOC.txt with the bare names.
        Collections.sort(newTocEntries);
        StringBuilder toc = new StringBuilder();
        for (String entry : newTocEntries) {
            toc.append(entry).append("\n");
        }
        toc.append("TOC.txt\n");

        Path tocPath = new File(dir, "TOC.txt").toPath();
        java.nio.file.Files.writeString(tocPath, toc.toString());
    }
}
