// Standalone Murmur3 vector generator — extracted from Cassandra's MurmurHash.java
// to avoid requiring full Cassandra build (ant + guava + compilecommand deps).
//
// The hash3_x64_128 implementation below is copied verbatim from:
//   cassandra/src/java/org/apache/cassandra/utils/MurmurHash.java
// Only the helper methods it calls (getBlock, rotl64, fmix) are included.
//
// Run:  javac tools/generate_murmur3_vectors.java -d tools/
//       java -cp tools generate_murmur3_vectors

import java.nio.ByteBuffer;

public class generate_murmur3_vectors {

    // --- Copied from Cassandra MurmurHash.java (Apache-2.0) ---

    static long getBlock(ByteBuffer key, int offset, int index) {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((long) key.get(blockOffset + 0) & 0xff) +         (((long) key.get(blockOffset + 1) & 0xff) << 8) +
               (((long) key.get(blockOffset + 2) & 0xff) << 16) + (((long) key.get(blockOffset + 3) & 0xff) << 24) +
               (((long) key.get(blockOffset + 4) & 0xff) << 32) + (((long) key.get(blockOffset + 5) & 0xff) << 40) +
               (((long) key.get(blockOffset + 6) & 0xff) << 48) + (((long) key.get(blockOffset + 7) & 0xff) << 56);
    }

    static long rotl64(long v, int n) {
        return ((v << n) | (v >>> (64 - n)));
    }

    static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    static long[] hash3_x64_128(ByteBuffer key, int offset, int length, long seed) {
        final int nblocks = length >> 4;

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        for (int i = 0; i < nblocks; i++) {
            long k1 = getBlock(key, offset, i * 2 + 0);
            long k2 = getBlock(key, offset, i * 2 + 1);

            k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1;
            h1 = rotl64(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;

            k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;
            h2 = rotl64(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
        }

        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15: k2 ^= ((long) key.get(offset + 14)) << 48;
            case 14: k2 ^= ((long) key.get(offset + 13)) << 40;
            case 13: k2 ^= ((long) key.get(offset + 12)) << 32;
            case 12: k2 ^= ((long) key.get(offset + 11)) << 24;
            case 11: k2 ^= ((long) key.get(offset + 10)) << 16;
            case 10: k2 ^= ((long) key.get(offset + 9)) << 8;
            case  9: k2 ^= ((long) key.get(offset + 8)) << 0;
                k2 *= c2; k2 = rotl64(k2, 33); k2 *= c1; h2 ^= k2;

            case  8: k1 ^= ((long) key.get(offset + 7)) << 56;
            case  7: k1 ^= ((long) key.get(offset + 6)) << 48;
            case  6: k1 ^= ((long) key.get(offset + 5)) << 40;
            case  5: k1 ^= ((long) key.get(offset + 4)) << 32;
            case  4: k1 ^= ((long) key.get(offset + 3)) << 24;
            case  3: k1 ^= ((long) key.get(offset + 2)) << 16;
            case  2: k1 ^= ((long) key.get(offset + 1)) << 8;
            case  1: k1 ^= ((long) key.get(offset));
                k1 *= c1; k1 = rotl64(k1, 31); k1 *= c2; h1 ^= k1;
        }

        h1 ^= length; h2 ^= length;
        h1 += h2; h2 += h1;
        h1 = fmix(h1); h2 = fmix(h2);
        h1 += h2; h2 += h1;

        return new long[]{h1, h2};
    }

    // --- End Cassandra code ---

    public static void main(String[] args) {
        Object[][] cases = {
            {new byte[]{}, 0L},
            {new byte[]{0}, 0L},
            {new byte[]{1}, 0L},
            {new byte[]{0, 1, 2, 3}, 0L},
            {"hello".getBytes(), 0L},
            {"cassandra".getBytes(), 0L},
            {"ferrosa".getBytes(), 0L},
            {new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}, 0L},
            {new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}, 0L},
            {new byte[]{42}, 0L},
            {new byte[]{42, 43}, 0L},
            {new byte[]{42, 43, 44}, 0L},
            {new byte[]{42, 43, 44, 45}, 0L},
            {new byte[]{42, 43, 44, 45, 46}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47, 48}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47, 48, 49}, 0L},
            {new byte[]{42, 43, 44, 45, 46, 47, 48, 49, 50}, 0L},
            {new byte[]{(byte)0xFF}, 0L},
            {new byte[]{(byte)0x80}, 0L},
            {new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD}, 0L},
            {new byte[]{0, (byte)0x80, (byte)0xFF, 1, (byte)0x81, (byte)0xFE, 2, (byte)0x82}, 0L},
            {new byte[]{(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,
                        (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF}, 0L},
            {new byte[]{(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,
                        (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,
                        (byte)0xFF}, 0L},
            {"hello".getBytes(), 42L},
        };

        for (Object[] c : cases) {
            byte[] input = (byte[]) c[0];
            long seed = (Long) c[1];
            long[] result = hash3_x64_128(ByteBuffer.wrap(input), 0, input.length, seed);
            System.out.printf("            (&%s, %d, %d_i64, %d_i64),\n",
                formatBytes(input), seed, result[0], result[1]);
        }
    }

    static String formatBytes(byte[] b) {
        if (b.length == 0) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < b.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("0x%02X", b[i] & 0xff));
        }
        sb.append("]");
        return sb.toString();
    }
}
