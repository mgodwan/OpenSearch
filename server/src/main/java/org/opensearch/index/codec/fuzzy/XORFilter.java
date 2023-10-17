/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.core.Assertions;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class XORFilter extends AbstractFuzzySet {

    private static final Logger logger = LogManager.getLogger(XORFilter.class);

    private static final int HASHES = 3;
    private static final int OFFSET = 32;
    private static final int FACTOR_TIMES_100 = 123;
    private static final MurmurHash3.Hash128 scratchHash = new MurmurHash3.Hash128();

    private final int size;
    private final int arrayLength;
    private final int blockLength;
    private long seed;
    private byte[] fingerprints;
    private final int bitCount;
    private final int mask;
    private final int bitsPerFingerprint;
    private final AtomicBoolean frozen = new AtomicBoolean();

    private static final int BASE_MEMORY_USAGE = 96;

    public XORFilter(CheckedSupplier<Iterator<BytesRef>, IOException> iteratorProvider, int bitsPerFingerprintSetting) throws IOException {
        this.size = count(iteratorProvider.get());
        this.bitsPerFingerprint = bitsPerFingerprintSetting;
        arrayLength = getArrayLength(size);
        bitCount = arrayLength * bitsPerFingerprint;
        mask = (1 << bitsPerFingerprint) - 1;
        blockLength = arrayLength / HASHES;
        addAll(iteratorProvider);
        if (Assertions.ENABLED) {
            assertAllElementsExist(iteratorProvider);
            if (bitsPerFingerprintSetting == 8) {
                assert mask == 0xff;
            }
        }
    }

    private static int count(Iterator<BytesRef> iterator) {
        int cnt = 0;
        while (iterator.hasNext()) {
            cnt ++;
            iterator.next();
        }
        return cnt;
    }

    XORFilter(DataInput in) throws IOException {
        this.bitsPerFingerprint = in.readInt();
        this.mask = (1 << bitsPerFingerprint) - 1;
        this.size = in.readInt();
        arrayLength = getArrayLength(this.size);
        bitCount = arrayLength * bitsPerFingerprint;
        blockLength = arrayLength / HASHES;
        seed = in.readLong();
        fingerprints = new byte[arrayLength];
        in.readBytes(fingerprints, 0, fingerprints.length);
    }

    @Override
    public SetType setType() {
        return SetType.XOR_FILTER_V1;
    }

    @Override
    public Result contains(BytesRef value) {
        long hash = Hash.hash64(generateKey(value), seed);
        int f = fingerprint(hash);
        int r0 = (int) hash;
        int r1 = (int) Long.rotateLeft(hash, 21);
        int r2 = (int) Long.rotateLeft(hash, 42);

        int h0 = Hash.reduce(r0, blockLength);
        int h1 = Hash.reduce(r1, blockLength) + blockLength;
        int h2 = Hash.reduce(r2, blockLength) + 2 * blockLength;

        f ^= fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2];
        return (f & mask) == 0 ? Result.MAYBE : Result.NO;
    }

    @Override
    protected void add(BytesRef value) {
        throw new UnsupportedOperationException("Cannot add a single element to xor filter!");
    }

    @Override
    protected void addAll(CheckedSupplier<Iterator<BytesRef>, IOException> valuesProvider) throws IOException {
        if (!frozen.compareAndSet(false, true)) {
            throw new IllegalStateException("Filter is frozen and cannot be updated");
        }

        long[] reverseOrder = new long[size];
        byte[] reverseH = new byte[size];
        int reverseOrderPos;
        long seed;
        boolean isFirstIteration = true;
        int cnt = 0;

        mainloop:
        do {
            if (isFirstIteration) cnt = 0;

            seed = Hash.randomSeed();
            reverseOrderPos = 0;
            byte[] t2count = new byte[arrayLength];
            long[] t2 = new long[arrayLength];

            Iterator<BytesRef> iterator = valuesProvider.get();
            while (iterator.hasNext()) {
                if (isFirstIteration) cnt ++;
                long k = generateKey(iterator.next());
                for (int hi = 0; hi < HASHES; hi++) {
                    int h = getHash(k, seed, hi);
                    t2[h] ^= k;
                    if (t2count[h] > 120) {
                        // probably something wrong with the hash function
                        continue mainloop;
                    }
                    t2count[h]++;
                }
            }

            int[][] alone = new int[HASHES][blockLength];
            int[] alonePos = new int[HASHES];
            for (int nextAlone = 0; nextAlone < HASHES; nextAlone ++) {
                for (int i = 0; i < blockLength; i++) {
                    if (t2count[nextAlone * blockLength + i] == 1) {
                        alone[nextAlone][alonePos[nextAlone]++] = nextAlone * blockLength + i;
                    }
                }
            }
            int found = -1;
            while (true) {
                int i = -1;
                for (int hi = 0; hi < HASHES; hi++) {
                    if (alonePos[hi] > 0) {
                        i = alone[hi][--alonePos[hi]];
                        found = hi;
                        break;
                    }
                }
                if (i == -1) {
                    // no entry found
                    break;
                }
                if (t2count[i] <= 0) {
                    continue;
                }
                long k = t2[i];
                if (t2count[i] != 1) {
                    throw new AssertionError();
                }
                --t2count[i];
                for (int hi = 0; hi < HASHES; hi++) {
                    if (hi != found) {
                        int h = getHash(k, seed, hi);
                        int newCount = --t2count[h];
                        if (newCount == 1) {
                            alone[hi][alonePos[hi]++] = h;
                        }
                        t2[h] ^= k;
                    }
                }
                reverseOrder[reverseOrderPos] = k;
                reverseH[reverseOrderPos] = (byte) found;
                reverseOrderPos++;
            }
        } while (reverseOrderPos != size); // This was size in actual but changed it to use actual count of docs in iterator.

        this.seed = seed;
        byte[] fp = new byte[arrayLength * (bitsPerFingerprint / 8)];
        for (int i = reverseOrderPos - 1; i >= 0; i--) {
            long k = reverseOrder[i];
            int found = reverseH[i];
            int change = -1;
            long hash = Hash.hash64(k, seed);
            int xor = fingerprint(hash);
            for (int hi = 0; hi < HASHES; hi++) {
                int h = getHash(k, seed, hi);
                if (found == hi) {
                    change = h;
                } else {
                    xor ^= fp[h];
                }
            }
            fp[change] = (byte) xor;
        }
        fingerprints = new byte[arrayLength];
        System.arraycopy(fp, 0, fingerprints, 0, fp.length);
    }

    @Override
    public boolean isSaturated() {
        return false;
    }

    @Override
    public Optional<FuzzySet> maybeDownsize() {
        return Optional.empty();
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(bitsPerFingerprint);
        out.writeInt(size);
        out.writeLong(seed);
        out.writeBytes(fingerprints, fingerprints.length);
    }

    @Override
    public long ramBytesUsed() {
        return BASE_MEMORY_USAGE + RamUsageEstimator.sizeOf(fingerprints);
    }

    private int getHash(long key, long seed, int index) {
        long r = Long.rotateLeft(Hash.hash64(key, seed), 21 * index);
        r = Hash.reduce((int) r, blockLength);
        r = r + index * blockLength;
        return (int) r;
    }

    private int fingerprint(long hash) {
        return (int) (hash & ((1 << bitsPerFingerprint) - 1));
    }

    private static int getArrayLength(int size) {
        return (int) (OFFSET + (long) FACTOR_TIMES_100 * size / 100);
    }

    public static class Hash {

        private static Random random = new Random();

        public static void setSeed(long seed) {
            random.setSeed(seed);
        }

        public static long hash64(long x, long seed) {
            x += seed;
            x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL;
            x = (x ^ (x >>> 33)) * 0xc4ceb9fe1a85ec53L;
            x = x ^ (x >>> 33);
            return x;
        }

        public static long randomSeed() {
            return random.nextLong();
        }

        /**
         * Shrink the hash to a value 0..n. Kind of like modulo, but using
         * multiplication and shift, which are faster to compute.
         *
         * @param hash the hash
         * @param n the maximum of the result
         * @return the reduced value
         */
        public static int reduce(int hash, int n) {
            // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
            return (int) (((hash & 0xffffffffL) * n) >>> 32);
        }
    }

    public static void main(String[] args) throws Exception {
        XORFilter filter = new XORFilter(() -> List.of(new BytesRef("item1"), new BytesRef("item2"), new BytesRef("item3")).iterator(), 8);
        System.out.println(filter.contains(new BytesRef("item1")));
        System.out.println(filter.contains(new BytesRef("item2")));
        System.out.println(filter.contains(new BytesRef("item3")));
        System.out.println(filter.contains(new BytesRef("item5")));

        byte[] b = new byte[1000000];
        DataOutput output = new ByteArrayDataOutput(b);

        filter.writeTo(output);

        XORFilter filter1 = new XORFilter(new ByteArrayDataInput(b));
        System.out.println(filter1.contains(new BytesRef("item1")));
        System.out.println(filter1.contains(new BytesRef("item2")));
        System.out.println(filter1.contains(new BytesRef("item3")));
        System.out.println(filter1.contains(new BytesRef("item5")));
    }
}
