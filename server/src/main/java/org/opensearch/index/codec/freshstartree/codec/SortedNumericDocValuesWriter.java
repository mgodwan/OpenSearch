/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.freshstartree.codec;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


public class SortedNumericDocValuesWriter {
    private final PackedLongValues.Builder pending; // stream of all values
    private PackedLongValues.Builder pendingCounts; // count of values per doc
    private final DocsWithFieldSet docsWithField;
    private final Counter iwBytesUsed;
    private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
    private final FieldInfo fieldInfo;
    private int currentDoc = -1;
    private long[] currentValues = new long[8];
    private int currentUpto = 0;

    private PackedLongValues finalValues;
    private PackedLongValues finalValuesCount;

    public SortedNumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
        this.fieldInfo = fieldInfo;
        this.iwBytesUsed = iwBytesUsed;
        pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        docsWithField = new DocsWithFieldSet();
        bytesUsed =
            pending.ramBytesUsed()
                + docsWithField.ramBytesUsed()
                + RamUsageEstimator.sizeOf(currentValues);
        iwBytesUsed.addAndGet(bytesUsed);
    }

    public void addValue(int docID, long value) {
        assert docID >= currentDoc;
        if (docID != currentDoc) {
            finishCurrentDoc();
            currentDoc = docID;
        }

        addOneValue(value);
        updateBytesUsed();
    }

    // finalize currentDoc: this sorts the values in the current doc
    private void finishCurrentDoc() {
        if (currentDoc == -1) {
            return;
        }
        if (currentUpto > 1) {
            Arrays.sort(currentValues, 0, currentUpto);
        }
        for (int i = 0; i < currentUpto; i++) {
            pending.add(currentValues[i]);
        }
        // record the number of values for this doc
        if (pendingCounts != null) {
            pendingCounts.add(currentUpto);
        } else if (currentUpto != 1) {
            pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
            for (int i = 0; i < docsWithField.cardinality(); ++i) {
                pendingCounts.add(1);
            }
            pendingCounts.add(currentUpto);
        }
        currentUpto = 0;

        docsWithField.add(currentDoc);
    }

    private void addOneValue(long value) {
        if (currentUpto == currentValues.length) {
            currentValues = ArrayUtil.grow(currentValues, currentValues.length + 1);
        }

        currentValues[currentUpto] = value;
        currentUpto++;
    }

    private void updateBytesUsed() {
        final long newBytesUsed =
            pending.ramBytesUsed()
                + (pendingCounts == null ? 0 : pendingCounts.ramBytesUsed())
                + docsWithField.ramBytesUsed()
                + RamUsageEstimator.sizeOf(currentValues);
        iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
        bytesUsed = newBytesUsed;
    }

    static final class LongValues {
        final long[] offsets;
        final PackedLongValues values;

        LongValues(
            int maxDoc,
            Sorter.DocMap sortMap,
            SortedNumericDocValues oldValues,
            float acceptableOverheadRatio)
            throws IOException {
            offsets = new long[maxDoc];
            PackedLongValues.Builder valuesBuiler =
                PackedLongValues.packedBuilder(acceptableOverheadRatio);
            int docID;
            long offsetIndex = 1; // 0 means the doc has no values
            while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
                int newDocID = sortMap.oldToNew(docID);
                int numValues = oldValues.docValueCount();
                valuesBuiler.add(numValues);
                offsets[newDocID] = offsetIndex++;
                for (int i = 0; i < numValues; i++) {
                    valuesBuiler.add(oldValues.nextValue());
                    offsetIndex++;
                }
            }
            values = valuesBuiler.build();
        }
    }

    private SortedNumericDocValues getValues(
        PackedLongValues values, PackedLongValues valueCounts, DocsWithFieldSet docsWithField) {
        if (valueCounts == null) {
            return DocValues.singleton(new BufferedNumericDocValues(values, docsWithField.iterator()));
        } else {
            return new BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator());
        }

    }

    public SortedNumericDocValues getSortedNumericDocValues()
        throws IOException {
        final PackedLongValues values;
        final PackedLongValues valueCounts;
        if (finalValues == null) {
            finishCurrentDoc();
            values = pending.build();
            valueCounts = pendingCounts == null ? null : pendingCounts.build();
            finalValues = values;
            finalValuesCount = valueCounts;
        } else {
            values = finalValues;
            valueCounts = finalValuesCount;
        }

        final SortedNumericDocValues buf = getValues(values, valueCounts, docsWithField);
        return buf;
    }

    private static class BufferedSortedNumericDocValues extends SortedNumericDocValues {
        final PackedLongValues.Iterator valuesIter;
        final PackedLongValues.Iterator valueCountsIter;
        final DocIdSetIterator docsWithField;
        private int valueCount;
        private int valueUpto;

        BufferedSortedNumericDocValues(
            PackedLongValues values, PackedLongValues valueCounts, DocIdSetIterator docsWithField) {
            valuesIter = values.iterator();
            valueCountsIter = valueCounts.iterator();
            this.docsWithField = docsWithField;
        }

        @Override
        public int docID() {
            return docsWithField.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            for (int i = valueUpto; i < valueCount; ++i) {
                valuesIter.next();
            }

            int docID = docsWithField.nextDoc();
            if (docID != NO_MORE_DOCS) {
                valueCount = Math.toIntExact(valueCountsIter.next());
                valueUpto = 0;
            }
            return docID;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docValueCount() {
            return valueCount;
        }

        @Override
        public long nextValue() {
            valueUpto++;
            return valuesIter.next();
        }

        @Override
        public long cost() {
            return docsWithField.cost();
        }
    }

    static class SortingSortedNumericDocValues extends SortedNumericDocValues {
        private final SortedNumericDocValues in;
        private final LongValues values;
        private int docID = -1;
        private long upto;
        private int numValues = -1;

        SortingSortedNumericDocValues(SortedNumericDocValues in, LongValues values) {
            this.in = in;
            this.values = values;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int nextDoc() {
            do {
                docID++;
                if (docID >= values.offsets.length) {
                    return docID = NO_MORE_DOCS;
                }
            } while (values.offsets[docID] <= 0);
            upto = values.offsets[docID];
            numValues = Math.toIntExact(values.values.get(upto - 1));
            return docID;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException("use nextDoc instead");
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            docID = target;
            upto = values.offsets[docID];
            if (values.offsets[docID] > 0) {
                numValues = Math.toIntExact(values.values.get(upto - 1));
                return true;
            }
            return false;
        }

        @Override
        public long nextValue() {
            return values.values.get(upto++);
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public int docValueCount() {
            return numValues;
        }
    }

    public static class BufferedNumericDocValues extends NumericDocValues {
        final PackedLongValues.Iterator iter;
        final DocIdSetIterator docsWithField;
        private long value;

        /** Values and doc with fields */
        public BufferedNumericDocValues(PackedLongValues values, DocIdSetIterator docsWithFields) {
            this.iter = values.iterator();
            this.docsWithField = docsWithFields;
        }

        @Override
        public int docID() {
            return docsWithField.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int docID = docsWithField.nextDoc();
            if (docID != NO_MORE_DOCS) {
                value = iter.next();
            }
            return docID;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return docsWithField.cost();
        }

        @Override
        public long longValue() {
            return value;
        }
    }
}
