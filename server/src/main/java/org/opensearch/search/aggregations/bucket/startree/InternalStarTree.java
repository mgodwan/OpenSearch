/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.startree;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalStarTree<B extends InternalStarTree.Bucket, R extends InternalStarTree<B, R>> extends InternalMultiBucketAggregation<
    R,
    B> {
    static final InternalStarTree.Factory FACTORY = new InternalStarTree.Factory();

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket {
        private final long docCount;
        private final InternalAggregations aggregations;
        private final String key;

        public Bucket(String key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public String getKey() {
            return getKeyAsString();
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        protected InternalStarTree.Factory<? extends InternalStarTree.Bucket, ?> getFactory() {
            return FACTORY;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Aggregation.CommonFields.KEY.getPreferredName(), key);
            builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            InternalStarTree.Bucket that = (InternalStarTree.Bucket) other;
            return Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations)
                && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, aggregations, key);
        }
    }

    public static class Factory<B extends Bucket, R extends InternalStarTree<B, R>> {
        public ValuesSourceType getValueSourceType() {
            return CoreValuesSourceType.NUMERIC;
        }

        public ValueType getValueType() {
            return ValueType.NUMERIC;
        }

        @SuppressWarnings("unchecked")
        public R create(String name, List<B> ranges, Map<String, Object> metadata) {
            return (R) new InternalStarTree<B, R>(name, ranges, metadata);
        }

        @SuppressWarnings("unchecked")
        public B createBucket(String key, long docCount, InternalAggregations aggregations) {
            return (B) new InternalStarTree.Bucket(key, docCount, aggregations);
        }

        @SuppressWarnings("unchecked")
        public R create(List<B> ranges, R prototype) {
            return (R) new InternalStarTree<B, R>(prototype.name, ranges, prototype.metadata);
        }

        @SuppressWarnings("unchecked")
        public B createBucket(InternalAggregations aggregations, B prototype) {
            return (B) new InternalStarTree.Bucket(prototype.getKey(), prototype.getDocCount(), aggregations);
        }
    }

    public InternalStarTree.Factory<B, R> getFactory() {
        return FACTORY;
    }

    private final List<B> ranges;

    public InternalStarTree(String name, List<B> ranges, Map<String, Object> metadata) {
        super(name, metadata);
        this.ranges = ranges;
    }

    /**
     * Read from a stream.
     */
    public InternalStarTree(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        List<B> ranges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            ranges.add(getFactory().createBucket(key, in.readVLong(), InternalAggregations.readFrom(in)));
        }
        this.ranges = ranges;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(ranges.size());
        for (B bucket : ranges) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return StarTreeAggregationBuilder.NAME;
    }

    @Override
    public List<B> getBuckets() {
        return ranges;
    }

    public R create(List<B> buckets) {
        return getFactory().create(buckets, (R) this);
    }

    @Override
    public B createBucket(InternalAggregations aggregations, B prototype) {
        return getFactory().createBucket(aggregations, prototype);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        reduceContext.consumeBucketsAndMaybeBreak(ranges.size());
        List<B>[] rangeList = new List[ranges.size()];
        for (int i = 0; i < rangeList.length; ++i) {
            rangeList[i] = new ArrayList<>();
        }
        for (InternalAggregation aggregation : aggregations) {
            InternalStarTree<B, R> ranges = (InternalStarTree<B, R>) aggregation;
            int i = 0;
            for (B range : ranges.ranges) {
                rangeList[i++].add(range);
            }
        }

        final List<B> ranges = new ArrayList<>();
        for (int i = 0; i < this.ranges.size(); ++i) {
            ranges.add((B) reduceBucket(rangeList[i], reduceContext));
        }
        return getFactory().create(name, ranges, getMetadata());
    }

    @Override
    protected B reduceBucket(List<B> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        long docCount = 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalStarTree.Bucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        InternalStarTree.Bucket prototype = buckets.get(0);
        return getFactory().createBucket(prototype.key, docCount, aggs);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());

        for (B range : ranges) {
            range.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ranges);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalStarTree<?, ?> that = (InternalStarTree<?, ?>) obj;
        return Objects.equals(ranges, that.ranges);
    }

}
