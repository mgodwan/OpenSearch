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
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalStarTree<B extends InternalStarTree.Bucket, R extends InternalStarTree<B, R>> extends InternalMultiBucketAggregation<
    R,
    B> {
    static final InternalStarTree.Factory FACTORY = new InternalStarTree.Factory();

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket {
        public long sum;
        public InternalAggregations aggregations;
        private final String key;

        public Bucket(String key, long sum, InternalAggregations aggregations) {
            this.key = key;
            this.sum = sum;
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
            return sum;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        protected InternalStarTree.Factory<? extends InternalStarTree.Bucket, ?> getFactory() {
            return FACTORY;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Aggregation.CommonFields.KEY.getPreferredName(), key);
            // TODO : this is hack ( we are mapping bucket.noofdocs to sum )
            builder.field("SUM", sum);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeVLong(sum);
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
            return Objects.equals(sum, that.sum) && Objects.equals(aggregations, that.aggregations) && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), sum, aggregations, key);
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
            // TODO : prototype.getDocCount() -- is mapped to sum - change this
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
        Map<String, List<B>> bucketsMap = new HashMap<>();

        for (InternalAggregation aggregation : aggregations) {
            InternalStarTree<B, R> filters = (InternalStarTree<B, R>) aggregation;
            int i = 0;
            for (B bucket : filters.ranges) {
                String key = bucket.getKey();
                List<B> sameRangeList = bucketsMap.get(key);
                if (sameRangeList == null) {
                    sameRangeList = new ArrayList<>(aggregations.size());
                    bucketsMap.put(key, sameRangeList);
                }
                sameRangeList.add(bucket);
            }
        }

        ArrayList<B> reducedBuckets = new ArrayList<>(bucketsMap.size());

        for (List<B> sameRangeList : bucketsMap.values()) {
            B reducedBucket = reduceBucket(sameRangeList, reduceContext);
            if (reducedBucket.getDocCount() >= 1) {
                reducedBuckets.add(reducedBucket);
            }
        }
        reduceContext.consumeBucketsAndMaybeBreak(reducedBuckets.size());
        Collections.sort(reducedBuckets, Comparator.comparing(InternalStarTree.Bucket::getKey));

        return getFactory().create(name, reducedBuckets, getMetadata());
    }

    @Override
    protected B reduceBucket(List<B> buckets, ReduceContext context) {
        assert buckets.size() > 0;

        B reduced = null;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (B bucket : buckets) {
            if (reduced == null) {
                reduced = (B) new Bucket(bucket.getKey(), bucket.getDocCount(), bucket.getAggregations());
            } else {
                reduced.sum += bucket.sum;
            }
            aggregationsList.add(bucket.getAggregations());
        }
        reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
        return reduced;
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
