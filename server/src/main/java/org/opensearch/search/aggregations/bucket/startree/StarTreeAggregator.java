/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.startree;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.freshstartree.codec.StarTreeAggregatedValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;


public class StarTreeAggregator extends BucketsAggregator implements SingleBucketAggregator {

   private Map<String, Long> sumMap = new HashMap<>();
    private Map<String, Integer> indexMap = new HashMap<>();

    final StarTree[] _starTrees;

    final InternalStarTree.Factory starTreeFactory;


    private static final Logger logger = LogManager.getLogger(StarTreeAggregator.class);


    public StarTreeAggregator(
        String name,
        AggregatorFactories factories,
        InternalStarTree.Factory starTreeFactory,
        StarTree[] starTrees,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata)
        throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this._starTrees = starTrees;
        this.starTreeFactory = starTreeFactory;
    }

    public static class StarTree implements Writeable, ToXContentObject {
        public static final ParseField KEY_FIELD = new ParseField("key");

        protected final String key;
        public StarTree(String key) {
            this.key = key;
        }
        /**
         * Read from a stream.
         */
        public StarTree(StreamInput in) throws IOException {
            key = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
        }

        public String getKey() {
            return this.key;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            builder.endObject();
            return builder;
        }

        public static final ConstructingObjectParser<StarTree, Void> PARSER = new ConstructingObjectParser<>("startree", arg -> {
            String key = (String) arg[0];
            return new StarTree(key);
        });

        static {
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.text(), KEY_FIELD, ObjectParser.ValueType.DOUBLE); // DOUBLE supports string and
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StarTree other = (StarTree) obj;
            return Objects.equals(key, other.key);
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds)
        throws IOException {

        return buildAggregationsForFixedBucketCount(
            owningBucketOrds,
            indexMap.size(),
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                String key = "";
                for(Map.Entry<String, Integer> entry : indexMap.entrySet()) {
                    if(offsetInOwningOrd == entry.getValue()) {
                        key = entry.getKey();
                        break;
                    }
                }

                //return starTreeFactory.createBucket(key, docCount, subAggregationResults);
                return new InternalStarTree.Bucket(key, sumMap.get(key), subAggregationResults);
            },
            buckets -> create(name, buckets, metadata())
        );
    }

    public InternalStarTree create(String name, List<InternalStarTree.Bucket> ranges, Map<String, Object> metadata) {
        return new InternalStarTree(name, ranges, metadata);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return null;//new InternalMissing(name, 0, buildEmptySubAggregations(), metadata());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub)
        throws IOException {
        StarTreeAggregatedValues values = (StarTreeAggregatedValues) ctx.reader().getAggregatedDocValues();
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                StarTreeAggregatedValues aggrVals = (StarTreeAggregatedValues) ctx.reader().getAggregatedDocValues();
                NumericDocValues dv = aggrVals.metricValues.get("status_sum");
                NumericDocValues hourValueDim = aggrVals.dimensionValues.get("hour");
                NumericDocValues dayValueDim = aggrVals.dimensionValues.get("day");
                SortedNumericDocValues statusValueDim = DocValues.singleton(aggrVals.dimensionValues.get("status"));
                NumericDocValues minuteValueDim = aggrVals.dimensionValues.get("minute");
                NumericDocValues monthValueDim = aggrVals.dimensionValues.get("month");
                if(dv.advanceExact(doc)) {
                    hourValueDim.advanceExact(doc);
                    long hour = hourValueDim.longValue();

                    statusValueDim.advanceExact(doc);
                    long status = statusValueDim.nextValue();

                    minuteValueDim.advanceExact(doc);
                    long minute = minuteValueDim.longValue();

                    dayValueDim.advanceExact(doc);
                    long day = dayValueDim.longValue();

                    monthValueDim.advanceExact(doc);
                    long month = monthValueDim.longValue();

                    String key = (status != -1 ? status : "ALL") + ":" + (month != -1 ? month : "ALL") + "-" +
                        (day != -1 ? day : "ALL") + "-" + (hour != -1 ? hour : "ALL") + "-" +
                        (minute != -1 ? minute : "ALL");

                    System.out.println("Key : " + key);
                    if(indexMap.containsKey(key)) {
                        sumMap.put(key, sumMap.getOrDefault(key, 0l) + dv.longValue());
                    } else {
                        indexMap.put(key, indexMap.size());
                        sumMap.put(key, dv.longValue());
                    }

                    collectBucket(sub, doc, subBucketOrdinal(bucket, indexMap.get(key)));
                }
            }
        };

    }

    private long subBucketOrdinal(long owningBucketOrdinal, int keyOrd) {
        long subord = owningBucketOrdinal * indexMap.size() + keyOrd;
        logger.info("Owning bucket ordinal : {} , rangeord : {} , len : {} == SubOrd : {}",
            owningBucketOrdinal, keyOrd, indexMap.size(), subord);
        return owningBucketOrdinal * indexMap.size() + keyOrd;
    }
}
