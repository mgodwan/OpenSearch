/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.startree;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StarTreeAggregationBuilder extends AbstractAggregationBuilder<StarTreeAggregationBuilder> {
    public static final String NAME = "startree";

    private List<String> fieldCols;
    private List<String> metrics;
    public static final ObjectParser<StarTreeAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        StarTreeAggregationBuilder::new
    );

    static {
        PARSER.declareStringArray(StarTreeAggregationBuilder::groupby, new ParseField("groupby"));
        PARSER.declareStringArray(StarTreeAggregationBuilder::metrics, new ParseField("metrics"));
    }

    private void groupby(List<String> strings) {
        fieldCols = new ArrayList<>();
        fieldCols.addAll(strings);
    }

    private void metrics(List<String> strings) {
        metrics = new ArrayList<>();
        metrics.addAll(strings);
    }

    public StarTreeAggregationBuilder(String name) {
        super(name);
    }

    protected StarTreeAggregationBuilder(
        StarTreeAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new StarTreeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public StarTreeAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        String[] fieldArr = in.readOptionalStringArray();
        String[] metrics = in.readOptionalStringArray();
        if (fieldArr != null) {
            fieldCols = Arrays.asList(fieldArr);
        }
        if(metrics != null) {
            this.metrics = Arrays.asList(metrics);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Nothing to write
        out.writeOptionalStringArray(fieldCols.toArray(new String[0]));
        out.writeOptionalStringArray(metrics.toArray(new String[0]));
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected AggregatorFactory doBuild(
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new StarTreeAggregatorFactory(name, queryShardContext, parent, subFactoriesBuilder, metadata, fieldCols, metrics);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

}
