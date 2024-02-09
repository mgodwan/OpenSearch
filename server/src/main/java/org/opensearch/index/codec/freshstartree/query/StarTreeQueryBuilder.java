/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.freshstartree.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.codec.freshstartree.builder.BaseSingleTreeBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class StarTreeQueryBuilder extends AbstractQueryBuilder<StarTreeQueryBuilder> {
    public static final String NAME = "startree";
    private static final ParseField FILTER = new ParseField("filter");
    private final List<QueryBuilder> filterClauses = new ArrayList<>();

    private final Set<String> groupBy = new HashSet<>();
    Map<String, List<Predicate<Long>>> predicateMap = new HashMap<>();
    private static final Logger logger = LogManager.getLogger(StarTreeQueryBuilder.class);

    public StarTreeQueryBuilder() {}

    /**
     * Read from a stream.
     */
    public StarTreeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        filterClauses.addAll(readQueries(in));
        in.readOptionalStringArray();
    }

    static List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<QueryBuilder> queries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            queries.add(in.readNamedWriteable(QueryBuilder.class));
        }
        return queries;
    }

    @Override
    protected void doWriteTo(StreamOutput out) {
        // only superclass has state
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        // printBoostAndQueryName(builder);
        doXArrayContent(FILTER, filterClauses, builder, params);
        builder.endObject();
    }

    private static void doXArrayContent(ParseField field, List<QueryBuilder> clauses, XContentBuilder builder, Params params)
        throws IOException {
        if (clauses.isEmpty()) {
            return;
        }
        builder.startArray(field.getPreferredName());
        for (QueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
    }

    private static final ObjectParser<StarTreeQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, StarTreeQueryBuilder::new);

    static {
        // declareStandardFields(PARSER);
        PARSER.declareObjectArrayOrNull(
            (builder, clauses) -> clauses.forEach(builder::filter),
            (p, c) -> parseInnerQueryBuilder(p),
            FILTER
        );
        PARSER.declareStringArray(StarTreeQueryBuilder::groupby, new ParseField("groupby"));

    }

    private void groupby(List<String> strings) {
        groupBy.addAll(strings);
    }

    public StarTreeQueryBuilder filter(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        filterClauses.add(queryBuilder);

        for (QueryBuilder filterClause : filterClauses) {
            if (filterClause instanceof BoolQueryBuilder) {
                BoolQueryBuilder bq = (BoolQueryBuilder) filterClause;
                List<QueryBuilder> shouldQbs = bq.should();
                for (QueryBuilder sqb : shouldQbs) {
                    if (sqb instanceof TermQueryBuilder) {
                        TermQueryBuilder tq = (TermQueryBuilder) sqb;
                        String field = tq.fieldName();
                        long val = Long.valueOf((String) tq.value());
                        List<Predicate<Long>> predicates = predicateMap.getOrDefault(field, new ArrayList<>());
                        Predicate<Long> predicate = status -> status == val;
                        predicates.add(predicate);
                        predicateMap.put(field, predicates);
                    }
                }
            }
        }

        return this;
    }

    public static StarTreeQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        // TODO : star tree supports either group by or filter
        if (predicateMap.size() > 0) {
            logger.info("Predicates: {} ", this.groupBy.toString() );
            return new StarTreeQuery(predicateMap, new HashSet<>());
        }
        logger.info("Group by : {} ", this.groupBy.toString() );
        return new StarTreeQuery(new HashMap<>(), this.groupBy);
    }

    @Override
    protected boolean doEquals(StarTreeQueryBuilder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
