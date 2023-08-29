/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.BloomFilterFactory;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.CompletionFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.bloom.DefaultBloomFilterFactory;

import java.util.function.Function;

/**
 * {@link PerFieldMappingPostingFormatCodec This postings format} is the default
 * {@link PostingsFormat} for OpenSearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} per field. This
 * allows users to change the low level postings format for individual fields
 * per index in real time via the mapping API. If no specific postings format is
 * configured for a specific field the default postings format is used.
 *
 * @opensearch.internal
 */
public class PerFieldMappingPostingFormatCodec extends Lucene95Codec {
    private final Logger logger;
    private final MapperService mapperService;
    private final DocValuesFormat dvFormat = new Lucene90DocValuesFormat();

    private Function<BloomFilterFactory, BloomFilteringPostingsFormat> bloomedPostingFormatSupplierForIdField;

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMappingPostingFormatCodec.class)
            : "PerFieldMappingPostingFormatCodec must subclass the latest " + "lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMappingPostingFormatCodec(Mode compressionMode, MapperService mapperService, Logger logger) {
        super(compressionMode);
        this.mapperService = mapperService;
        this.logger = logger;
        this.bloomedPostingFormatSupplierForIdField = (factory) -> new BloomFilteringPostingsFormat(super.getPostingsFormatForField(IdFieldMapper.NAME), factory);
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        final MappedFieldType fieldType = mapperService.fieldType(field);
        if (fieldType == null) {
            logger.warn("no index mapper found for field: [{}] returning default postings format", field);
        } else if (fieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            return CompletionFieldMapper.CompletionFieldType.postingsFormat();
        } else if (IdFieldMapper.NAME.equals(field) && mapperService.getIndexSettings().isUseBloomFilterForDocIds()) {
            return bloomedPostingFormatSupplierForIdField.apply(new OpenSearchBloomFilterFactory(new DefaultBloomFilterFactory(),
                mapperService.getIndexSettings().getBloomFilterForDocIdFalsePositiveProbability()));
        }
        return super.getPostingsFormatForField(field);
    }

    // Extend bloom filter factory
    private static class OpenSearchBloomFilterFactory extends BloomFilterFactory {

        private BloomFilterFactory bloomFilterFactory;
        private float falsePositiveProbability;

        public OpenSearchBloomFilterFactory(BloomFilterFactory bloomFilterFactory, float falsePositiveProbability) {
            this.bloomFilterFactory = bloomFilterFactory;
            this.falsePositiveProbability = falsePositiveProbability;
        }

        @Override
        public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
            return FuzzySet.createOptimalSet(state.segmentInfo.maxDoc(), falsePositiveProbability);
        }

        @Override
        public boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo) {
            return bloomFilterFactory.isSaturated(bloomFilter, fieldInfo);
        }
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return dvFormat;
    }

    public static void main(String[] args) {
        long maxNumUniqueValues = 1022688423;
        for(double i = 0.1023d; i < 1.0d; i += 0.05) {
            double targetMaxFpp = i;
            long setSize =
                (long)
                    Math.ceil(
                        (maxNumUniqueValues * Math.log(targetMaxFpp))
                            / Math.log(1 / Math.pow(2, Math.log(2))));
            System.out.println(i + "," + setSize);
        }

    }
}
