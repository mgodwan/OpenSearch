/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexableField;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.collect.Tuple;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ParquetRecordWriter {

    private static final String schemaString = "message example {\n" +
        "required int32 id;\n" +
        "required binary name (UTF8);\n" +
        "required int32 age;\n" +
        "}";

    private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

    private final AtomicLong counter = new AtomicLong();

    private final CheckedSupplier<Tuple<String, ParquetWriter<Group>>, IOException> writerSupplier;

    private ConcurrentLinkedQueue<Tuple<String, ParquetWriter<Group>>> pool = new ConcurrentLinkedQueue<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public ParquetRecordWriter() {
        writerSupplier = () -> {
            long generation = counter.incrementAndGet();
            String fileName = "/tmp/generation-" + generation + ".parquet";
            return Tuple.tuple(fileName, ExampleParquetWriter.builder(new Path(fileName))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withDictionaryEncoding(true)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .build());
        };
    }

    public void write(ParseContext.Document document) throws IOException {
        try {
            lock.readLock().lock();
            Tuple<String, ParquetWriter<Group>> tuple = pool.poll();
            if (tuple == null) {
                tuple = writerSupplier.get();
            }
            ParquetWriter<Group> writer = tuple.v2();
            writer.write(parquetRow(document));
            pool.offer(tuple);
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<String> latestFlushPoint = new ArrayList<>();

    public void flush() throws IOException {
        final List<Tuple<String, ParquetWriter<Group>>> writers = new ArrayList<>();
        try {
            lock.writeLock().lock();
            while (!pool.isEmpty()) {
                writers.add(pool.poll());
            }
        } finally {
            lock.writeLock().unlock();
        }
        for (Tuple<String, ParquetWriter<Group>> writer : writers) {
            writer.v2().close();
        }
        latestFlushPoint = writers.stream().map(Tuple::v1).toList();
    }

    private Group parquetRow(ParseContext.Document doc) {
        Group group = new SimpleGroup(schema);
        for (IndexableField field : doc) {
            if (field.stringValue() != null) {
                group.add(field.name(), field.stringValue());
            } else if (field.numericValue() != null) {
                group.add(field.name(), field.numericValue().longValue());
            }
        }
        return group;
    }
}
