/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;

public class LuceneIWTests extends OpenSearchTestCase {

    public void testOut() throws Exception {
        Directory d = FSDirectory.open(Files.createTempDirectory(this.getClass().getSimpleName()));
        IndexWriter iw = new IndexWriter(d, new IndexWriterConfig().setUseCompoundFile(false));
        for (int i = 1; i <= 100; i ++) {
            Document document = new Document();
            if (i % 2 == 0) {
                document.add(new SortedNumericDocValuesField("f1", i));
            }
            document.add(new SortedNumericDocValuesField("f2", 1000 + i));
            iw.addDocument(document);
        }
        iw.forceMerge(1);
        iw.close();
        SegmentInfos sis = SegmentInfos.readLatestCommit(d);
        sis.iterator().forEachRemaining(sci -> {
            try {
                Codec c = sci.info.getCodec();
                final String segmentSuffix = Long.toString(sci.getFieldInfosGen(), Character.MAX_RADIX);
                FieldInfos fis = c.fieldInfosFormat().read(d, sci.info, segmentSuffix, IOContext.READONCE);
                DocValuesProducer dvp = c.docValuesFormat()
                    .fieldsProducer(new SegmentReadState(d, sci.info, fis, IOContext.READONCE));

                SortedNumericDocValues dvf1 = dvp.getSortedNumeric(fis.fieldInfo("f1"));
                SortedNumericDocValues dvf2 = dvp.getSortedNumeric(fis.fieldInfo("f2"));

                for (int i = 1; i <= sci.info.maxDoc(); i ++) {
                    System.out.println(dvf1.nextDoc() + " --- > " + dvf2.nextDoc());
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
    }
}
