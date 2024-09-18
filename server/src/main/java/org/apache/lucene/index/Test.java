/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {

    public static void main(String[] args) throws Exception {
        Directory dir = FSDirectory.open(Path.of("/Users/mgodwan/docval_elb_logs"));
        DirectoryReader reader = DirectoryReader.open(dir);

        Set<Long> timestamps = new HashSet<>();
        Map<String, Map<String, Long>> sizeCounter = new HashMap<>();
        reader.leaves().forEach(lrc -> {
            try {
                FieldInfo fi = lrc.reader().getFieldInfos().fieldInfo("timestamp");
                SortedNumericDocValues sndv = lrc.reader().getSortedNumericDocValues("timestamp");

                sndv.nextDoc();
                while (sndv.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                    timestamps.add(sndv.nextValue());
                    sndv.nextDoc();
                }
                System.out.println(timestamps.size());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
