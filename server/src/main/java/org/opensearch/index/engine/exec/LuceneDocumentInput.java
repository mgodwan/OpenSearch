/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

public class LuceneDocumentInput implements DocumentInput<Iterable <? extends  IndexableField>> {

    private ParseContext.Document document = new ParseContext.Document();

    public void addField(MappedFieldType fieldType, Object value) {
        if (fieldType.typeName().equals("string")) {
            document.add(new TextField(fieldType.name(), (String) value, Field.Store.NO));
        }
    }

    @Override
    public Iterable<? extends IndexableField> getFinalInput() {
        return document;
    }
}
