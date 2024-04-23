/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.template.contextaware;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@PublicApi(since = "3.0.0")
public class Context implements Writeable, ToXContentFragment {
    private static ParseField VERSION = new ParseField("version");
    private static ParseField PARAMS = new ParseField("params");
    private static ParseField NAME = new ParseField("name");

    private final String name;
    private final long version;
    private final Map<String, Object> params;

    public Context(String name, long version, Map<String, Object> params) {
        this.name = name;
        this.version = version;
        this.params = params;
    }

    public Context(StreamInput in) throws IOException {
        this.name = in.readString();
        this.version = in.readLong();
        this.params = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(version);
        out.writeMap(params, StreamOutput::writeString, (o, v) -> o.writeString(v.toString()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);

        builder.field("version", version);

        builder.startObject("params");
        for (Map.Entry<String, Object> paramVal: this.params.entrySet()) {
            builder.field(paramVal.getKey(), paramVal.getValue().toString());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    public static Context fromXContent(XContentParser parser) throws IOException {
        String contextName = null;
        long version = -1L;
        Map<String, Object> params = null;

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            throw new IllegalArgumentException("No context is specified");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (PARAMS.match(currentFieldName, parser.getDeprecationHandler())) {
                    params = parser.mapOrdered();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                    version = parser.longValue();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    contextName = parser.text();
                }
            }
        }

        if (contextName == null || version < 0 || params == null) {
            throw new IllegalArgumentException("Unable to parse context field");
        }
        return new Context(contextName, version, params);
    }

    public String name() {
        return name;
    }

    public long version() {
        return version;
    }

    public Map<String, Object> params() {
        return params;
    }

    @Override
    public String toString() {
        return "Context{" +
            "name='" + name + '\'' +
            ", version=" + version +
            ", params=" + params +
            '}';
    }
}
