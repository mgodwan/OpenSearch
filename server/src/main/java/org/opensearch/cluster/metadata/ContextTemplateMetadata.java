/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ContextTemplateMetadata implements Metadata.Custom {

    public static final String TYPE = "context_template";
    private static final ParseField CONTEXT_TEMPLATE = new ParseField(TYPE);
    private static final ConstructingObjectParser<ContextTemplateMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        a -> new ContextTemplateMetadata((Map<String, ContextTemplate>) a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, ContextTemplate> templates = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                templates.put(name, ContextTemplate.PARSER.apply(p, null));
            }
            return templates;
        }, CONTEXT_TEMPLATE);
    }

    public static ContextTemplateMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, ContextTemplate> contextTemplates;

    public ContextTemplateMetadata(Map<String, ContextTemplate> contextTemplates) {
        this.contextTemplates = contextTemplates;
    }

    public ContextTemplateMetadata(StreamInput in) throws IOException {
        this.contextTemplates = in.readMap(StreamInput::readString, ContextTemplate::new);
    }

    public Map<String, ContextTemplate> getContextTemplates() {
        return contextTemplates;
    }


    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(contextTemplates, StreamOutput::writeString, (s, v) -> v.writeTo(s));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (Map.Entry<String, ContextTemplate> entry: contextTemplates.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return null;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new ContextTemplateMetadataDiff((ContextTemplateMetadata) previousState, this);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    public static class ContextTemplate extends AbstractDiffable<ContextTemplate> implements ToXContentObject {

        private static final ParseField VERSION = new ParseField("version");
        private static final ParseField NAME = new ParseField("name");
        private static final ParseField SETTINGS = new ParseField("settings");

        public static final ConstructingObjectParser<ContextTemplate, Void> PARSER = new ConstructingObjectParser<>(
            TYPE,
            false,
            a -> new ContextTemplate((String) a[0], (String) a[1])
        );

        static {
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SETTINGS);
        }

        private final String name;
        private final String settings;
        private final long version;

        public ContextTemplate(String name, String settings) {
            this.name = name;
            this.settings = settings;
            this.version = 0L;
        }

        public String getSettings() {
            return settings;
        }

        public String getName() {
            return name;
        }

        public long getVersion() {
            return version;
        }

        public ContextTemplate(StreamInput in) throws IOException {
            this.name = in.readString();
            this.settings = in.readString();
            this.version = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(settings);
            out.writeLong(version);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", this.name);
            builder.field("settings", this.settings);
            builder.field("version", this.version);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ContextTemplate that = (ContextTemplate) o;
            return version == that.version && name.equals(that.name) && settings.equals(that.settings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, settings, version);
        }
    }

    public static class ContextTemplateMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, ContextTemplate>> contextTemplateDiff;

        public ContextTemplateMetadataDiff(ContextTemplateMetadata before, ContextTemplateMetadata after) {
            this.contextTemplateDiff = DiffableUtils.diff(before.contextTemplates, after.contextTemplates, DiffableUtils.getStringKeySerializer());
        }

        public ContextTemplateMetadataDiff(StreamInput in) throws IOException {
            this.contextTemplateDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ContextTemplate::new,
                si -> AbstractDiffable.readDiffFrom(ContextTemplate::new, si)
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ContextTemplateMetadata(contextTemplateDiff.apply(((ContextTemplateMetadata) part).contextTemplates));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            contextTemplateDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
