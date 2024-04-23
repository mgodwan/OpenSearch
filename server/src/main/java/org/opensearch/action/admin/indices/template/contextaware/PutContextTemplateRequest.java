/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.template.contextaware;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

@PublicApi(since = "3.0")
public class PutContextTemplateRequest  extends ClusterManagerNodeRequest<PutContextTemplateRequest> implements IndicesRequest, ToXContentObject {

    // TODO: Use as a valid json
    private String settings;

    private String name;

    private String version = "-1";

    public String settings() {
        return settings;
    }

    public String name() {
        return name;
    }

    public String version() {
        return version;
    }

    public PutContextTemplateRequest() {
    }

    public PutContextTemplateRequest(StreamInput in) throws IOException {
        this.name = in.readString();
        this.settings = in.readString();
        this.version = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(settings);
        out.writeString(version);
    }


    public PutContextTemplateRequest settings(String settings) {
        this.settings = settings;
        return this;
    }

    public PutContextTemplateRequest name(String name) {
        this.name = name;
        return this;
    }

    public PutContextTemplateRequest version(String version) {
        this.version = version;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("settings", settings);
        builder.field("version", version);
        builder.endObject();
        return null;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String[] indices() {
        return new String[0];
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.STRICT_EXPAND_OPEN;
    }

    @Override
    public String toString() {
        return "PutContextTemplateRequest{" +
            "settings='" + settings + '\'' +
            ", name='" + name + '\'' +
            ", version='" + version + '\'' +
            '}';
    }
}
