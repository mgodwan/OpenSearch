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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.action.support.replication.ReplicatedWriteRequest;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * A bulk shard request targeting a specific shard ID
 *
 * @opensearch.internal
 */
public class BulkShardRequest extends ReplicatedWriteRequest<BulkShardRequest> implements Accountable {

    public static final Version COMPACT_SHARD_ID_VERSION = LegacyESVersion.V_7_9_0;
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

    private final BulkItemRequest[] items;

    public Map<Short, Object>[] parsedEntities;

    private static enum Type {
        INT((short) 0),
        LONG((short) 1),
        SHORT((short) 2),
        BYTE((short) 3),
        DOUBLE((short) 4),
        STRING((short) 5),
        FLOAT((short) 6),
        GEO_POINT((short) 7);

        short s;
        Type(short s) {
            this.s = s;
        }
        short getval() {
            return s;
        }

        static Type [] arr;

        static {
            arr = new Type[Type.values().length];
            for (Type t: Type.values()) {
                arr[t.getval()] = t;
            }
        }

        static Type from(short s) {
            return arr[s];
        }
    }



    public BulkShardRequest(StreamInput in) throws IOException {
        super(in);
        final ShardId itemShardId = in.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION) ? shardId : null;
        items = in.readArray(i -> i.readOptionalWriteable(inpt -> new BulkItemRequest(itemShardId, inpt)), BulkItemRequest[]::new);
        parsedEntities = in.readArray(is -> {
            int sz = is.readInt();
            Map<Short, Object> map = new ConcurrentHashMap<>(sz);
            for (int i = 0; i < sz; i ++) {
                short key = is.readShort();
                Object val = null;
                Type t = Type.from(in.readShort());
                switch (t) {
                    case INT:
                        val = in.readInt();
                        break;
                    case LONG:
                        val = in.readLong();
                        break;
                    case FLOAT:
                        val = in.readFloat();
                        break;
                    case DOUBLE:
                        val = in.readDouble();
                        break;
                    case SHORT:
                        val = in.readShort();
                        break;
                    case BYTE:
                        val = in.readByte();
                        break;
                    case STRING:
                        val = in.readString();
                        break;
                    case GEO_POINT:
                        int cnt = in.readInt();
                        List<GeoPoint> points = new ArrayList<GeoPoint>(cnt);
                        for (int ii = 0; ii < cnt; ii ++) points.add(new GeoPoint(in));
                        val = points;
                        break;
                }
                map.put(key, val);
            }
            return map;
        }, ConcurrentHashMap[]::new);
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items) {
        super(shardId);
        this.items = items;
        setRefreshPolicy(refreshPolicy);
        parsedEntities = new ConcurrentHashMap[items.length];
    }

    public BulkItemRequest[] items() {
        return items;
    }

    @Override
    public String[] indices() {
        // A bulk shard request encapsulates items targeted at a specific shard of an index.
        // However, items could be targeting aliases of the index, so the bulk request although
        // targeting a single concrete index shard might do so using several alias names.
        // These alias names have to be exposed by this method because authorization works with
        // aliases too, specifically, the item's target alias can be authorized but the concrete
        // index might not be.
        Set<String> indices = new HashSet<>(1);
        for (BulkItemRequest item : items) {
            if (item != null) {
                indices.add(item.index());
            }
        }
        return indices.toArray(new String[0]);
    }


    private static final List<? extends GeoPoint> gpl = new ArrayList<>();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(out.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION) ? (o, item) -> {
            if (item != null) {
                o.writeBoolean(true);
                item.writeThin(o);
            } else {
                o.writeBoolean(false);
            }
        } : StreamOutput::writeOptionalWriteable, items);
        out.writeArray((o, v) -> {
            o.writeInt(v == null ? 0 : v.size());
            if (v != null) {
                for (Map.Entry<Short, Object> entry : v.entrySet()) {
                    o.writeShort(entry.getKey());
                    Object val = entry.getValue();
                    if (val instanceof Number) {
                        if (val instanceof Integer) {
                            o.writeShort(Type.INT.getval());
                            o.writeInt((int) val);
                        } else if (val instanceof Long) {
                            o.writeShort(Type.LONG.getval());
                            o.writeLong((long) val);
                        } else if (val instanceof Float) {
                            o.writeShort(Type.FLOAT.getval());
                            o.writeFloat((float) val);
                        } else if (val instanceof Double) {
                            o.writeShort(Type.DOUBLE.getval());
                            o.writeDouble((double) val);
                        } else if (val instanceof Short) {
                            o.writeShort(Type.SHORT.getval());
                            o.writeShort((short) val);
                        }
                        else if (val instanceof Byte) {
                            o.writeShort(Type.BYTE.getval());
                            o.writeByte((byte) val);
                        }
                    } else if (val.getClass().isAssignableFrom(gpl.getClass())) {
                        o.writeShort(Type.GEO_POINT.getval());
                        List<? extends GeoPoint> gpls = (List<? extends GeoPoint>) val;
                        o.writeInt(gpls.size());
                        for (int i = 0; i < gpls.size(); i ++) gpls.get(i).writeTo(o);
                    } else {
                        o.writeShort(Type.STRING.getval());
                        o.writeString(entry.getValue().toString());
                    }
                }
            }
        }, parsedEntities);
    }

    @Override
    public String toString() {
        // This is included in error messages so we'll try to make it somewhat user friendly.
        StringBuilder b = new StringBuilder("BulkShardRequest [");
        b.append(shardId).append("] containing [");
        if (items.length > 1) {
            b.append(items.length).append("] requests");
        } else {
            b.append(items[0].request()).append("]");
        }

        switch (getRefreshPolicy()) {
            case IMMEDIATE:
                b.append(" and a refresh");
                break;
            case WAIT_UNTIL:
                b.append(" blocking until refresh");
                break;
            case NONE:
                break;
        }

        b.append("Parsed Entities: " + parsedEntities.length + " \n");
        for (Map<Short, Object> v: parsedEntities) {
            b.append(v);
        }
        return b.toString();
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder().append("requests[").append(items.length).append("], index").append(shardId);
        final RefreshPolicy refreshPolicy = getRefreshPolicy();
        if (refreshPolicy == RefreshPolicy.IMMEDIATE || refreshPolicy == RefreshPolicy.WAIT_UNTIL) {
            stringBuilder.append(", refresh[").append(refreshPolicy).append(']');
        }
        return stringBuilder.toString();
    }

    @Override
    protected BulkShardRequest routedBasedOnClusterVersion(long routedBasedOnClusterVersion) {
        return super.routedBasedOnClusterVersion(routedBasedOnClusterVersion);
    }

    @Override
    public void onRetry() {
        for (BulkItemRequest item : items) {
            if (item.request() instanceof ReplicationRequest) {
                // all replication requests need to be notified here as well to ie. make sure that internal optimizations are
                // disabled see IndexRequest#canHaveDuplicates()
                ((ReplicationRequest<?>) item.request()).onRetry();
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + Stream.of(items).mapToLong(Accountable::ramBytesUsed).sum();
    }
}
