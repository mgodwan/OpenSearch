/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Keys {

    private static List<String> keys = List.of(  "cab_color",
        "dropoff_datetime",
        "dropoff_location",
        "ehail_fee",
        "extra",
        "fare_amount",
        "improvement_surcharge",
        "mta_tax",
        "passenger_count",
        "payment_type",
        "pickup_datetime",
        "pickup_location",
        "rate_code_id",
        "store_and_fwd_flag",
        "surcharge",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "trip_distance",
        "trip_type",
        "vendor_id",
        "vendor_name");

    private static final Map<String, Short> key_to_val = new HashMap<>();
    private static final Map<Short, String> val_to_key = new HashMap<>();

    static {
        for (int i = 0; i < keys.size(); i ++) {
            key_to_val.put(keys.get(i), (short) i);
            val_to_key.put((short) i, keys.get(i));
        }
    }

    public static short getId(String name) {
        return key_to_val.get(name);
    }

    public static String getKey(short val) {
        return val_to_key.get(val);
    }
}
