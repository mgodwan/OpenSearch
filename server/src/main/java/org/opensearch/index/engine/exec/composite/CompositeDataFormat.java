/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.format.DataFormat;
import org.opensearch.index.mapper.ParametrizedFieldMapper;

import java.util.ArrayList;
import java.util.List;

public class CompositeDataFormat implements DataFormat {

    private List<DataFormat> dataFormats;

    public CompositeDataFormat(List<DataFormat> dataFormats) {
        this.dataFormats = dataFormats;
    }

    @Override
    public String name() {
        return "__composite__";
    }

    @Override
    public Settings dataFormatSettings() {
        Settings.Builder builder = Settings.builder();
        for (DataFormat dataFormat : dataFormats) {
            if (dataFormat.dataFormatSettings() != null) {
                builder.put(dataFormat.dataFormatSettings());
            }
        }
        return builder.build();
    }

    @Override
    public Settings nodeLevelDataFormatSettings() {
        Settings.Builder builder = Settings.builder();
        for (DataFormat dataFormat : dataFormats) {
            if (dataFormat.nodeLevelDataFormatSettings() != null) {
                builder.put(dataFormat.nodeLevelDataFormatSettings());
            }
        }
        return builder.build();
    }

    @Override
    public List<ParametrizedFieldMapper.Parameter<?>> parameters() {
        final List<ParametrizedFieldMapper.Parameter<?>> parameters = new ArrayList<>();
        for (DataFormat dataFormat : dataFormats) {
            if (dataFormat.parameters() != null) {
                parameters.addAll(dataFormat.parameters());
            }
        }
        return parameters;
    }
}
