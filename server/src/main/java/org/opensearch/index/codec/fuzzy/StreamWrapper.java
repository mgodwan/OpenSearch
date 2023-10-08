/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.EOFException;
import java.io.IOException;

public class StreamWrapper {

    public static StreamInput fromDataInput(DataInput in) {
        return new StreamInput() {
            @Override
            public byte readByte() throws IOException {
                return in.readByte();
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                in.readBytes(b, offset, len);
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int available() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            protected void ensureCanReadBytes(int length) throws EOFException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read() throws IOException {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static StreamOutput wrap(DataOutput output) {
        return new StreamOutput() {
            @Override
            public void writeByte(byte b) throws IOException {
                output.writeByte(b);
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                output.writeBytes(b, offset, length);
            }

            @Override
            public void flush() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void reset() throws IOException {
                throw new UnsupportedOperationException();
            }
        };
    }
}
