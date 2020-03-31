/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network.banzaicloud;

import org.apache.kafka.common.network.PlaintextTransportLayer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Read buffered {@link PlaintextTransportLayer} to be able to push back on receiving buffer
 */
public class ReadBufferedPlaintextTransportLayer extends PlaintextTransportLayer {
    private static final Logger log = LoggerFactory.getLogger(ReadBufferedPlaintextTransportLayer.class);
    private static final int BUFF = 2;
    private ByteBuffer netReadBuffer;
    private boolean authenticationDone;
    private boolean skipOnce;

    public ReadBufferedPlaintextTransportLayer(SelectionKey key) throws IOException {
        super(key);
        netReadBuffer = ByteBuffer.allocate(BUFF);
    }

    /**
     * No more buffer duplication needed
     */
    public void authenticationDone() {
        authenticationDone = true;
    }

    /**
     * Ignore buffer content once
     */
    public void gotPayloadLength() {
        skipOnce = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if ((offset < 0) || (length < 0) || (offset > dsts.length - length)) {
            throw new IndexOutOfBoundsException();
        }

        int totalRead = 0;
        int i = offset;
        while (i < length) {
            if (dsts[i].hasRemaining()) {
                int read = read(dsts[i]);
                if (read > 0) {
                    totalRead += read;
                } else {
                    break;
                }
            } else {
                i++;
            }
        }
        return totalRead;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        log.trace("dst: {}", new BufferDetails(dst));
        netReadBuffer = Utils.ensureCapacity(netReadBuffer, netReadBuffer.capacity() + (dst.remaining() - netReadBuffer.remaining()));

        int read = 0;
        if (hasBytesBuffered()) {
            read += readFromNetReadBuffer(dst);
        }
        log.trace("dst: {}", new BufferDetails(dst));

        int netread = 0;
        while (dst.hasRemaining()) {
            if (netReadBuffer.hasRemaining()) {
                netread = readSocket();
                read += readFromNetReadBuffer(dst);
            }

            if (errOrNothing(netread)) {
                break;
            }
        }
        log.trace("dst: {}", new BufferDetails(dst));

        if (netread < 0 && read == 0 && !hasBytesBuffered()) {
            throw new EOFException("EOF during read");
        }

        log.trace("byte read: " + read);
        return read;
    }

    private int readFromNetReadBuffer(ByteBuffer dst) {
        ByteBuffer buffer = createBuffer();
        int copied = copyBuffer(buffer, dst);
        compactBuffer(buffer);
        return copied;
    }

    private ByteBuffer createBuffer() {
        ByteBuffer buff = netReadBuffer;
        if (!authenticationDone) {
            buff = netReadBuffer.duplicate();
            if (skipOnce) {
                skipOnce = false;
                buff.position(0);
            }
        }
        return buff;
    }

    private int copyBuffer(ByteBuffer from, ByteBuffer to) {
        from.flip();
        int remaining = Math.min(from.remaining(), to.remaining());
        if (remaining > 0) {
            int oldLimit = from.limit();
            from.limit(from.position() + remaining);
            to.put(from);
            from.limit(oldLimit);
        }
        return remaining;
    }

    private void compactBuffer(ByteBuffer buffer) {
        if (authenticationDone) {
            buffer.compact();
        }
    }

    private int readSocket() throws IOException {
        return socketChannel().read(netReadBuffer);
    }

    private boolean errOrNothing(int val) {
        return val <= 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasBytesBuffered() {
        return netReadBuffer.position() != 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        netReadBuffer = null;
        super.close();
    }

    /**
     * Get rid of some bytes we don't need after authentication has finished
     *
     * @param pos the buffer position in front of we want to get rid of buffer's content
     */
    public void ignoreExtraPayloadFrom(int pos) {
        log.trace("net buffer: {}", new BufferDetails(netReadBuffer));

        netReadBuffer.flip();
        int oldLimit = netReadBuffer.limit();
        netReadBuffer.limit(pos);
        byte[] toDispose = new byte[pos];
        netReadBuffer.get(toDispose, 0, pos);
        netReadBuffer.limit(oldLimit);
        netReadBuffer.compact();

        log.trace("net buffer: {}", new BufferDetails(netReadBuffer));
    }

    /**
     * Simple wrapper class to present basic buffer attributes
     */
    static class BufferDetails {
        private final ByteBuffer bb;

        BufferDetails(ByteBuffer byteBuffer) {
            bb = byteBuffer;
        }

        @Override
        public String toString() {
            return  " pos: " + bb.position() +
                    " limit: " + bb.limit() +
                    " rem: " + bb.remaining() +
                    " cap: " + bb.capacity();
        }
    }
}
