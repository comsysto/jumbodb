package org.jumbodb.data.common.lz4;
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.Utils;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link InputStream} implementation to decode data written with
 * {@link LZ4BlockOutputStream}. This class is not thread-safe and does not
 * support {@link #mark(int)}/{@link #reset()}.
 *
 * @see LZ4BlockOutputStream
 */
public final class LZ4BlockInputStream extends FilterInputStream {
    private static final int HEADER_LENGTH = LZ4BlockOutputStream.HEADER_LENGTH;

    private final LZ4FastDecompressor decompressor;
    private byte[] buffer;
    private byte[] compressedBuffer;
    private int originalLen;
    private int o;
    private boolean finished;

    /**
     * Create a new {@link InputStream}.
     *
     * @param in           the {@link InputStream} to poll
     * @param decompressor the {@link LZ4FastDecompressor decompressor} instance to
     *                     use
     */
    public LZ4BlockInputStream(InputStream in, LZ4FastDecompressor decompressor) {
        super(in);
        this.decompressor = decompressor;
        this.buffer = new byte[0];
        this.compressedBuffer = new byte[HEADER_LENGTH];
        o = originalLen = 0;
        finished = false;
    }

    /**
     * Create a new instance which uses the fastest {@link LZ4FastDecompressor} available.
     *
     * @see LZ4Factory#fastestInstance()
     * @see #LZ4BlockInputStream(InputStream, LZ4FastDecompressor)
     */
    public LZ4BlockInputStream(InputStream in) {
        this(in, LZ4Factory.fastestInstance().fastDecompressor());
    }

    @Override
    public int available() throws IOException {
        return originalLen - o;
    }

    @Override
    public int read() throws IOException {
        if (finished) {
            return -1;
        }
        if (o == originalLen) {
            refill();
        }
        if (finished) {
            return -1;
        }
        return buffer[o++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Utils.checkRange(b, off, len);
        if (finished) {
            return -1;
        }
        if (o == originalLen) {
            refill();
        }
        if (finished) {
            return -1;
        }
        len = Math.min(len, originalLen - o);
        System.arraycopy(buffer, o, b, off, len);
        o += len;
        return len;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException {
        if (finished) {
            return -1;
        }
        if (o == originalLen) {
            refill();
        }
        if (finished) {
            return -1;
        }
        final int skipped = (int) Math.min(n, originalLen - o);
        o += skipped;
        return skipped;
    }

    private void refill() throws IOException {
        readFully(compressedBuffer, HEADER_LENGTH);
        final int compressedLen = Utils.readIntLE(compressedBuffer, 0);
        originalLen = Utils.readIntLE(compressedBuffer, 4);
        if (
                originalLen < 0
                        || compressedLen < 0
                        || (originalLen == 0 && compressedLen != 0)
                        || (originalLen != 0 && compressedLen == 0)) {
            throw new IOException("Stream is corrupted");
        }
        if (buffer.length < originalLen) {
            buffer = new byte[Math.max(originalLen, buffer.length * 3 / 2)];
        }

        if (compressedBuffer.length < originalLen) {
            compressedBuffer = new byte[Math.max(compressedLen, compressedBuffer.length * 3 / 2)];
        }
        readFully(compressedBuffer, compressedLen);
        try {
            final int compressedLen2 = decompressor.decompress(compressedBuffer, 0, buffer, 0, originalLen);
            if (compressedLen != compressedLen2) {
                throw new IOException("Stream is corrupted");
            }
        } catch (LZ4Exception e) {
            throw new IOException("Stream is corrupted", e);
        }

        o = 0;
    }

    private void readFully(byte[] b, int len) throws IOException {
        int read = 0;
        while (read < len) {
            final int r = in.read(b, read, len - read);
            if (r < 0) {
                throw new EOFException("Stream ended prematurely");
            }
            read += r;
        }
        assert len == read;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @SuppressWarnings("sync-override")
    @Override
    public void mark(int readlimit) {
        // unsupported
    }

    @SuppressWarnings("sync-override")
    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(in=" + in
                + ", decompressor=" + decompressor + ")";
    }

}
