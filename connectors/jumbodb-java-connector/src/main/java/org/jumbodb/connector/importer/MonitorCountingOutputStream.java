package org.jumbodb.connector.importer;

import org.apache.commons.io.output.CountingOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Carsten Hufe
 */
public class MonitorCountingOutputStream extends CountingOutputStream {
    private long lastTime;
    private long lastBytes = 0l;
    private long intervalMs;
    private long rateInBytesPerSecond = 0l;
    private long measuredBytes = 0l;
    /**
     * Constructs a new CountingOutputStream.
     *
     * @param out the OutputStream to write to
     * @param intervalMs interval in ms
     */
    public MonitorCountingOutputStream(OutputStream out, long intervalMs) {
        super(out);
        this.intervalMs = intervalMs;
        this.lastTime = System.currentTimeMillis();
    }

    @Override
    protected void afterWrite(int n) throws IOException {
        long current = System.currentTimeMillis();
        long timeDiff = current - lastTime;
        if(timeDiff > intervalMs) {
            long currentBytes = getByteCount();
            long bytesDiff = currentBytes - lastBytes;
            long speed = (bytesDiff * 1000) / timeDiff;
            onInterval(speed, bytesDiff);
            measuredBytes += bytesDiff;
            lastBytes = currentBytes;
            lastTime = current;
        }
        super.afterWrite(n);
    }

    protected void onInterval(long rateInBytesPerSecond, long copiedBytesSinceLastCall) {
        this.rateInBytesPerSecond = rateInBytesPerSecond;
    }

    public long getRateInBytesPerSecond() {
        return rateInBytesPerSecond;
    }

    public long getMeasuredBytes() {
        return measuredBytes;
    }

    public long getNotMeasuredBytes() {
        return getByteCount() - measuredBytes;
    }
}
