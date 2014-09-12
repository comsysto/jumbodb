package org.jumbodb.database.service.exporter;

import org.apache.commons.io.FileUtils;

/**
 * @author Carsten Hufe
 */
public class ExportDelivery {
    public enum State {WAITING, RUNNING, FINISHED, FAILED, ABORTED}
    private String id;
    private String host;
    private int port;
    private long startTimeMillis;
    private boolean activateChunk;
    private boolean activateVersion;
    private String deliveryChunkKey;
    private String version;
    private State state;
    private String status;
    private long currentBytes;
    private long totalBytes;
    private long copyRateInBytes;
    private long lastHitTimeMillis = System.currentTimeMillis();
    private long bytesSinceLastHit = 0l;

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public void setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public void addCurrentBytes(long bytes) {
        currentBytes += bytes;
        bytesSinceLastHit += bytes;
        long currentTimeMillis = System.currentTimeMillis();
        long timeMillisDiff = currentTimeMillis - lastHitTimeMillis;
        if(timeMillisDiff > 3000) {
            copyRateInBytes = (bytesSinceLastHit * 1000) / timeMillisDiff;
            bytesSinceLastHit = 0l;
            lastHitTimeMillis = currentTimeMillis;
        }
    }

    public String getFormattedCopyRate() {
        return FileUtils.byteCountToDisplaySize(copyRateInBytes) + " /s";
    }

    public String getFormattedCurrent() {
        return (currentBytes / 1024 / 1024) + " MB";
    }

    public String getFormattedTotal() {
        return (totalBytes / 1024 / 1024) + " MB";
    }

    public long getPercentage() {
        if(totalBytes == 0l) {
            return 0l;
        }
        return (currentBytes * 100) / totalBytes;
    }

    public String getDeliveryChunkKey() {
        return deliveryChunkKey;
    }

    public void setDeliveryChunkKey(String deliveryChunkKey) {
        this.deliveryChunkKey = deliveryChunkKey;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public long getCopyRateInBytes() {
        return copyRateInBytes;
    }

    public void setCopyRateInBytes(long copyRateInBytes) {
        this.copyRateInBytes = copyRateInBytes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isActivateChunk() {
        return activateChunk;
    }

    public void setActivateChunk(final boolean activateChunk) {
        this.activateChunk = activateChunk;
    }

    public boolean isActivateVersion() {
        return activateVersion;
    }

    public void setActivateVersion(final boolean activateVersion) {
        this.activateVersion = activateVersion;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getCurrentBytes() {
        return currentBytes;
    }

    public void setCurrentBytes(long currentBytes) {
        this.currentBytes = currentBytes;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }
}
