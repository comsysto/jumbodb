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
    private boolean activate;
    private String deliveryChunkKey;
    private String version;
    private State state;
    private String status;
    private long currentBytes;
    private long totalBytes;
    private long copyRateInBytesCompressed;
    private long copyRateInBytesUncompressed;

    public String getFormattedCopyRateUncompressed() {
        return FileUtils.byteCountToDisplaySize(copyRateInBytesUncompressed) + " /s";
    }

    public String getFormattedCopyRateCompressed() {
        return FileUtils.byteCountToDisplaySize(copyRateInBytesCompressed) + " /s";
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

    public long getCopyRateInBytesUncompressed() {
        return copyRateInBytesUncompressed;
    }

    public void setCopyRateInBytesUncompressed(long copyRateInBytesUncompressed) {
        this.copyRateInBytesUncompressed = copyRateInBytesUncompressed;
    }

    public long getCopyRateInBytesCompressed() {
        return copyRateInBytesCompressed;
    }

    public void setCopyRateInBytesCompressed(long copyRateInBytesCompressed) {
        this.copyRateInBytesCompressed = copyRateInBytesCompressed;
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

    public boolean isActivate() {
        return activate;
    }

    public void setActivate(boolean activate) {
        this.activate = activate;
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
