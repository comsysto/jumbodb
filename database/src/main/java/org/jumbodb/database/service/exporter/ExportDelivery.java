package org.jumbodb.database.service.exporter;

/**
 * @author Carsten Hufe
 */
public class ExportDelivery {
    private String id;
    private String host;
    private int port;
    private boolean activate;
    private boolean running;
    private boolean finishedSuccessful;
    private String status;
    private double percentage;
    private long currentBytes;
    private long totalBytes;
    private long copyRateInBytes;

    public String getFormattedCopyRate() {
        return (copyRateInBytes / 1024 / 1024) + "MB/s";
    }

    public String getFormattedCurrent() {
        return (copyRateInBytes / 1024 / 1024) + "MB";
    }

    public String getFormattedTotal() {
        return (copyRateInBytes / 1024 / 1024) + "MB";
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

    public boolean isActivate() {
        return activate;
    }

    public void setActivate(boolean activate) {
        this.activate = activate;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isFinishedSuccessful() {
        return finishedSuccessful;
    }

    public void setFinishedSuccessful(boolean finishedSuccessful) {
        this.finishedSuccessful = finishedSuccessful;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public double getPercentage() {
        return percentage;
    }

    public void setPercentage(double percentage) {
        this.percentage = percentage;
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
