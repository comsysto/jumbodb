package org.jumbodb.connector.hadoop.configuration;

/**
 * @author Carsten Hufe
 */
public class CommitNotification {
    private String deliveryChunkKey;
    private ImportHost host;

    public CommitNotification(String deliveryChunkKey, ImportHost host) {
        this.deliveryChunkKey = deliveryChunkKey;
        this.host = host;
    }

    public String getDeliveryChunkKey() {
        return deliveryChunkKey;
    }

    public void setDeliveryChunkKey(String deliveryChunkKey) {
        this.deliveryChunkKey = deliveryChunkKey;
    }

    public ImportHost getHost() {
        return host;
    }

    public void setHost(ImportHost host) {
        this.host = host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommitNotification that = (CommitNotification) o;

        if (deliveryChunkKey != null ? !deliveryChunkKey.equals(that.deliveryChunkKey) : that.deliveryChunkKey != null)
            return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = deliveryChunkKey != null ? deliveryChunkKey.hashCode() : 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CommitNotification{" +
                "deliveryChunkKey='" + deliveryChunkKey + '\'' +
                ", host=" + host +
                '}';
    }
}
