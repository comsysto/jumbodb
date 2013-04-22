package org.jumbodb.connector.hadoop.configuration;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class FinishedNotification {
    private String deliveryChunkKey;
    private ImportHost host;

    public FinishedNotification(String deliveryChunkKey, ImportHost host) {
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

        FinishedNotification that = (FinishedNotification) o;

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
        return "FinishedNotification{" +
                "deliveryChunkKey='" + deliveryChunkKey + '\'' +
                ", host=" + host +
                '}';
    }
}
