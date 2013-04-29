package org.jumbodb.database.service.exporter;

/**
 * @author Carsten Hufe
 */
public class StartReplication {
    private String deliveryChunkKey;
    private String version;
    private String host;
    private int port;
    private boolean activate;

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

    @Override
    public String toString() {
        return "StartReplication{" +
                "deliveryChunkKey='" + deliveryChunkKey + '\'' +
                ", version='" + version + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", activate=" + activate +
                '}';
    }
}
