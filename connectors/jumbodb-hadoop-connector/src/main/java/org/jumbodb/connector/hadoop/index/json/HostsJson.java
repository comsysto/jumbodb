package org.jumbodb.connector.hadoop.index.json;

/**
 * User: carsten
 * Date: 4/18/13
 * Time: 10:51 AM
 */
public class HostsJson {
    private String host;
    private int port;

    public HostsJson(String host, int port) {
        this.host = host;
        this.port = port;
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

    @Override
    public String toString() {
        return "HostsJson{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
