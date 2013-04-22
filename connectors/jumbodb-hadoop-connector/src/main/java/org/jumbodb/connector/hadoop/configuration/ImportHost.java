package org.jumbodb.connector.hadoop.configuration;

/**
 * User: carsten
 * Date: 4/18/13
 * Time: 10:51 AM
 */
public class ImportHost {
    private String host;
    private int port;

    public ImportHost() {
    }

    public ImportHost(String host, int port) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImportHost that = (ImportHost) o;

        if (port != that.port) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "ImportHost{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
