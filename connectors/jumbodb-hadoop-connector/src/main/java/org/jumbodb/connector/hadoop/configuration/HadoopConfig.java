package org.jumbodb.connector.hadoop.configuration;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:01 PM
 */
public class HadoopConfig {
    private String key;
    private String value;

    public HadoopConfig() {
    }

    public HadoopConfig(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "HadoopConfig{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
