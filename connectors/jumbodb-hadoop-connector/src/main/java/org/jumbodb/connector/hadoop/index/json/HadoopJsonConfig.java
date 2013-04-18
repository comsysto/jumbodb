package org.jumbodb.connector.hadoop.index.json;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:01 PM
 */
public class HadoopJsonConfig {
    private String key;
    private String value;

    public HadoopJsonConfig() {
    }

    public HadoopJsonConfig(String key, String value) {
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
        return "HadoopJsonConfig{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
