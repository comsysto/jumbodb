package org.jumbodb.connector.importer;

/**
 * @author Carsten Hufe
 */
public class ImportInfo {
    private String deliveryKey;
    private String deliveryVersion;
    private String date;
    private String info;

    public ImportInfo(final String deliveryKey, final String deliveryVersion, final String date, final String info) {
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.date = date;
        this.info = info;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    public String getDate() {
        return date;
    }

    public String getInfo() {
        return info;
    }

    @Override
    public String toString() {
        return "ImportInfo{" +
          "deliveryKey='" + deliveryKey + '\'' +
          ", deliveryVersion='" + deliveryVersion + '\'' +
          ", date='" + date + '\'' +
          ", info='" + info + '\'' +
          '}';
    }
}
