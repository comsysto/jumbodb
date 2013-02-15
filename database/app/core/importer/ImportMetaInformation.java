package core.importer;

/**
 * User: carsten
 * Date: 2/8/13
 * Time: 10:00 AM
 */
public class ImportMetaInformation {
    private String collection;
    private String deliveryKey;
    private String deliveryVersion;
    private String sourcePath;
    private String info;

    public ImportMetaInformation(String collection, String deliveryKey, String deliveryVersion, String sourcePath, String info) {
        this.collection = collection;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
        this.sourcePath = sourcePath;
        this.info = info;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getInfo() {
        return info;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    public String toString() {
        return "ImportMetaInformation{" +
                "collection='" + collection + '\'' +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                ", sourcePath='" + sourcePath + '\'' +
                ", info='" + info + '\'' +
                '}';
    }
}
