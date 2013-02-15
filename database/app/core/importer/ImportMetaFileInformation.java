package core.importer;

public class ImportMetaFileInformation {
    public enum FileType {INDEX, DATA}

    private FileType fileType;
    private String fileName;
    private String collection;
    private String indexName;
    private long fileLength;
    private String deliveryKey;
    private String deliveryVersion;

    public ImportMetaFileInformation(FileType fileType, String fileName, String collection, String indexName, long fileLength, String deliveryKey, String deliveryVersion) {
        this.fileType = fileType;
        this.fileName = fileName;
        this.collection = collection;
        this.indexName = indexName;
        this.fileLength = fileLength;
        this.deliveryKey = deliveryKey;
        this.deliveryVersion = deliveryVersion;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getFileName() {
        return fileName;
    }

    public String getCollection() {
        return collection;
    }

    public String getIndexName() {
        return indexName;
    }

    public long getFileLength() {
        return fileLength;
    }

    public String getDeliveryKey() {
        return deliveryKey;
    }

    public String getDeliveryVersion() {
        return deliveryVersion;
    }

    @Override
    public String toString() {
        return "ImportMetaFileInformation{" +
                "fileType=" + fileType +
                ", fileName='" + fileName + '\'' +
                ", collection='" + collection + '\'' +
                ", indexName='" + indexName + '\'' +
                ", fileLength=" + fileLength +
                ", deliveryKey='" + deliveryKey + '\'' +
                ", deliveryVersion='" + deliveryVersion + '\'' +
                '}';
    }
}