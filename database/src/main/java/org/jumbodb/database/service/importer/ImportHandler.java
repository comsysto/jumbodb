package org.jumbodb.database.service.importer;

import java.io.InputStream;

public interface ImportHandler {
    void onImport(ImportMetaFileInformation information, InputStream dataInputStream);
    void onCollectionMetaData(ImportMetaData information);
    void onCollectionMetaIndex(ImportMetaIndex information);
    void onActivateDelivery(ImportMetaData information);
    void onFinished(String deliveryKey, String deliveryVersion);
}