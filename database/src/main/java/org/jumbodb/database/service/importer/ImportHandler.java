package org.jumbodb.database.service.importer;

import java.io.InputStream;

public interface ImportHandler {
    void onImport(ImportMetaFileInformation information, InputStream dataInputStream);
    void onCollectionMetaInformation(ImportMetaInformation information);
    void onActivateDelivery(ImportMetaInformation information);
    void onFinished(String deliveryKey, String deliveryVersion);
}