package org.jumbodb.database.service.importer;

import java.io.InputStream;

public interface ImportHandler {
    String onImport(ImportMetaFileInformation information, InputStream dataInputStream);
    // CARSTEN weg damit!
    void onCollectionMetaData(ImportMetaData information);
    // CARSTEN weg damit!
    void onCollectionMetaIndex(ImportMetaIndex information);
    // CARSTEN weg damit!
    void onActivateDelivery(ImportMetaData information);
    // CARSTEN hier das activate mit rein!
    void onFinished(String deliveryKey, String deliveryVersion);
    boolean existsDeliveryVersion(String deliveryKey, String deliveryVersion);
}