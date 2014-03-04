package org.jumbodb.database.service.importer;

import java.io.InputStream;

public interface ImportHandler {
    void onInit(String deliveryKey, String deliveryVersion, String date, String info) throws DeliveryVersionExistsException;
    void onImport(ImportMetaFileInformation information, InputStream dataInputStream) throws FileChecksumException;
    void onFinished(String deliveryKey, String deliveryVersion, boolean activateChunk, boolean activateVersion);
    boolean existsDeliveryVersion(String deliveryKey, String deliveryVersion);
}