package org.jumbodb.database.service.exporter;

import org.jumbodb.database.service.management.storage.StorageManagement;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author Carsten Hufe
 */
public class ExportDeliveryService {
    private ExecutorService executorService;

    public void startReplication(StartReplication startReplication) {
        StorageManagement storageManagement;


    }

    public List<ExportDelivery> getReplications() {
        ExportDelivery exportDelivery = new ExportDelivery();
        exportDelivery.setHost("host");
        exportDelivery.setPort(12001);
        exportDelivery.setCurrentBytes(1000);
        exportDelivery.setTotalBytes(10000);
        exportDelivery.setPercentage(50d);
        exportDelivery.setState(ExportDelivery.State.WAITING);
        exportDelivery.setStatus("Copying file");
        return Arrays.asList(exportDelivery);
    }

    public void deleteReplication(String id) {
    }

    public void stopReplication(String id) {
    }
}
