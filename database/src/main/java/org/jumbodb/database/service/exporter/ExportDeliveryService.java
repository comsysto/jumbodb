package org.jumbodb.database.service.exporter;

import org.jumbodb.database.service.management.storage.StorageManagement;
import org.springframework.beans.factory.annotation.Required;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * @author Carsten Hufe
 */
public class ExportDeliveryService {
    private ExecutorService executorService;
    private List<ExportDelivery> replications = new CopyOnWriteArrayList<ExportDelivery>();
    private StorageManagement storageManagement;

    public void startReplication(StartReplication startReplication) {
        ExportDelivery exportDelivery = new ExportDelivery();
        exportDelivery.setActivate(startReplication.isActivate());
        exportDelivery.setCopyRateInBytesCompressed(0l);
        exportDelivery.setCurrentBytes(0l);
        exportDelivery.setTotalBytes(0l);
        exportDelivery.setHost(startReplication.getHost());
        exportDelivery.setPort(startReplication.getPort());
        exportDelivery.setId(UUID.randomUUID().toString());
        exportDelivery.setState(ExportDelivery.State.WAITING);
        exportDelivery.setStatus("Waiting for execution slot");
        exportDelivery.setDeliveryChunkKey(startReplication.getDeliveryChunkKey());
        exportDelivery.setVersion(startReplication.getVersion());
        replications.add(exportDelivery);
        executorService.submit(new ExportDeliveryTask(exportDelivery, storageManagement));
    }

    public List<ExportDelivery> getReplications() {
        return replications;
    }

    public void deleteReplication(String id) {
        ExportDelivery exportDelivery = findById(id);
        if(exportDelivery != null) {
            if(exportDelivery.getState() == ExportDelivery.State.RUNNING) {
                stopReplication(id);
            }
            replications.remove(exportDelivery);
        }
    }

    public void stopReplication(String id) {
        ExportDelivery exportDelivery = findById(id);
        if(exportDelivery != null) {
            exportDelivery.setState(ExportDelivery.State.ABORTED);
        }

    }

    private ExportDelivery findById(String id) {
        for (ExportDelivery replication : replications) {
            if(replication.getId().equals(id)) {
                return replication;
            }
        }
        return null;
    }

    @Required
    public void setStorageManagement(StorageManagement storageManagement) {
        this.storageManagement = storageManagement;
    }

    @Required
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
}
