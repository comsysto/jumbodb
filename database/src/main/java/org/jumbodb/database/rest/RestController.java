package org.jumbodb.database.rest;

import org.jumbodb.database.rest.dto.Message;
import org.jumbodb.database.service.exporter.ExportDelivery;
import org.jumbodb.database.service.exporter.ExportDeliveryService;
import org.jumbodb.database.service.exporter.StartReplication;
import org.jumbodb.database.service.management.status.StatusService;
import org.jumbodb.database.service.management.status.dto.ServerInformation;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.management.storage.dto.collections.JumboCollection;
import org.jumbodb.database.service.management.storage.dto.deliveries.ChunkedDeliveryVersion;
import org.jumbodb.database.service.management.storage.dto.maintenance.TemporaryFiles;
import org.jumbodb.database.service.management.storage.dto.queryutil.QueryUtilCollection;
import org.jumbodb.database.service.queryutil.QueryUtilService;
import org.jumbodb.database.service.queryutil.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:45 AM
 */
@Controller
@RequestMapping(value = "/rest")
public class RestController {
    private StatusService statusService;
    private StorageManagement storageManagement;
    private ExportDeliveryService exportDeliveryService;
    private QueryUtilService queryUtilService;

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    @ResponseBody
    public ServerInformation getStatus() {
        return statusService.getStatus();
    }

// CARSTEN method to active and disable chunk!
// CARSTEN separate labels for disabled chunks

    @RequestMapping(value = "/collections", method = RequestMethod.GET)
    @ResponseBody
    public List<JumboCollection> getJumboCollections() {
        return storageManagement.getJumboCollections();
    }

    @RequestMapping(value = "/deliveries", method = RequestMethod.GET)
    @ResponseBody
    public List<ChunkedDeliveryVersion> getChunkedDeliveryVersions() {
        return storageManagement.getChunkedDeliveryVersions();
    }

    @RequestMapping(value = "/query/collections", method = RequestMethod.GET)
    @ResponseBody
    public List<QueryUtilCollection> getQueryableCollections() {
        return storageManagement.findQueryableCollections();
    }

    @RequestMapping(value = "/query/{collection}/", method = RequestMethod.POST)
    @ResponseBody
    public QueryResult query(@PathVariable String collection, @RequestBody String query) {
        return queryUtilService.findDocumentsByQuery(collection, query, -1);
    }

    @RequestMapping(value = "/query/{collection}/defaultLimit", method = RequestMethod.POST)
    @ResponseBody
    public QueryResult queryWithDefault(@PathVariable String collection, @RequestBody String query) {
        return queryUtilService.findDocumentsByQuery(collection, query, 20);
    }


    @RequestMapping(value = "/chunk/{chunkDeliveryKey}/activate", method = RequestMethod.POST)
    @ResponseBody
    public Message activateChunk(@PathVariable String chunkDeliveryKey) {
        // CARSTEN unit test
        // CARSTEN wire no frontend
        storageManagement.activateChunk(chunkDeliveryKey);
        return new Message("activate", "Chunk '" + chunkDeliveryKey + "' has been activated.");
    }

    @RequestMapping(value = "/chunk/{chunkDeliveryKey}/inactivate", method = RequestMethod.POST)
    @ResponseBody
    public Message inactivateChunk(@PathVariable String chunkDeliveryKey) {
        // CARSTEN unit test
        // CARSTEN wire no frontend
        storageManagement.inactivateChunk(chunkDeliveryKey);
        return new Message("inactivate", "Chunk '" + chunkDeliveryKey + "' has been inactivated.");
    }

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}", method = RequestMethod.PUT)
    @ResponseBody
    public Message activateChunkedVersion(@PathVariable String chunkDeliveryKey, @PathVariable String version) {
        storageManagement.activateChunkedVersion(chunkDeliveryKey, version);
        return new Message("activate", "Version '" + version + "' for chunk '" + chunkDeliveryKey + "' has been activated.");
    }

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}", method = RequestMethod.DELETE)
    @ResponseBody
    public Message deleteChunkedVersion(@PathVariable String chunkDeliveryKey, @PathVariable String version) {
        storageManagement.deleteChunkedVersion(chunkDeliveryKey, version);
        return new Message("delete", "Version '" + version + "' for chunk '" + chunkDeliveryKey + "' has been deleted.");
    }

    @RequestMapping(value = "/replication", method = RequestMethod.POST)
    @ResponseBody
    public Message startReplication(@RequestBody StartReplication startReplication) {
        exportDeliveryService.startReplication(startReplication);
        return new Message("success", "Replication started.");
    }


    @RequestMapping(value = "/replication", method = RequestMethod.GET)
    @ResponseBody
    public List<ExportDelivery> getReplications() {
        return exportDeliveryService.getReplications();
    }

    @RequestMapping(value = "/replication/{id}", method = RequestMethod.PUT)
    @ResponseBody
    public Message abortReplications(@PathVariable String id) {
        exportDeliveryService.stopReplication(id);
        return new Message("abort", "Replication was aborted.");
    }

    @RequestMapping(value = "/replication/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    public Message deleteReplications(@PathVariable String id) {
        exportDeliveryService.deleteReplication(id);
        return new Message("delete", "Replication was deleted.");
    }

    @RequestMapping(value = "/maintenance/tmp/info", method = RequestMethod.GET)
    @ResponseBody
    public TemporaryFiles maintenanceInfo() {
        return storageManagement.getMaintenanceTemporaryFilesInfo();
    }

    @RequestMapping(value = "/maintenance/tmp/cleanup", method = RequestMethod.DELETE)
    @ResponseBody
    public Message maintenanceCleanup() {
        try {
            storageManagement.maintenanceCleanupTemporaryFiles();
            return new Message("success", "Deleted temporary files of aborted deliveries!");
        } catch(Exception e) {
            return new Message("error", "Exception: " + e.getMessage());
        }
    }


    @Autowired
    public void setStatusService(StatusService statusService) {
        this.statusService = statusService;
    }

    @Autowired
    public void setStorageManagement(StorageManagement storageManagement) {
        this.storageManagement = storageManagement;
    }

    @Autowired
    public void setExportDeliveryService(ExportDeliveryService exportDeliveryService) {
        this.exportDeliveryService = exportDeliveryService;
    }

    @Autowired
    public void setQueryUtilService(QueryUtilService queryUtilService) {
        this.queryUtilService = queryUtilService;
    }
}
