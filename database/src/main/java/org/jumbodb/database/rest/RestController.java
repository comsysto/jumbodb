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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:45 AM
 */
@Controller
@RequestMapping(value = "/rest")
public class RestController {
    @Autowired
    private StatusService statusService;
    @Autowired
    private StorageManagement storageManagement;
    @Autowired
    private ExportDeliveryService exportDeliveryService;

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    @ResponseBody
    public ServerInformation getStatus() {
        return statusService.getStatus();
    }


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

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}", method = RequestMethod.PUT)
    @ResponseBody
    public Message activateChunkedVersionForAllCollections(@PathVariable String chunkDeliveryKey, @PathVariable String version) {
        storageManagement.activateChunkedVersionForAllCollections(chunkDeliveryKey, version);
        return new Message("activate", "Version '" + version + "' for chunk '" + chunkDeliveryKey + "' on all collections has been activated.");
    }

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}/{collection}", method = RequestMethod.PUT)
    @ResponseBody
    public Message activateChunkedVersionInCollection(@PathVariable String chunkDeliveryKey, @PathVariable String version, @PathVariable String collection) {
        storageManagement.activateChunkedVersionInCollection(chunkDeliveryKey, version, collection);
        return new Message("activate", "Version '" + version + "' for '" + collection + "' has been activated.");
    }

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}", method = RequestMethod.DELETE)
    @ResponseBody
    public Message deleteChunkedVersionForAllCollections(@PathVariable String chunkDeliveryKey, @PathVariable String version) {
        storageManagement.deleteChunkedVersionForAllCollections(chunkDeliveryKey, version);
        return new Message("delete", "Version '" + version + "' for chunk '" + chunkDeliveryKey + "' (incl. all collections) has been deleted.");
    }

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}/{collection}", method = RequestMethod.DELETE)
    @ResponseBody
    public Message deleteChunkedVersionInCollection(@PathVariable String chunkDeliveryKey, @PathVariable String version, @PathVariable String collection) {
        storageManagement.deleteChunkedVersionInCollection(chunkDeliveryKey, version, collection);
        return new Message("delete", "Version '" + version + "' for '" + collection + "' has been deleted.");
    }

    @RequestMapping(value = "/collection/{collection}", method = RequestMethod.DELETE)
    @ResponseBody
    public Message deleteCompleteCollection(@PathVariable String collection) {
        storageManagement.deleteCompleteCollection(collection);
        return new Message("delete", "Complete collection '" + collection + "' has been deleted.");
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


}
