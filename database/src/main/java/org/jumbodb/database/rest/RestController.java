package org.jumbodb.database.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.rest.dto.Message;
import org.jumbodb.database.service.exporter.ExportDelivery;
import org.jumbodb.database.service.exporter.ExportDeliveryService;
import org.jumbodb.database.service.exporter.StartReplication;
import org.jumbodb.database.service.management.status.StatusService;
import org.jumbodb.database.service.management.status.dto.ServerInformation;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.management.storage.dto.collections.JumboCollection;
import org.jumbodb.database.service.management.storage.dto.deliveries.ChunkedDeliveryVersion;
import org.jumbodb.database.service.management.storage.dto.queryutil.QueryUtilCollection;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.queryutil.QueryUtilService;
import org.jumbodb.database.service.queryutil.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
        return queryUtilService.findDocumentsByQuery(collection, query);
    }

    @RequestMapping(value = "/query/{collection}/stream", method = RequestMethod.POST)
    public void queryStream(@PathVariable String collection, @RequestBody String query, final HttpServletResponse response) {
        final AtomicInteger counter = new AtomicInteger(0);
        queryUtilService.findDocumentsByQuery(collection, query, new ResultCallback() {
             @Override
             public void writeResult(byte[] result) throws IOException {
                synchronized (response) {
                    response.getWriter().println(new String(result, "UTF-8"));
                    response.getWriter().flush();
                    counter.incrementAndGet();
                }
             }

             @Override
             public boolean needsMore(JumboQuery jumboQuery) throws IOException {
                 return counter.get() < jumboQuery.getLimit();
             }
         });
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
