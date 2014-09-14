package org.jumbodb.database.rest;

import org.apache.commons.io.IOUtils;
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
import org.jumbodb.database.service.queryutil.dto.ExplainResult;
import org.jumbodb.database.service.queryutil.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
@Controller
@RequestMapping(value = "/rest")
public class RestController {
    private StatusService statusService;
    private StorageManagement storageManagement;
    private ExportDeliveryService exportDeliveryService;
    private QueryUtilService queryUtilService;
    private ServletContext servletContext;

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    @ResponseBody
    public ServerInformation getStatus() {
        ServerInformation status = statusService.getStatus();
        Properties buildInfo = getBuildInfo();
        status.setVersion(checkNull(buildInfo.getProperty("Implementation-Version")));
        status.setGitRevision(checkNull(buildInfo.getProperty("Change")));
        status.setBuildDate(checkNull(buildInfo.getProperty("Build-Date")));
        return status;
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

    @RequestMapping(value = "/query/json/", method = RequestMethod.POST)
    @ResponseBody
    public QueryResult queryJson(@RequestBody String query) {
        return queryUtilService.findDocumentsByJsonQuery(query, -1);
    }

    @RequestMapping(value = "/query/json/defaultLimit", method = RequestMethod.POST)
    @ResponseBody
    public QueryResult queryJsonWithDefault(@RequestBody String query) {
        return queryUtilService.findDocumentsByJsonQuery(query, 20);
    }

    @RequestMapping(value = "/query/sql", method = RequestMethod.POST)
    @ResponseBody
    public QueryResult querySql(@RequestBody String query) {
        return queryUtilService.findDocumentsBySqlQuery(query, -1);
    }

    @RequestMapping(value = "/query/sql/explain", method = RequestMethod.POST)
    @ResponseBody
    public ExplainResult explainSql(@RequestBody String query) {
        return queryUtilService.explainSqlQuery(query);
    }

    @RequestMapping(value = "/query/sql/defaultLimit", method = RequestMethod.POST)
    @ResponseBody
    public QueryResult querySqlWithDefault(@RequestBody String query) {
        return queryUtilService.findDocumentsBySqlQuery(query, 20);
    }


    @RequestMapping(value = "/chunk/{chunkDeliveryKey}/activate", method = RequestMethod.POST)
    @ResponseBody
    public Message activateChunk(@PathVariable String chunkDeliveryKey) {
        storageManagement.activateChunk(chunkDeliveryKey);
        return new Message("activate", "Chunk '" + chunkDeliveryKey + "' has been activated.");
    }

    @RequestMapping(value = "/chunk/{chunkDeliveryKey}/inactivate", method = RequestMethod.POST)
    @ResponseBody
    public Message inactivateChunk(@PathVariable String chunkDeliveryKey) {
        storageManagement.inactivateChunk(chunkDeliveryKey);
        return new Message("inactivate", "Chunk '" + chunkDeliveryKey + "' has been inactivated.");
    }

    @RequestMapping(value = "/version/{chunkDeliveryKey}/{version}/activate", method = RequestMethod.POST)
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

    @RequestMapping(value = "/maintenance/databases/reload", method = RequestMethod.POST)
    @ResponseBody
    public Message triggerReloadDatabases() {
        try {
            storageManagement.triggerReloadDatabases();
            return new Message("success", "Databases meta information was reloaded successfully");
        } catch(Exception e) {
            return new Message("error", "Exception: " + e.getMessage());
        }
    }

    private String checkNull(String property) {
        if(property == null) {
            return "unknown";
        }
        return property;
    }


    private Properties getBuildInfo() {
        InputStream resourceAsStream = null;
        Properties props = new Properties();
        try {
            resourceAsStream = servletContext.getResourceAsStream("/META-INF/jumbodb.properties");
            if(resourceAsStream == null) {
                return props;
            }
            props.load(resourceAsStream);
        } catch (IOException e) {
            // do nothing
        } finally {
            IOUtils.closeQuietly(resourceAsStream);
        }
        return props;
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

    @Autowired
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }
}
