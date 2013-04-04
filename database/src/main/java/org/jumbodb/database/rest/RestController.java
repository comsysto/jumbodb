package org.jumbodb.database.rest;

import org.jumbodb.database.service.management.status.StatusService;
import org.jumbodb.database.service.management.status.dto.ServerInformation;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.management.storage.dto.collections.JumboCollection;
import org.jumbodb.database.service.management.storage.dto.deliveries.ChunkedDeliveryVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

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
}
