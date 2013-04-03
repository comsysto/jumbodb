package org.jumbodb.database.rest;

import org.jumbodb.database.service.management.status.StatusService;
import org.jumbodb.database.service.management.status.dto.ServerInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.DateFormat;
import java.text.NumberFormat;

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

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    @ResponseBody
    public ServerInformation getStatus() {
        return statusService.getStatus();
    }
}
