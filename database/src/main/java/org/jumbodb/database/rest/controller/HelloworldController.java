package org.jumbodb.database.rest.controller;

import org.jumbodb.database.rest.dto.Helloworld;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:45 AM
 */
@Controller
public class HelloworldController {
    @RequestMapping(value = "/helloworld", method = RequestMethod.GET)
    @ResponseBody
    public Helloworld getHelloWorld() {
        return new Helloworld("Carsten", "it works");
    }
}
