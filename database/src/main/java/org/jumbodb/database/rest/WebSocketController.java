package org.jumbodb.database.rest;

import org.jumbodb.database.rest.dto.Greeting;
import org.jumbodb.database.rest.dto.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created with IntelliJ IDEA.
 * User: alica
 * Date: 12/2/13
 * Time: 7:39 PM
 * To change this template use File | Settings | File Templates.
 */
@Controller
public class WebSocketController {

    @Autowired
    public WebSocketController(SimpMessageSendingOperations messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    private SimpMessageSendingOperations messagingTemplate;

    @MessageMapping("/hello")
    public void greeting(Message message) throws Exception {
        Thread.sleep(3000); // simulated delay
        Greeting greeting = new Greeting("Hello, " + message.getMessage() + "!");
        messagingTemplate.convertAndSend("/queue/greetings", greeting);
    }
}
