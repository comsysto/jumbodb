package org.jumbodb.database.rest.dto;

/**
 * Created with IntelliJ IDEA.
 * User: alica
 * Date: 12/2/13
 * Time: 2:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class Greeting {

    private String content;

    public Greeting(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

}
