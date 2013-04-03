package org.jumbodb.database.rest.dto;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 4/3/13
 * Time: 11:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class Helloworld {
    private String firstname;
    private String message;

    public Helloworld() {
    }

    public Helloworld(String firstname, String message) {
        this.firstname = firstname;
        this.message = message;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
