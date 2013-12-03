package org.jumbodb.database.rest.dto;

/**
 * User: carsten
 * Date: 4/4/13
 * Time: 7:58 PM
 */
public class Message {
    private String type;
    private String message;

    public Message() {
    }

    public Message(String type, String message) {
        this.type = type;
        this.message = message;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Message{" +
                "type='" + type + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
