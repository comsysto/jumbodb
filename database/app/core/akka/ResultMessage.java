package core.akka;

import core.query.OlchingQuery;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 3:24 PM
 */
public class ResultMessage {
    private String result;

    public ResultMessage(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }
}
