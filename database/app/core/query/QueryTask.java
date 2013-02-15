package core.query;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import play.Logger;

import java.io.*;
import java.net.Socket;

public class QueryTask implements Runnable {
    private static final int PROTOCOL_VERSION = 2;


    private Socket clientSocket;
    private int clientID = -1;
    private DumboSearcher dumboSearcher;
    private final ObjectMapper jsonMapper;
    private DatabaseQuerySession databaseQuerySession;

    public QueryTask(Socket s, int clientID, DumboSearcher dumboSearcher, ObjectMapper jsonMapper) {
        clientSocket = s;
        this.clientID = clientID;
        this.dumboSearcher = dumboSearcher;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void run() {
        Logger.info("QueryServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress().getHostName());

        try {
            databaseQuerySession = new DatabaseQuerySession(clientSocket, clientID);
            databaseQuerySession.query(new DatabaseQuerySession.QueryHandler() {
                @Override
                public int onQuery(String collection, String query, final DatabaseQuerySession.ResultWriter resultWriter) {
                    try {
                        DumboQuery searchQuery = jsonMapper.readValue(query, DumboQuery.class);
                        return dumboSearcher.findResultAndWriteIntoCallback(collection, searchQuery, new ResultCallback() {
                            @Override
                            public void writeResult(String result) throws IOException {
                                resultWriter.writeResult(result);
                            }
                        });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(databaseQuerySession);
            IOUtils.closeQuietly(clientSocket);
        }
    }
}