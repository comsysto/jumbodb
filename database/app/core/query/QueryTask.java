package core.query;

import core.GlobalStatistics;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import play.Logger;

import java.io.*;
import java.net.Socket;

public class QueryTask implements Runnable {
    private Socket clientSocket;
    private int clientID = -1;
    private JumboSearcher jumboSearcher;
    private final ObjectMapper jsonMapper;
    private DatabaseQuerySession databaseQuerySession;

    public QueryTask(Socket s, int clientID, JumboSearcher jumboSearcher, ObjectMapper jsonMapper) {
        clientSocket = s;
        this.clientID = clientID;
        this.jumboSearcher = jumboSearcher;
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
                        JumboQuery searchQuery = jsonMapper.readValue(query, JumboQuery.class);
                        return jumboSearcher.findResultAndWriteIntoCallback(collection, searchQuery, new ResultCallback() {
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