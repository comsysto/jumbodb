package core.query;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.xerial.snappy.SnappyOutputStream;
import play.Logger;

import java.io.*;
import java.net.Socket;

public class QueryTask implements Runnable {
    private static final int PROTOCOL_VERSION = 2;


    private Socket clientSocket;
    private int clientID = -1;
    private OlchingDbSearcher olchingDbSearcher;
    private final ObjectMapper jsonMapper;
    private DatabaseQuerySession databaseQuerySession;

    public QueryTask(Socket s, int clientID, OlchingDbSearcher olchingDbSearcher, ObjectMapper jsonMapper) {
        clientSocket = s;
        this.clientID = clientID;
        this.olchingDbSearcher = olchingDbSearcher;
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
                        OlchingQuery searchQuery = jsonMapper.readValue(query, OlchingQuery.class);
                        return olchingDbSearcher.findResultAndWriteIntoCallback(collection, searchQuery, new ResultCallback() {
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