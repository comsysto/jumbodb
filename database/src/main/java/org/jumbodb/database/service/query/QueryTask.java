package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.JumboQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

public class QueryTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(QueryTask.class);

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
        log.info("QueryServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress().getHostName());

        try {
            databaseQuerySession = new DatabaseQuerySession(clientSocket, clientID);
            databaseQuerySession.query(new DatabaseQuerySession.QueryHandler() {
                @Override
                public int onQuery(String collection, byte[] query, final DatabaseQuerySession.ResultWriter resultWriter) {
                    try {
                        JumboQuery searchQuery = jsonMapper.readValue(query, JumboQuery.class);
                        return jumboSearcher.findResultAndWriteIntoCallback(collection, searchQuery, new ResultCallback() {
                            @Override
                            public void writeResult(byte[] result) throws IOException {
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