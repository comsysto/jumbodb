package org.jumbodb.database.service.query;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:41 PM
 */
public class QueryServer {
    private Logger log = LoggerFactory.getLogger(QueryServer.class);

    private boolean serverActive = false;
    private ExecutorService serverSocketExecutor = Executors.newCachedThreadPool();
    private JumboSearcher jumboSearcher;
    private final ObjectMapper jsonMapper;
    private JumboConfiguration config;
    private  ServerSocket serverSocket;

    public QueryServer(JumboConfiguration config, JumboSearcher jumboSearcher) {
        this.jumboSearcher = jumboSearcher;
        this.config = config;
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public void start() throws Exception {
        serverActive = true;
        serverSocket = new ServerSocket(config.getQueryPort());
        serverSocketExecutor.submit(new Thread() {
            @Override
            public void run() {
                try {

                    int id = 0;
                    log.info("QueryServer started");
                    log.info("Configuration " + config.toString());
                    while (serverActive) {
                        Socket clientSocket = serverSocket.accept();
                        serverSocketExecutor.submit(new QueryTask(clientSocket, id++, jumboSearcher, jsonMapper));
                    }
//                    serverSocket.close();
                    log.info("QueryServer stopped");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

//    @Override
//    public void restart() {
//        jumboSearcher.restart();
//    }

    public void stop() throws IOException {
        serverSocket.close();
        serverActive = false;
        serverSocketExecutor.shutdown();
    }


    public boolean isServerActive() {
        return serverActive;
    }
}



