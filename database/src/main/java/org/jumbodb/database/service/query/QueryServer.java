package org.jumbodb.database.service.query;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:41 PM
 */
public class QueryServer {
    private Logger log = LoggerFactory.getLogger(QueryServer.class);

    private boolean serverActive = false;
    private ExecutorService serverSocketExecutor = Executors.newCachedThreadPool();
    private ExecutorService queryTaskTimeoutExecutor = Executors.newCachedThreadPool();
    private JumboSearcher jumboSearcher;
    private final ObjectMapper jsonMapper;
    private JumboConfiguration config;
    private  ServerSocket serverSocket;
    private long queryTimeoutInSeconds;

    public QueryServer(JumboConfiguration config, JumboSearcher jumboSearcher, long queryTimeoutInSeconds) {
        this.jumboSearcher = jumboSearcher;
        this.config = config;
        this.queryTimeoutInSeconds = queryTimeoutInSeconds;
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public void start() throws Exception {
        serverActive = true;
        serverSocket = new ServerSocket(config.getQueryPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    int id = 0;
                    log.info("QueryServer started");
                    log.info("Configuration " + config.toString());
                    while (serverActive) {
                        Socket clientSocket = serverSocket.accept();
                        serverSocketExecutor.submit(new QueryTask(clientSocket, id++, jumboSearcher, jsonMapper, queryTaskTimeoutExecutor, queryTimeoutInSeconds));
                    }
                    log.info("QueryServer stopped");
                } catch (Exception e) {
                    log.error("Unknown error: ", e);
                } finally {
                    serverActive = false;
                }
            }
        }.start();
    }

    protected void setServerSocketExecutor(ExecutorService serverSocketExecutor) {
        this.serverSocketExecutor = serverSocketExecutor;
    }

    public void stop() throws IOException {
        serverActive = false;
        serverSocket.close();
        queryTaskTimeoutExecutor.shutdown();
        serverSocketExecutor.shutdown();
    }


    public boolean isServerActive() {
        return serverActive;
    }
}



