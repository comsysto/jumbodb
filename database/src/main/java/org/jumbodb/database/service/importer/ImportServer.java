package org.jumbodb.database.service.importer;


import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.query.Restartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:41 PM
 */
public class ImportServer {
    private Logger log = LoggerFactory.getLogger(ImportServer.class);

    private boolean serverActive = false;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private Restartable queryServer;
    private JumboConfiguration config;


    public ImportServer(JumboConfiguration config, Restartable queryServer) {
        this.config = config;
        this.queryServer = queryServer;
    }

    public void start() throws Exception {
        serverActive = true;
        new Thread() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(config.getImportPort());
                    int id = 0;
                    log.info("ImportServer started");
                    while (isServerActive()) {
                        Socket clientSocket = serverSocket.accept();
                        executorService.submit(new ImportTask(clientSocket, id++, config.getDataPath(), config.getIndexPath(), queryServer));
                    }
                    serverSocket.close();
                    log.info("ImportServer stopped");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }

    public void stop() {
        serverActive = false;
        executorService.shutdown();
    }

    public boolean isServerActive() {
        return serverActive;
    }
}



