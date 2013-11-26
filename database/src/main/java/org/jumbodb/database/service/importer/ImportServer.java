package org.jumbodb.database.service.importer;


import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.query.JumboSearcher;
import org.jumbodb.database.service.query.data.DataStrategyManager;
import org.jumbodb.database.service.query.index.IndexStrategyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:41 PM
 */
public class ImportServer {
    private Logger log = LoggerFactory.getLogger(ImportServer.class);

    private boolean serverActive = false;
    private ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    private JumboConfiguration config;
    private JumboSearcher jumboSearcher;
    private DataStrategyManager dataStrategyManager;
    private IndexStrategyManager indexStrategyManager;
    private ServerSocket serverSocket;


    public ImportServer(JumboConfiguration config, JumboSearcher jumboSearcher, DataStrategyManager dataStrategyManager, IndexStrategyManager indexStrategyManager) {
        this.config = config;
        this.jumboSearcher = jumboSearcher;
        this.dataStrategyManager = dataStrategyManager;
        this.indexStrategyManager = indexStrategyManager;
    }

    public void start() throws Exception {
        serverActive = true;
        serverSocket = new ServerSocket(config.getImportPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    int id = 0;
                    log.info("ImportServer started");
                    while (isServerActive()) {
                        Socket clientSocket = serverSocket.accept();
                        executorService.submit(new ImportTask(clientSocket, id++, config.getDataPath(), config.getIndexPath(), jumboSearcher, dataStrategyManager, indexStrategyManager));
                    }
                    log.info("ImportServer stopped");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }

    public void stop() throws IOException {
        serverActive = false;
        executorService.shutdown();
        serverSocket.close();
    }

    public boolean isImportRunning() {
        return executorService.getActiveCount() > 0;
    }

    public boolean isServerActive() {
        return serverActive;
    }
}



