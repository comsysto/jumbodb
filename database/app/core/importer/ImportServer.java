package core.importer;


import core.query.OlchingDbSearcher;
import core.query.QueryServer;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import play.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:41 PM
 */
public class ImportServer {
    private boolean serverActive = false;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private int port;
    private File dataPath;
    private File indexPath;
    private QueryServer queryServer;


    public ImportServer(int port, File dataPath, File indexPath, QueryServer queryServer) {
        this.port = port;
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.queryServer = queryServer;
    }

    public void start() throws Exception {
        serverActive = true;
        new Thread() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(port);
                    int id = 0;
                    Logger.info("ImportServer started");
                    while (isServerActive()) {
                        Socket clientSocket = serverSocket.accept();
                        executorService.submit(new ImportTask(clientSocket, id++, dataPath, indexPath, queryServer));
                    }
                    Logger.info("ImportServer stopped");
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



