package core.query;


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import play.Logger;


/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:41 PM
 */
public class QueryServer {
    private boolean serverActive = false;
    private ExecutorService serverSocketExecutor = Executors.newCachedThreadPool();
    private int port;
    private JumboSearcher jumboSearcher;
    private final ObjectMapper jsonMapper;

    public QueryServer(int port, File dataPath, File indexPath) {
        this.jumboSearcher = new JumboSearcher(dataPath, indexPath);
        this.port = port;
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public void start() throws Exception {
        serverActive = true;
        serverSocketExecutor.submit(new Thread() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(port);
                    int id = 0;
                    Logger.info("QueryServer started");
                    while (serverActive) {
                        Socket clientSocket = serverSocket.accept();
                        serverSocketExecutor.submit(new QueryTask(clientSocket, id++, jumboSearcher, jsonMapper));
                    }
                    serverSocket.close();
                    Logger.info("QueryServer stopped");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void restart() {
        jumboSearcher.restart();
    }

    public void stop() {
        serverActive = false;
        jumboSearcher.stop();
        serverSocketExecutor.shutdown();
    }


    public boolean isServerActive() {
        return serverActive;
    }
}



