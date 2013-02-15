package core.query;


import core.query.OlchingDbSearcher;
import core.query.OlchingQuery;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.xerial.snappy.SnappyOutputStream;
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
    private OlchingDbSearcher olchingDbSearcher;
    private final ObjectMapper jsonMapper;

    public QueryServer(int port, File dataPath, File indexPath) {
        this.olchingDbSearcher = new OlchingDbSearcher(dataPath, indexPath);
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
                    ServerSocket m_ServerSocket = new ServerSocket(port);
                    int id = 0;
                    Logger.info("QueryServer started");
                    while (serverActive) {
                        Socket clientSocket = m_ServerSocket.accept();
                        serverSocketExecutor.submit(new QueryTask(clientSocket, id++, olchingDbSearcher, jsonMapper));
                    }
                    Logger.info("QueryServer stopped");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void restart() {
        olchingDbSearcher.restart();
    }

    public void stop() {
        serverActive = false;
        olchingDbSearcher.stop();
        serverSocketExecutor.shutdown();
    }


    public boolean isServerActive() {
        return serverActive;
    }
}



