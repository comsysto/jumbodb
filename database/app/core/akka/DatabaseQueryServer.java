package core.akka;

import akka.actor.*;
import akka.routing.RoundRobinRouter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import play.Logger;
import play.libs.Akka;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2/5/13
 * Time: 2:45 PM
 */
public class DatabaseQueryServer {
    private static final int PROTOCOL_VERSION = 1;
    private boolean serverActive = false;
    private int port;
    private ActorSystem actorSystem;
    private int id = 0;

    private Map<String, DataCollection> dataCollections = new HashMap<String, DataCollection>();
    private ObjectMapper jsonMapper;
    private ActorRef searchIndexFileActor;

    public DatabaseQueryServer(int port, final File dataPath, final File indexPath) {
        this.port = port;
        File[] files = indexPath.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        if(files == null) {
            return;
        }
        for (File collectionIndexFolder  : files) {
            String collectionName = collectionIndexFolder.getName();
            DataCollection dataCollection = createDataCollection(collectionIndexFolder, new File(dataPath.getAbsolutePath() + "/" + collectionName + "/"));
            dataCollections.put(collectionName, dataCollection);
        }
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.actorSystem = Akka.system();
        searchIndexFileActor = actorSystem.actorOf(new Props(SearchIndexFileActor.class).withRouter(new RoundRobinRouter(20)));
        Logger.info("IndexedFileSearcher initialized for " + indexPath.getAbsolutePath());

    }

    private DataCollection createDataCollection(File collectionIndexFolder, File collectionDataFolder) {
        DataCollection col = new DataCollection();
        File[] indexFolders = collectionIndexFolder.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        for (File indexFolder : indexFolders) {
            File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".odx"));
            for (File indexFile : indexFiles) {
                if(indexFile.length() > 0) {
                    col.indexFiles.put(indexFolder.getName(), createIndexFileDescription(indexFile));
                }
            }
        }
        File[] dataFiles = collectionDataFolder.listFiles();
        for (File dataFile : dataFiles) {
            col.dataFiles.put(dataFile.getName().hashCode(), dataFile);
        }
        return col;
    }


    private IndexFile createIndexFileDescription(File indexFile) {
        IndexFile res = new IndexFile();
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(indexFile, "r");
            res.indexFile = indexFile;
            res.fromHash = raf.readInt();
            raf.seek(raf.length() - 16);
            res.toHash = raf.readInt();
            // CARSTEN I know unsafe ...  but to lazy
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(raf);
        }
        return res;
    }


    public void start() throws Exception {
        serverActive = true;
        new Thread() {
            @Override
            public void run() {
                try {
                    ServerSocket m_ServerSocket = new ServerSocket(port);
                    Logger.info("DatabaseQueryServer started");
                    while (serverActive) {
                        final Socket clientSocket = m_ServerSocket.accept();
                        ActorRef actor = actorSystem.actorOf(new Props(new UntypedActorFactory() {
                            public UntypedActor create() {
                                return new DatabaseQueryActor(clientSocket, id++, jsonMapper, searchIndexFileActor, dataCollections);
                            }
                        }));
                        actor.tell(DatabaseQueryMessage.QUERY);
                    }
                    Logger.info("DatabaseQueryServer stopped");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }

    public void stop() {
        serverActive = false;
    }
}
