package core.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import core.query.OlchingDbSearcher;
import core.query.OlchingQuery;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.xerial.snappy.SnappyOutputStream;
import play.Logger;

import java.io.*;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 3:14 PM
 */
public class DatabaseQueryActor extends UntypedActor {
    private static final int PROTOCOL_VERSION = 1;
    private Socket clientSocket;
    private int clientID;
    private final ObjectMapper jsonMapper;
    private ActorRef searchIndexFileActor;
    private Map<String, DataCollection> dataCollections;
    private List<SearchIndexFileResultMessage> indexResult = new LinkedList<SearchIndexFileResultMessage>();
    InputStream inputStream = null;
    DataInputStream dataInputStream = null;
    OutputStream outputStream = null;
    DataOutputStream dataOutputStream = null;
    BufferedOutputStream bufferedOutputStream = null;
    SnappyOutputStream snappyOutputStream = null;
    private OlchingQuery searchQuery;

    public DatabaseQueryActor(Socket s, int i, ObjectMapper jsonMapper, ActorRef searchIndexFileActor, Map<String, DataCollection> dataCollections) {
        clientSocket = s;
        clientID = i;
        this.jsonMapper = jsonMapper;
        this.searchIndexFileActor = searchIndexFileActor;
        this.dataCollections = dataCollections;
        try {
            inputStream = clientSocket.getInputStream();
            dataInputStream = new DataInputStream(inputStream);
            outputStream = clientSocket.getOutputStream();
            bufferedOutputStream = new BufferedOutputStream(outputStream);
            snappyOutputStream = new SnappyOutputStream(bufferedOutputStream);
            dataOutputStream = new DataOutputStream(snappyOutputStream);
            dataOutputStream.writeUTF("ok");
            dataOutputStream.writeInt(PROTOCOL_VERSION);
            dataOutputStream.flush();
            snappyOutputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void onReceive(Object message) {
        Logger.info("QueryServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress().getHostName());

        if(message instanceof DatabaseQueryMessage) {
            DatabaseQueryMessage databaseQueryMessage = (DatabaseQueryMessage) message;
            if(databaseQueryMessage == DatabaseQueryMessage.QUERY) {
                try {

                    Logger.info("before CMD: ");
                    String cmd = dataInputStream.readUTF();
                    Logger.info("CMD: " + cmd);
                    if(cmd.equals(":cmd:query")) {
                        String collection = dataInputStream.readUTF();
                        Logger.info("Collection: " + collection);
                        String jsonQueryString = dataInputStream.readUTF();
                        searchQuery = jsonMapper.readValue(jsonQueryString, OlchingQuery.class);
                        Logger.info("Query: " + searchQuery.toString());
                        long start = System.currentTimeMillis();
                        ActorRef actor = context().actorOf(new Props(new UntypedActorFactory() {
                            public UntypedActor create() {
                                return new SearchIndexActor(searchIndexFileActor);
                            }
                        }));

                        List<OlchingQuery.IndexComparision> indexComparision = searchQuery.getIndexComparision();
                        for (OlchingQuery.IndexComparision comparision : indexComparision) {
                            SearchIndexQueryMessage m = new SearchIndexQueryMessage(dataCollections.get(collection), comparision);
                            actor.tell(m);
                        }

                        // CARSTEN manage all the state from here!

                        // CARSTEN search indexes
                        // instance SearchIndexActor
//                        databaseSearcher.tell(new SearchMessage(collection, searchQuery));



                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            } else  if(databaseQueryMessage == DatabaseQueryMessage.CLOSE) {
                try {
                    snappyOutputStream.flush();
                    dataOutputStream.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                IOUtils.closeQuietly(dataInputStream);
                IOUtils.closeQuietly(bufferedOutputStream);
                IOUtils.closeQuietly(snappyOutputStream);
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(outputStream);
                IOUtils.closeQuietly(dataOutputStream);
                IOUtils.closeQuietly(clientSocket);
                context().stop(getSelf());
            }

        }
        if(message instanceof SearchIndexFileResultMessage) {
            SearchIndexFileResultMessage searchIndexFileResultMessage = (SearchIndexFileResultMessage) message;
            indexResult.add(searchIndexFileResultMessage);
            if(indexResult.size() == searchQuery.getIndexComparision().size()) {
                System.out.println("all indexes found");
                IOUtils.closeQuietly(clientSocket);

                // flat list
                // fire dataset search
            }
            IOUtils.closeQuietly(clientSocket);


        }
        if(message instanceof ResultMessage) {
            ResultMessage resultMessage = (ResultMessage) message;
            try {
                dataOutputStream.writeUTF(resultMessage.getResult());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
