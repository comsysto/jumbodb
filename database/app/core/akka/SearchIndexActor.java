package core.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinRouter;
import com.google.common.collect.HashMultimap;
import play.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 2:58 PM
 */
// CARSTEN der SearchIndexActor sollte user spezifisch sein
public class SearchIndexActor extends UntypedActor {
    private ActorRef searchIndexFileActor;

    private int numberOfRequiredMessages = -1;
    private int numberOfCurrentMessages = 0;
    private Set<FileOffset> result = new HashSet<FileOffset>();

    public SearchIndexActor(ActorRef searchIndexFileActor) {
        this.searchIndexFileActor = searchIndexFileActor;
    }

    @Override
    public void onReceive(Object message) {
        if(message instanceof SearchIndexQueryMessage) {
            SearchIndexQueryMessage searchIndexQueryMessage = (SearchIndexQueryMessage) message;
            long start = System.currentTimeMillis();
            HashMultimap<File, Integer> groupedByIndexFile = groupByIndexFile(searchIndexQueryMessage);
            numberOfRequiredMessages = groupedByIndexFile.keySet().size();
            for (File indexFile : groupedByIndexFile.keySet()) {
                searchIndexFileActor.tell(new SearchIndexFileQueryMessage(indexFile, groupedByIndexFile.get(indexFile)));
            }
//            Logger.info("Time for search one complete index offsets " + query.getValues().size() + ": " + (System.currentTimeMillis() - start) + "ms");
        }
        if(message instanceof SearchIndexFileResultMessage) {
            SearchIndexFileResultMessage searchIndexFileResultMessage = (SearchIndexFileResultMessage) message;
            // CARSTEN how to handle
            numberOfCurrentMessages++;
            result.addAll(searchIndexFileResultMessage.getFileOffsets());
            if(numberOfCurrentMessages == numberOfRequiredMessages) {
                sender().tell(new SearchIndexFileResultMessage(result));
            }
        }
    }

    private HashMultimap<File, Integer> groupByIndexFile(SearchIndexQueryMessage searchIndexQueryMessage) {
        Set<IndexFile> indexFiles = searchIndexQueryMessage.getDataCollection().indexFiles.get(searchIndexQueryMessage.getQuery().getName());
        // CARSTEN this one is very slow
        HashMultimap<File, Integer> groupByIndexFile = HashMultimap.create();
        for (IndexFile indexFile : indexFiles) {
            for (String obj : searchIndexQueryMessage.getQuery().getValues()) {
                int hash = obj.hashCode();
                if(hash >= indexFile.fromHash && hash <= indexFile.toHash) {
                    groupByIndexFile.put(indexFile.indexFile, hash);
                }

            }
        }
        return groupByIndexFile;
    }
}
