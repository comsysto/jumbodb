package core.akka;

import akka.actor.UntypedActor;
import org.apache.commons.io.IOUtils;
import play.Logger;

import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 2:50 PM
 */
public class SearchIndexFileActor extends UntypedActor {
    @Override
    public void onReceive(Object message) {
        if(message instanceof SearchIndexFileQueryMessage) {
            SearchIndexFileQueryMessage searchIndexFileQueryMessage = (SearchIndexFileQueryMessage) message;
            long start = System.currentTimeMillis();
            RandomAccessFile raf = null;
            int resultSize = 0;
            try {
                raf = new RandomAccessFile(searchIndexFileQueryMessage.getIndexFile(), "r");
                for (Integer hash : searchIndexFileQueryMessage.getHashes()) {
                    // CARSTEN kann optimiert werden innerhalb von findOffset wird immer ein neuer stream aufgemacht
                    sender().tell(new SearchIndexFileResultMessage(findOffsetForHashCode(raf, hash, searchIndexFileQueryMessage.getIndexFile())));
                    resultSize++;
                }
            }
            catch(IOException e) {
                throw new RuntimeException(e);
            }
            finally {
                IOUtils.closeQuietly(raf);
            }
            Logger.info("Time for search one index part-file offsets " + resultSize + ": " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    private Set<FileOffset> findOffsetForHashCode(RandomAccessFile indexRaf, int searchHash, File indexFile) throws IOException {
        long length = indexRaf.length();
        long fromOffset = 0l;
        long toOffset = length;
        while(toOffset - fromOffset != 16) {
            long offsetDiff = (toOffset - fromOffset) / 2;
            long currentOffset = offsetDiff - (offsetDiff % 16) + fromOffset;
            indexRaf.seek(currentOffset);
            int currentHash = indexRaf.readInt();
            if(currentHash == searchHash) {
                Set<FileOffset> result = new HashSet<FileOffset>();
                long jumpBackDatasets = 50;
                int fileHash =  indexRaf.readInt();
                long offset = indexRaf.readLong();

                // jump back to get all data sets
                while(currentHash == searchHash && currentOffset != 0) {
                    currentOffset -= (jumpBackDatasets * 16);
                    currentOffset = currentOffset > 0 ? currentOffset : 0;
                    indexRaf.seek(currentOffset);
                    currentHash = indexRaf.readInt();
                    indexRaf.skipBytes(12);
//                        fileHash =  indexRaf.readInt();
//                        offset = indexRaf.readLong();
                    jumpBackDatasets *= 2;
                }

                // new buffered input stream is faster for sequential read, then random access
                // now get forward
                FileInputStream fis = new FileInputStream(indexFile);
                BufferedInputStream bis = new BufferedInputStream(fis);
                DataInputStream dis = new DataInputStream(bis);
                try {
                    dis.skip(indexRaf.getFilePointer());
                    boolean foundItAgain = false;
                    while((!foundItAgain || currentHash == searchHash && dis.available() > 0)) {  //
                        currentHash = dis.readInt();
                        fileHash =  dis.readInt();
                        offset = dis.readLong();
                        if(currentHash == searchHash) {
                            foundItAgain = true;
                            result.add(new FileOffset(fileHash, offset));
                        }
                    }
                } finally {
                    IOUtils.closeQuietly(dis);
                    IOUtils.closeQuietly(fis);
                    IOUtils.closeQuietly(bis);
                }
                return result;
            }
            else if(currentHash < searchHash) {
                fromOffset = currentOffset;
            }
            else {
                toOffset = currentOffset;
            }
        }
        return Collections.emptySet();
    }
}
