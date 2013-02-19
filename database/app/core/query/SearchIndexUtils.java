package core.query;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.IOUtils;
import play.Logger;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 5:40 PM
 */
public class SearchIndexUtils {
    public static Set<FileOffset> searchOffsetsByHashes(File indexFile, Set<Integer> hashes) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        Set<FileOffset> result = new HashSet<FileOffset>();
        try {
            raf = new RandomAccessFile(indexFile, "r");
            for (Integer hash : hashes) {
                // CARSTEN kann optimiert werden innerhalb von findOffset wird immer ein neuer stream aufgemacht
                result.addAll(findOffsetForHashCode(raf, hash, indexFile));
            }
        } finally {

            IOUtils.closeQuietly(raf);
        }
        Logger.info("Time for search one index part-file offsets " + result.size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }

    private static Set<FileOffset> findOffsetForHashCode(RandomAccessFile indexRaf, int searchHash, File indexFile) throws IOException {
        long length = indexRaf.length();
        long fromOffset = 0l;
        long toOffset = length;
        while (toOffset - fromOffset != 16) {
            long offsetDiff = (toOffset - fromOffset) / 2;
            long currentOffset = offsetDiff - (offsetDiff % 16) + fromOffset;
            indexRaf.seek(currentOffset);
            int currentHash = indexRaf.readInt();
            if (currentHash == searchHash) {
                Set<FileOffset> result = new HashSet<FileOffset>();
                long jumpBackDatasets = 50;
                int fileHash = indexRaf.readInt();
                long offset = indexRaf.readLong();

                // jump back to get all data sets
                while (currentHash == searchHash && currentOffset != 0) {
                    currentOffset -= (jumpBackDatasets * 16);
                    currentOffset = currentOffset > 0 ? currentOffset : 0;
                    indexRaf.seek(currentOffset);
                    currentHash = indexRaf.readInt();
                    indexRaf.skipBytes(12);
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
                    while ((!foundItAgain || currentHash == searchHash && dis.available() > 0)) {  //
                        currentHash = dis.readInt();
                        fileHash = dis.readInt();
                        offset = dis.readLong();
                        if (currentHash == searchHash) {
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
            } else if (currentHash < searchHash) {
                fromOffset = currentOffset;
            } else {
                toOffset = currentOffset;
            }
        }
        return Collections.emptySet();
    }


    public static HashMultimap<File, Integer> groupByIndexFile(DataCollection dataCollection, JumboQuery.IndexComparision query) {
        Collection<IndexFile> indexFiles = dataCollection.getIndexFiles().get(query.getName());
        // CARSTEN this map one is very slow
        HashMultimap<File, Integer> groupByIndexFile = HashMultimap.create();
        for (IndexFile indexFile : indexFiles) {
            for (String obj : query.getValues()) {
                int hash = obj.hashCode();
                if (hash >= indexFile.getFromHash() && hash <= indexFile.getToHash()) {
                    groupByIndexFile.put(indexFile.getIndexFile(), hash);
                }

            }
        }
        return groupByIndexFile;
    }
}
