package org.jumbodb.database.service.query;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

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
    private static Logger log = LoggerFactory.getLogger(SearchIndexUtils.class);

    public static Set<FileOffset> searchOffsetsByHashes(File indexFile, Set<Integer> hashes) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        Set<FileOffset> result = new HashSet<FileOffset>();
        try {
            SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(indexFile);
            raf = new RandomAccessFile(indexFile, "r");
            for (Integer hash : hashes) {
                result.addAll(findOffsetForHashCode(raf, hash, snappyChunks));
            }
        } finally {
            IOUtils.closeQuietly(raf);
        }
        log.info("Time for search one index part-file offsets " + result.size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }

    private static long findFirstMatchingChunk(RandomAccessFile indexRaf, int searchHash, SnappyChunks snappyChunks) throws IOException {
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        int maxChunk = numberOfChunks - 1;

        // TODO verify snappy version
        while(toChunk - fromChunk != 0 || fromChunk == maxChunk) {
            int chunkDiff = (toChunk - fromChunk) / 2;
            int currentChunk = chunkDiff + fromChunk;

            byte[] uncompressed = getUncompressed(indexRaf, snappyChunks, currentChunk);
            int firstHash = readFirstHash(uncompressed);
            int lastHash = readLastHash(uncompressed);


            if(firstHash == searchHash) {
//                Logger.info("firstHash == searchHash " + searchHash);
                // ok ist gleich ein block weiter zurück ... da es bereits da beginnen könnte
                while(currentChunk > 0) {
                    currentChunk--;
                    uncompressed = getUncompressed(indexRaf, snappyChunks, currentChunk);
                    firstHash = readFirstHash(uncompressed);
                    if(firstHash < searchHash) {
                        return currentChunk;
                    }
                }
                if(firstHash == searchHash) {
                    // chunk 0, erster hash ist gleich
                    return currentChunk;
                }

            }
            else if(firstHash <= searchHash && lastHash >= searchHash) {
//                Logger.info("firstHash <= searchHash && lastHash >= searchHash" + searchHash);
                // ok firstHash == searchHash hat nicht gegriffen, aber die condition, der block den wir suchen!
                return currentChunk;
            }
            else if (lastHash < searchHash) {
//                Logger.info("lastHash < searchHash" + searchHash);
                fromChunk = currentChunk;
            } else if(firstHash > searchHash) {
//                Logger.info("firstHash > searchHash" + searchHash);
                toChunk = currentChunk;
            }
        }
        return -1;
    }

    public static int readLastHash(byte[] uncompressed) {
        return readInt(uncompressed, uncompressed.length - 16);
    }

    public static int readFirstHash(byte[] uncompressed) {
        return readInt(uncompressed, 0);
    }

    private static Set<FileOffset> findOffsetForHashCode(RandomAccessFile indexRaf, int searchHash, SnappyChunks snappyChunks) throws IOException {
        long currentChunk = findFirstMatchingChunk(indexRaf, searchHash, snappyChunks);
        long numberOfChunks = snappyChunks.getNumberOfChunks();
        if(currentChunk >= 0) {
            Set<FileOffset> result = new HashSet<FileOffset>();
            while(currentChunk < numberOfChunks) {
                byte[] uncompressed = getUncompressed(indexRaf, snappyChunks, currentChunk);
                ByteArrayInputStream bais = null;
                DataInputStream dis = null;
                try {
                    bais = new ByteArrayInputStream(uncompressed);
                    dis = new DataInputStream(bais);
                    while(bais.available() > 0) {
                        int hash = dis.readInt();
                        int fileNameHash = dis.readInt();
                        long offset = dis.readLong();
                        if(hash == searchHash) {
                            result.add(new FileOffset(fileNameHash, offset));
                        } else if(!result.isEmpty()) {
                            // found some results, but here it isnt equal, that means end of results
                            return result;
                        }
                    }
                } finally {
                    IOUtils.closeQuietly(dis);
                    IOUtils.closeQuietly(bais);
                }

                currentChunk++;
            }
            return result;
        }
        return Collections.emptySet();

//        long length = indexRaf.length();
//        long fromOffset = 0l;
//        long toOffset = length;
//        while (toOffset - fromOffset != 16) {
//            long offsetDiff = (toOffset - fromOffset) / 2;
//            long currentOffset = offsetDiff - (offsetDiff % 16) + fromOffset;
//            indexRaf.seek(currentOffset);
//            int currentHash = indexRaf.readInt();
//            if (currentHash == searchHash) {
//                Set<FileOffset> result = new HashSet<FileOffset>();
//                long jumpBackDatasets = 50;
//                int fileHash = indexRaf.readInt();
//                long offset = indexRaf.readLong();
//
//                // jump back to get all data sets
//                while (currentHash == searchHash && currentOffset != 0) {
//                    currentOffset -= (jumpBackDatasets * 16);
//                    currentOffset = currentOffset > 0 ? currentOffset : 0;
//                    indexRaf.seek(currentOffset);
//                    currentHash = indexRaf.readInt();
//                    indexRaf.skipBytes(12);
//                    jumpBackDatasets *= 2;
//                }
//
//                // new buffered input stream is faster for sequential read, then random access
//                // now get forward
//                FileInputStream fis = new FileInputStream(indexFile);
//                BufferedInputStream bis = new BufferedInputStream(fis);
//                DataInputStream dis = new DataInputStream(bis);
//                try {
//                    dis.skip(indexRaf.getFilePointer());
//                    boolean foundItAgain = false;
//                    while ((!foundItAgain || currentHash == searchHash && dis.available() > 0)) {  //
//                        currentHash = dis.readInt();
//                        fileHash = dis.readInt();
//                        offset = dis.readLong();
//                        if (currentHash == searchHash) {
//                            foundItAgain = true;
//                            result.add(new FileOffset(fileHash, offset));
//                        }
//                    }
//                } finally {
//                    IOUtils.closeQuietly(dis);
//                    IOUtils.closeQuietly(fis);
//                    IOUtils.closeQuietly(bis);
//                }
//                return result;
//            } else if (currentHash < searchHash) {
//                fromOffset = currentOffset;
//            } else {
//                toOffset = currentOffset;
//            }
//        }
//        return Collections.emptySet();
    }

    public static byte[] getUncompressed(RandomAccessFile indexRaf, SnappyChunks snappyChunks, long currentChunk) throws IOException {
//        Logger.info("currentChunk " + currentChunk);
        long offsetForChunk = snappyChunks.getOffsetForChunk(currentChunk);
//        Logger.info("offsetForChunk" + offsetForChunk);
        indexRaf.seek(offsetForChunk);
        int snappyBlockLength = indexRaf.readInt();
//        Logger.info("snappyBlockLength" + snappyBlockLength);
//        Logger.info("from meta" + snappyChunks.getChunks().get(0));
        byte[] compressed = new byte[snappyBlockLength];
        indexRaf.read(compressed);
//        int uncompressedLength = Snappy.uncompressedLength(compressed, 0, snappyChunks.getChunkSize());
//        byte[] uncompressed = new byte[uncompressedLength];
//        int actualUncompressedLength = Snappy.uncompress(compressed, 0, snappyChunks.getChunkSize(), uncompressed, 0);

//        return uncompressed;
        return Snappy.uncompress(compressed);
    }

    public static int readInt(byte[] buffer, int pos) {
        int b1 = (buffer[pos] & 0xFF) << 24;
        int b2 = (buffer[pos + 1] & 0xFF) << 16;
        int b3 = (buffer[pos + 2] & 0xFF) << 8;
        int b4 = buffer[pos + 3] & 0xFF;
        return b1 | b2 | b3 | b4;
    }


    public static HashMultimap<File, Integer> groupByIndexFile(DataDeliveryChunk dataDeliveryChunk, JumboQuery.IndexComparision query) {
        Collection<IndexFile> indexFiles = dataDeliveryChunk.getIndexFiles().get(query.getName());
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