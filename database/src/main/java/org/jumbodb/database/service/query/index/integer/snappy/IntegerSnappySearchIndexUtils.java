package org.jumbodb.database.service.query.index.integer.snappy;

import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil;
import org.jumbodb.database.service.query.snappy.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 5:40 PM
 */
public class IntegerSnappySearchIndexUtils {
    private static Logger log = LoggerFactory.getLogger(IntegerSnappySearchIndexUtils.class);

    private final static Map<QueryOperation, OperationSearch<Integer>> OPERATIONS = createOperations();

    private static Map<QueryOperation, OperationSearch<Integer>> createOperations() {
        Map<QueryOperation, OperationSearch<Integer>> operations = new HashMap<QueryOperation, OperationSearch<Integer>>();
        operations.put(QueryOperation.EQ, new IntegerEqOperationSearch());
        operations.put(QueryOperation.NE, new IntegerNeOperationSearch());
        operations.put(QueryOperation.LT, new IntegerLtOperationSearch());
        operations.put(QueryOperation.GT, new IntegerGtOperationSearch());
        operations.put(QueryOperation.BETWEEN, new IntegerBetweenOperationSearch());
        return operations;
    }

    public static Set<FileOffset> searchOffsetsByClauses(File indexFile, Set<QueryClause> clauses) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        Set<FileOffset> result = new HashSet<FileOffset>();
        try {
            SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(indexFile);
            raf = new RandomAccessFile(indexFile, "r");

            for (QueryClause clause : clauses) {
                result.addAll(findOffsetForClause(raf, clause, snappyChunks));
            }

        } finally {
            IOUtils.closeQuietly(raf);
        }
        log.info("Time for search one index part-file offsets " + result.size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }

    public static int readLastInt(byte[] uncompressed) {
        return SnappyUtil.readInt(uncompressed, uncompressed.length - 16);
    }

    public static int readFirstInt(byte[] uncompressed) {
        return SnappyUtil.readInt(uncompressed, 0);
    }

    private static Set<FileOffset> findOffsetForClause(RandomAccessFile indexRaf, QueryClause clause, SnappyChunks snappyChunks) throws IOException {
        OperationSearch<Integer> integerOperationSearch = OPERATIONS.get(clause.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + clause.getQueryOperation());
        }
        long currentChunk = integerOperationSearch.findFirstMatchingChunk(indexRaf, clause, snappyChunks);
        long numberOfChunks = snappyChunks.getNumberOfChunks();
        if(currentChunk >= 0) {
            Set<FileOffset> result = new HashSet<FileOffset>();
            while(currentChunk < numberOfChunks) {
                byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
                ByteArrayInputStream bais = null;
                DataInputStream dis = null;
                try {
                    bais = new ByteArrayInputStream(uncompressed);
                    dis = new DataInputStream(bais);
                    while(bais.available() > 0) {
                        int currentIntValue = dis.readInt();
                        int fileNameHash = dis.readInt();
                        long offset = dis.readLong();
                        if(integerOperationSearch.matching(currentIntValue, clause)) {
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
    }

    public static boolean acceptIndexFile(QueryClause queryClause, IntegerSnappyIndexFile hashCodeSnappyIndexFile) {
        OperationSearch<Integer> integerOperationSearch = OPERATIONS.get(queryClause.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + queryClause.getQueryOperation());
        }
        return integerOperationSearch.acceptIndexFile(queryClause, hashCodeSnappyIndexFile);
    }
}
