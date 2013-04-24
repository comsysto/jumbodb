package org.jumbodb.database.service.query.index.integer.snappy;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.jumbodb.database.service.query.index.IndexKey;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil;
import org.jumbodb.database.service.query.snappy.SnappyStreamToFileCopy;
import org.jumbodb.database.service.query.snappy.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class IntegerSnappyIndexStrategy extends NumberSnappyIndexStrategy<Integer, NumberSnappyIndexFile<Integer>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(IntegerSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Integer, NumberSnappyIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Integer, NumberSnappyIndexFile<Integer>>> operations = new HashMap<QueryOperation, OperationSearch<Integer, NumberSnappyIndexFile<Integer>>>();
        operations.put(QueryOperation.EQ, new IntegerEqOperationSearch(this));
        operations.put(QueryOperation.NE, new IntegerNeOperationSearch(this));
        operations.put(QueryOperation.GT, new IntegerGtOperationSearch(this));
        operations.put(QueryOperation.LT, new IntegerLtOperationSearch(this));
        operations.put(QueryOperation.BETWEEN, new IntegerBetweenOperationSearch(this));
        return operations;
    }

    @Override
    public int getSnappyChunkSize() {
        return SNAPPY_INDEX_CHUNK_SIZE;
    }

    @Override
    public Integer readValueFromDataInputStream(DataInputStream dis) throws IOException {
        return dis.readInt();
    }

    @Override
    public Integer readLastValue(byte[] uncompressed) {
        return SnappyUtil.readInt(uncompressed, uncompressed.length - 16);
    }

    @Override
    public Integer readFirstValue(byte[] uncompressed) {
        return SnappyUtil.readInt(uncompressed, 0);
    }

    @Override
    public NumberSnappyIndexFile<Integer> createIndexFile(Integer from, Integer to, File indexFile) {
        return new NumberSnappyIndexFile<Integer>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "INTEGER_SNAPPY_V1";
    }
}
