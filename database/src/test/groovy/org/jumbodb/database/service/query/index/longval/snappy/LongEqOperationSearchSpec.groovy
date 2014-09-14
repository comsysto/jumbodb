package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongEqOperationSearchSpec extends Specification {
    def operation = new LongEqOperationSearch()

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isEqual
        where:
        value | testValue | isEqual
        -123l | -123l     | true
        123l  | 123l      | true
        123l  | 124l      | false
        123l  | 122l      | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def retriever = LongDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -1700l      | 0 // is outside of the generated range
        -1599l      | 0
        -1l         | 0
        0l          | 0
        1l          | 1
        1600l       | 1   // is equal to starting value has to check a chunk before
        1601l       | 2
        12801l      | 9
        14400l      | 9 // is equal to starting value has to check a chunk before
        14401l      | 10
        15999l      | 10
        20000l      | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, queryValue)
        def indexFile = new NumberSnappyIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0l         | 1l            | 11l         | false
        1l         | 1l            | 11l         | true
        2l         | 1l            | 11l         | true
        10l        | 1l            | 11l         | true
        11l        | 1l            | 11l         | true
        12l        | 1l            | 11l         | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, 5l))
        then:
        valueRetriever instanceof LongQueryValueRetriever
    }
}