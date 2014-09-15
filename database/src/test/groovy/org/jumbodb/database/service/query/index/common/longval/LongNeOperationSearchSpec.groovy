package org.jumbodb.database.service.query.index.common.longval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongNeOperationSearchSpec extends Specification {
    def operation = new LongNeOperationSearch()

    @Unroll
    def "not equal match #value != #testValue == #isNotEqual"() {
        expect:
        def query = new IndexQuery("testIndex", QueryOperation.NE, value)

        operation.matching(testValue, operation.getQueryValueRetriever(query)) == isNotEqual
        where:
        value | testValue | isNotEqual
        5l    | 5l        | false
        5l    | 4l        | true
        5l    | 6l        | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchDate with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def retriever = LongDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.NE, searchDate)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchDate | expectedChunk
        -1700l     | 0 // is outside of the generated range
        -1599l     | 0
        -1l        | 0
        0l         | 0
        1l         | 0
        1600l      | 0   // is equal to starting value has to check a chunk before
        1601l      | 0
        12801l     | 0
        14400l     | 0 // is equal to starting value has to check a chunk before
        14401l     | 0
        15999l     | 0
        20000l     | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.NE, queryValue)
        def indexFile = new NumberIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0l         | 1l            | 11l         | true
        1l         | 1l            | 11l         | true
        2l         | 1l            | 11l         | true
        10l        | 1l            | 11l         | true
        11l        | 1l            | 11l         | true
        12l        | 1l            | 11l         | true
        1l         | 1l            | 1l          | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.NE, 5l))
        then:
        valueRetriever instanceof LongQueryValueRetriever
    }
}