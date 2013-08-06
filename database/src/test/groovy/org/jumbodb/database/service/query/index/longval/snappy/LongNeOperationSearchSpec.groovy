package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongNeOperationSearchSpec extends Specification {
    def operation = new LongNeOperationSearch(new LongSnappyIndexStrategy())

    @Unroll
    def "not equal match #value != #testValue == #isNotEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.NE, value)

        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isNotEqual
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
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.NE, searchDate)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
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
        def queryClause = new QueryClause(QueryOperation.NE, queryValue)
        def indexFile = new NumberSnappyIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
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
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.NE, 5l))
        then:
        valueRetriever instanceof LongQueryValueRetriever
    }
}