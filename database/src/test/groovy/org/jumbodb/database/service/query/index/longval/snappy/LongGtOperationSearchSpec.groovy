package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleDataGeneration
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleGtOperationSearch
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleQueryValueRetriever
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleSnappyIndexStrategy
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongGtOperationSearchSpec extends Specification {
    def operation = new LongGtOperationSearch(new LongSnappyIndexStrategy())

    @Unroll
    def "greater match #value < #testValue == #isGreater"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GT, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isGreater
        where:
        value | testValue | isGreater
        5l    | 5l        | false
        5l    | 6l        | true
        5l    | 4l        | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.GT, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
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
        def queryClause = new QueryClause(QueryOperation.GT, queryValue)
        def indexFile = new NumberSnappyIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0l         | 1l            | 11l         | true
        1l         | 1l            | 11l         | true
        1l         | 1l            | 11l         | true
        11l        | 1l            | 11l         | false
        10l        | 1l            | 11l         | true
        12l        | 1l            | 11l         | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.GT, 5l))
        then:
        valueRetriever instanceof LongQueryValueRetriever
    }
}