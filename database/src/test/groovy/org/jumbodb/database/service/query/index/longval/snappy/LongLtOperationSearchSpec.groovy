package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleDataGeneration
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleLtOperationSearch
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleQueryValueRetriever
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleSnappyIndexStrategy
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongLtOperationSearchSpec extends Specification {
    def operation = new LongLtOperationSearch(new LongSnappyIndexStrategy())

    @Unroll
    def "less match #value > #testValue == #isLess"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isLess
        where:
        value | testValue | isLess
        2l    | 2l        | false
        2l    | 3l        | false
        2l    | 1l        | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
        file.delete();
        where:
        searchValue | expectedChunk
        -1700l      | 0 // is outside of the generated range
        -1599l      | 0
        -1l         | 0
        0l          | 0
        1l          | 0
        1600l       | 0   // is equal to starting value has to check a chunk before
        1601l       | 0
        12801l      | 0
        14400l      | 0 // is equal to starting value has to check a chunk before
        14401l      | 0
        15999l      | 0
        20000l      | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, queryValue)
        def indexFile = new NumberSnappyIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0l         | 1l            | 11l         | false
        1l         | 1l            | 11l         | false
        2l         | 1l            | 11l         | true
        10l        | 1l            | 11l         | true
        11l        | 1l            | 11l         | true
        12l        | 1l            | 11l         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, 5l))
        then:
        valueRetriever instanceof LongQueryValueRetriever
    }
}