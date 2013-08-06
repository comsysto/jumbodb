package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleBetweenOperationSearch
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleBetweenQueryValueRetriever
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleDataGeneration
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleSnappyIndexStrategy
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongBetweenOperationSearchSpec extends Specification {
    def operation = new LongBetweenOperationSearch(new LongSnappyIndexStrategy())

    @Unroll
    def "between match #from < #testValue > #to == #isBetween"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))

        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isBetween
        where:
        from | to | testValue | isBetween
        1l   | 5l | 2l        | true
        1l   | 5l | 0l        | false
        1l   | 5l | 1l        | false
        1l   | 5l | 5l        | false
        1l   | 5l | 4l        | true
        1l   | 5l | 6l        | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, [searchValue, 20000l])), snappyChunks) == expectedChunk
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
    def "acceptIndexFile from=#indexFileFrom to=#indexFileTo "() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(queryFrom, queryTo))
        def indexFile = new NumberSnappyIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryFrom | queryTo | indexFileFrom | indexFileTo | accept
        1l        | 10l     | 2l            | 9l          | true
        1l        | 10l     | 1l            | 10l         | false
        1l        | 10l     | 2l            | 10l         | true
        1l        | 10l     | 1l            | 9l          | false
        1l        | 10l     | 0l            | 11l         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, []))
        then:
        valueRetriever instanceof LongBetweenQueryValueRetriever
    }
}