package org.jumbodb.database.service.query.index.common.longval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongBetweenOperationSearchSpec extends Specification {
    def operation = new LongBetweenOperationSearch()

    @Unroll
    def "between match #from <= #testValue >= #to == #isBetween"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.BETWEEN, Arrays.asList(from, to))

        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isBetween
        where:
        from | to | testValue | isBetween
        1l   | 5l | 2l        | true
        1l   | 5l | 0l        | false
        1l   | 5l | 1l        | true
        1l   | 5l | 5l        | true
        1l   | 5l | 4l        | true
        1l   | 5l | 6l        | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def retriever = LongDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, [searchValue, 20000l])), snappyChunks) == expectedChunk
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
    def "acceptIndexFile from=#indexFileFrom to=#indexFileTo "() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.BETWEEN, Arrays.asList(queryFrom, queryTo))
        def indexFile = new NumberIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryFrom | queryTo | indexFileFrom | indexFileTo | accept
        1l        | 10l     | 2l            | 9l          | true
        1l        | 10l     | 1l            | 10l         | true
        1l        | 10l     | 2l            | 10l         | true
        1l        | 10l     | 1l            | 9l          | true
        1l        | 10l     | 0l            | 11l         | true
        1l        | 5l      | 5l            | 11l         | true
        1l        | 4l      | 5l            | 11l         | false
        12l       | 13l     | 5l            | 11l         | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, []))
        then:
        valueRetriever instanceof LongBetweenQueryValueRetriever
    }
}