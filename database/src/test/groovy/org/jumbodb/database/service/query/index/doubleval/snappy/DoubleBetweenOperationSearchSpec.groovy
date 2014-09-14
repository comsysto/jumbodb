package org.jumbodb.database.service.query.index.doubleval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleBetweenOperationSearchSpec extends Specification {
    def operation = new DoubleBetweenOperationSearch()

    @Unroll
    def "between match #from < #testValue > #to == #isBetween"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.BETWEEN, Arrays.asList(from, to))

        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isBetween
        where:
        from | to | testValue | isBetween
        1d   | 5d | 2d        | true
        1d   | 5d | 0.99d     | false
        1d   | 5d | 1d        | false
        1d   | 5d | 5d        | false
        1d   | 5d | 1.01d     | true
        1d   | 5d | 4.99d     | true
        1d   | 5d | 5.01d     | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = DoubleDataGeneration.createFile();
        def snappyChunks = DoubleDataGeneration.createIndexFile(file)
        def retriever = DoubleDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, [searchValue, 20000d])), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -1700d      | 0 // is outside of the generated range
        -1599d      | 0
        -1d         | 0
        0d          | 0
        1d          | 1
        1600d       | 1   // is equal to starting value has to check a chunk before
        1601d       | 2
        12801d      | 9
        14400d      | 9 // is equal to starting value has to check a chunk before
        14401d      | 10
        15999d      | 10
        20000d      | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile from=#indexFileFrom to=#indexFileTo "() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.BETWEEN, Arrays.asList(queryFrom, queryTo))
        def indexFile = new NumberSnappyIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryFrom | queryTo | indexFileFrom | indexFileTo | accept
        1d        | 10d     | 1.01d         | 9.99d       | true
        1d        | 10d     | 1d            | 10d         | false
        1d        | 10d     | 1.01d         | 10d         | true
        1d        | 10d     | 1d            | 9.99d       | false
        1d        | 10d     | 0.99d         | 10.01d      | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, []))
        then:
        valueRetriever instanceof DoubleBetweenQueryValueRetriever
    }
}