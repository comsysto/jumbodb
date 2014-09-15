package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatEqOperationSearchSpec extends Specification {
    def operation = new FloatEqOperationSearch()

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isEqual
        where:
        value    | testValue  | isEqual
        33.333f  | 33.333f    | true
        4444.44f | 4444.44f   | true
        4444.44f | 4444.441f  | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = FloatDataGeneration.createFile();
        def snappyChunks = FloatDataGeneration.createIndexFile(file)
        def retriever = FloatDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -2050f      | 0 // is outside of the generated range
        -2047f      | 0
        -1f         | 0
        0f          | 0
        1f          | 1
        2048f       | 1   // is equal to starting value has to check a chunk before
        2049f       | 2
        16385f      | 9
        18432f      | 9 // is equal to starting value has to check a chunk before
        18433f      | 10
        20479f      | 10
        22000f      | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, queryValue)
        def indexFile = new NumberSnappyIndexFile<Float>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99f      | 1f            | 11f         | false
        1f         | 1f            | 11f         | true
        1f         | 0.99f         | 11f         | true
        11f        | 1f            | 11f         | true
        11.01f     | 1f            | 11f         | false
        5f         | 1f            | 11f         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, 5f))
        then:
        valueRetriever instanceof FloatQueryValueRetriever
    }
}