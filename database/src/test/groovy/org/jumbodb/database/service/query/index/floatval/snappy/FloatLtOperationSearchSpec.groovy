package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatLtOperationSearchSpec extends Specification {
    def operation = new FloatLtOperationSearch(new FloatSnappyIndexStrategy())

    @Unroll
    def "less match #value > #testValue == #isLess"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isLess
        where:
        value | testValue | isLess
        2f    | 2f        | false
        2f    | 2.01f     | false
        2f    | 1.99f     | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = FloatDataGeneration.createFile();
        def snappyChunks = FloatDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
        file.delete();
        where:
        searchValue | expectedChunk
        -2050f      | 0 // is outside of the generated range
        -2047f      | 0
        -1f         | 0
        0f          | 0
        1f          | 0
        2048f       | 0   // is equal to starting value has to check a chunk before
        2049f       | 0
        16385f      | 0
        18432f      | 0 // is equal to starting value has to check a chunk before
        18433f      | 0
        20479f      | 0
        22000f      | 0 // is outside of the generated rang
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, queryValue)
        def indexFile = new NumberSnappyIndexFile<Float>(indexFileFrom, indexFileTo, Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99f      | 1f            | 11f         | false
        1f         | 1f            | 11f         | false
        1.01f      | 1f            | 11f         | true
        10.99f     | 1f            | 11f         | true
        11f        | 1f            | 11f         | true
        11.01f     | 1f            | 11f         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, 5f))
        then:
        valueRetriever instanceof FloatQueryValueRetriever
    }
}