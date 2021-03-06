package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.hashcode32.snappy.HashCode32DataGeneration
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatBetweenOperationSearchSpec extends Specification {
    def operation = new FloatBetweenOperationSearch()

    @Unroll
    def "between match #from < #testValue > #to == #isBetween"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))

        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isBetween
        where:
        from | to | testValue | isBetween
        1f   | 5f | 2f        | true
        1f   | 5f | 0.99f     | false
        1f   | 5f | 1f        | false
        1f   | 5f | 5f        | false
        1f   | 5f | 1.01f     | true
        1f   | 5f | 4.99f     | true
        1f   | 5f | 5.01f     | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = FloatDataGeneration.createFile();
        def snappyChunks = FloatDataGeneration.createIndexFile(file)
        def retriever = FloatDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, [searchValue, 22000f])), snappyChunks) == expectedChunk
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
    def "acceptIndexFile from=#indexFileFrom to=#indexFileTo "() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(queryFrom, queryTo))
        def indexFile = new NumberSnappyIndexFile<Float>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryFrom | queryTo | indexFileFrom | indexFileTo | accept
        1f        | 10f     | 1.01f         | 9.99f       | true
        1f        | 10f     | 1f            | 10f         | false
        1f        | 10f     | 1.01f         | 10f         | true
        1f        | 10f     | 1f            | 9.99f       | false
        1f        | 10f     | 0.99f         | 10.01f      | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, []))
        then:
        valueRetriever instanceof FloatBetweenQueryValueRetriever
    }
}