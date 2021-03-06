package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerBetweenOperationSearchSpec extends Specification {
    def operation = new IntegerBetweenOperationSearch()

    @Unroll
    def "between match #from < #testValue > #to == #isBetween"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))

        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isBetween
        where:
        from | to | testValue | isBetween
        1    | 5  | 2         | true
        1    | 5  | 0         | false
        1    | 5  | 1         | false
        1    | 5  | 5         | false
        1    | 5  | 4         | true
        1    | 5  | 6         | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = IntegerDataGeneration.createFile();
        def snappyChunks = IntegerDataGeneration.createIndexFile(file)
        def retriever = IntegerDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, [searchValue, 20000l])), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -2050       | 0 // is outside of the generated range
        -2047       | 0
        -1          | 0
        0           | 0
        1           | 1
        2048        | 1   // is equal to starting value has to check a chunk before
        2049        | 2
        16385       | 9
        18432       | 9 // is equal to starting value has to check a chunk before
        18433       | 10
        20479       | 10
        22000       | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile from=#indexFileFrom to=#indexFileTo "() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(queryFrom, queryTo))
        def indexFile = new NumberSnappyIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryFrom | queryTo | indexFileFrom | indexFileTo | accept
        1         | 10      | 2             | 9           | true
        1         | 10      | 1             | 10          | false
        1         | 10      | 2             | 10          | true
        1         | 10      | 1             | 9           | false
        1         | 10      | 0             | 11          | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, []))
        then:
        valueRetriever instanceof IntegerBetweenQueryValueRetriever
    }
}