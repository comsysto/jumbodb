package org.jumbodb.database.service.query.index.common.integer

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerNeOperationSearchSpec extends Specification {
    def operation = new IntegerNeOperationSearch()

    @Unroll
    def "not equal match #value != #testValue == #isNotEqual"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.NE, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isNotEqual
        where:
        value | testValue | isNotEqual
        5     | 5         | false
        5     | 4         | true
        5     | 6         | true
    }

    @Unroll
    def "findFirstMatchingBlock #searchDate with expected chunk #expectedChunk"() {
        setup:
        def file = IntegerDataGeneration.createFile();
        def blocks = IntegerDataGeneration.createIndexFile(file)
        def retriever = IntegerDataGeneration.createFileDataRetriever(file, blocks)

        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.NE, searchDate)), blocks) == expectedBlock
        cleanup:
        file.delete();
        where:
        searchDate | expectedBlock
        -2050      | 0 // is outside of the generated range
        -2047      | 0
        -1         | 0
        0          | 0
        1          | 0
        2048       | 0   // is equal to starting value has to check a chunk before
        2049       | 0
        16385      | 0
        18432      | 0 // is equal to starting value has to check a chunk before
        18433      | 0
        20479      | 0
        22000      | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.NE, queryValue)
        def indexFile = new NumberIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0          | 1             | 11          | true
        1          | 1             | 11          | true
        2          | 1             | 11          | true
        10         | 1             | 11          | true
        11         | 1             | 11          | true
        12         | 1             | 11          | true
        1          | 1             | 1           | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.NE, 5))
        then:
        valueRetriever instanceof IntegerQueryValueRetriever
    }
}