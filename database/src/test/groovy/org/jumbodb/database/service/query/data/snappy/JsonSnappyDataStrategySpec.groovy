package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.data.notfound.NotFoundDataStrategy

/**
 * @author Carsten Hufe
 */
class JsonSnappyDataStrategySpec extends spock.lang.Specification {
    def strategy = new JsonSnappyDataStrategy()

    def "verify supported operations"() {
        expect:
        strategy.getSupportedOperations().contains(operation)
        where:
        operation << QueryOperation.values()


    }

    def "should responsible always because it's the only data strategy"() {

    }

    def "verify strategy name"() {

    }

    def "onImport stream should be snappy compressed saved"() {

    }

    def "matches should delegate to the appropriate operation"() {

    }

    def "matches should throw an exception when no approprivate operation was found"() {

    }

    def "buildFileOffsetsMap should group by filename hash"() {

    }

    def "findDataSetsByFileOffsets should run index search and submit tasks"() {

    }

    def "findDataSetsByFileOffsets should run scanned search and submit tasks"() {

    }
}
