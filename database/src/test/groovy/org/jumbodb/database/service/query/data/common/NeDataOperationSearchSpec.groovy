package org.jumbodb.database.service.query.data.common

import org.jumbodb.common.query.JsonQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.data.common.NeDataOperationSearch
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class NeDataOperationSearchSpec extends Specification {
    def operation = new NeDataOperationSearch()

    @Unroll
    def "matches non equality #value ne #testValue == #isNotEquals"() {
        expect:
        def query = new JsonQuery("field", QueryOperation.EQ, value)
        operation.matches(query, testValue) == isNotEquals
        where:
        value     | testValue   | isNotEquals
        "testStr" | "testStr"   | false
        "testStr" | "testStrNr" | true
        1         | 1           | false
        1         | 2           | true
    }
}
