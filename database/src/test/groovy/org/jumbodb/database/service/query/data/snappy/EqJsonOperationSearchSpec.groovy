package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.JsonQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class EqJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new EqJsonOperationSearch()

    @Unroll
    def "matches equality #value eq #testValue == #isEquals"() {
        expect:
        def jsonQuery = new JsonQuery("testField", QueryOperation.EQ, value)
        operation.matches(jsonQuery, testValue) == isEquals
        where:
        value     | testValue   | isEquals
        "testStr" | "testStr"   | true
        "testStr" | "testStrNr" | false
        1         | 1           | true
        1         | 2           | false
    }
}
