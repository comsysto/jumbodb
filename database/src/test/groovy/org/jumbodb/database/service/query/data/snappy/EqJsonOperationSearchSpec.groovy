package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation


/**
 * @author Carsten Hufe
 */
class EqJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new EqJsonOperationSearch()

    def "matches equality"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, value)
        operation.matches(queryClause, testValue) == result
        where:
        value     | testValue   | result
        "testStr" | "testStr"   | true
        "testStr" | "testStrNr" | false
        1         | 1           | true
        1         | 2           | false
    }
}
