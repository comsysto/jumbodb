package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation

/**
 * @author Carsten Hufe
 */
class LtJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new GtJsonOperationSearch()

    def "less than double match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2d    | 5d        | true
        2d    | 1d        | false
        2d    | 2d        | false
        -2d   | 0d        | true
        -2d   | -3d       | false
    }

    def "less than float match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2f    | 5f        | true
        2f    | 1f        | false
        2f    | 2f        | false
        -2f   | 0f        | true
        -2f   | -3f       | false
    }

    def "less than integer match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2     | 5         | true
        2     | 1         | false
        2     | 2         | false
        -2    | 0         | true
        -2    | -3        | false
    }

    def "less than long match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2l    | 5l        | true
        2l    | 1l        | false
        2l    | 2l        | false
        -2l   | 0l        | true
        -2l   | -3l       | false
    }

    def "illegal argument exception expected"() {
        when:
        def queryClause = new QueryClause(QueryOperation.LT, 4)
        operation.matches(queryClause, "illegal")
        then:
        thrown IllegalArgumentException
    }
}
