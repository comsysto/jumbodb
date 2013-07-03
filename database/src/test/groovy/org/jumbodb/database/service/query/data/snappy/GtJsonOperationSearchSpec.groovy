package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GtJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new GtJsonOperationSearch()

    @Unroll
    def "greater than double match #testValue > #value == #isGreaterThan"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2d    | 5d        | true
        2d    | 1d        | false
        2d    | 2d        | false
        -2d   | 0d        | true
        -2d   | -3d       | false
    }

    @Unroll
    def "greater than float match #testValue > #value== #isGreaterThan"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2f    | 5f        | true
        2f    | 1f        | false
        2f    | 2f        | false
        -2f   | 0f        | true
        -2f   | -3f       | false
    }

    @Unroll
    def "greater than integer match #testValue > #value == #isGreaterThan"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GT, value)
        operation.matches(queryClause, testValue) == isGreaterThan
        where:
        value | testValue | isGreaterThan
        2     | 5         | true
        2     | 1         | false
        2     | 2         | false
        -2    | 0         | true
        -2    | -3        | false
    }

    @Unroll
    def "greater than long match #testValue > #value == #isGreaterThan"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GT, value)
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
        def queryClause = new QueryClause(QueryOperation.GT, 4)
        operation.matches(queryClause, "illegal")
        then:
        thrown IllegalArgumentException
    }
}
