package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation

/**
 * @author Carsten Hufe
 */
class BetweenJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new BetweenJsonOperationSearch()

    def "between double match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(queryClause, testValue) == result
        where:
        from | to  | testValue | result
        2d   | 10d | 5d        | true
        2d   | 10d | 1d        | false
        2d   | 10d | 11d       | false
        2d   | 10d | 2d        | false
        2d   | 10d | 10d       | false
        -2d  | 1d  | 0d        | true
    }

    def "between float match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(queryClause, testValue) == result
        where:
        from | to  | testValue | result
        2f   | 10f | 5f        | true
        2f   | 10f | 1f        | false
        2f   | 10f | 11f       | false
        2f   | 10f | 2f        | false
        2f   | 10f | 10f       | false
        -2f  | 1f  | 0f        | true
    }

    def "between integer match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(queryClause, testValue) == result
        where:
        from | to | testValue | result
        2    | 10 | 5         | true
        2    | 10 | 1         | false
        2    | 10 | 11        | false
        2    | 10 | 2         | false
        2    | 10 | 10        | false
        -2   | 1  | 0         | true

    }

    def "between long match"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(queryClause, testValue) == result
        where:
        from | to  | testValue | result
        2l   | 10l | 5l        | true
        2l   | 10l | 1l        | false
        2l   | 10l | 11l       | false
        2l   | 10l | 2l        | false
        2l   | 10l | 10l       | false
        -2l  | 1l  | 0l        | true
    }

    def "illegal argument exception expected"() {
        when:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(5, 10))
        operation.matches(queryClause, "illegal")
        then:
        thrown IllegalArgumentException
    }
}
