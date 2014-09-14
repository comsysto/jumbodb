package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.JsonQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class BetweenJsonOperationSearchSpec extends Specification {
    def operation = new BetweenJsonOperationSearch()

    @Unroll
    def "between double match #from < #testValue > #to == #isBetween"() {
        expect:
        def jsonQuery = new JsonQuery("testField", QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(jsonQuery, testValue) == isBetween
        where:
        from | to  | testValue | isBetween
        2d   | 10d | 5d        | true
        2d   | 10d | 1d        | false
        2d   | 10d | 11d       | false
        2d   | 10d | 2d        | false
        2d   | 10d | 10d       | false
        -2d  | 1d  | 0d        | true
    }

    @Unroll
    def "between float match #from < #testValue > #to == #isBetween"() {
        expect:
        def jsonQuery = new JsonQuery("testField", QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(jsonQuery, testValue) == isBetween
        where:
        from | to  | testValue | isBetween
        2f   | 10f | 5f        | true
        2f   | 10f | 1f        | false
        2f   | 10f | 11f       | false
        2f   | 10f | 2f        | false
        2f   | 10f | 10f       | false
        -2f  | 1f  | 0f        | true
    }

    @Unroll
    def "between integer match #from < #testValue > #to == #isBetween"() {
        expect:
        def jsonQuery = new JsonQuery("testField", QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(jsonQuery, testValue) == isBetween
        where:
        from | to | testValue | isBetween
        2    | 10 | 5         | true
        2    | 10 | 1         | false
        2    | 10 | 11        | false
        2    | 10 | 2         | false
        2    | 10 | 10        | false
        -2   | 1  | 0         | true

    }

    @Unroll
    def "between long match #from < #testValue > #to == #isBetween"() {
        expect:
        def jsonQuery = new JsonQuery("testField", QueryOperation.BETWEEN, Arrays.asList(from, to))
        operation.matches(jsonQuery, testValue) == isBetween
        where:
        from | to  | testValue | isBetween
        2l   | 10l | 5l        | true
        2l   | 10l | 1l        | false
        2l   | 10l | 11l       | false
        2l   | 10l | 2l        | false
        2l   | 10l | 10l       | false
        -2l  | 1l  | 0l        | true
    }

    def "illegal argument exception expected"() {
        when:
        def jsonQuery = new JsonQuery("testField", QueryOperation.BETWEEN, Arrays.asList(5, 10))
        operation.matches(jsonQuery, "illegal")
        then:
        thrown IllegalArgumentException
    }
}
