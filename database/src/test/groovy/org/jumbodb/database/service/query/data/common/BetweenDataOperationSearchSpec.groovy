package org.jumbodb.database.service.query.data.common

import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class BetweenDataOperationSearchSpec extends Specification {
    def operation = new BetweenDataOperationSearch()

    @Unroll
    def "between double match #from < #testValue > #to == #isBetween"() {
        expect:
        operation.matches(testValue, Arrays.asList(from, to)) == isBetween
        where:
        from | to  | testValue | isBetween
        2d   | 10d | 5d        | true
        2d   | 10d | 1d        | false
        2d   | 10d | 11d       | false
        2d   | 10d | 2d        | true
        2d   | 10d | 10d       | true
        -2d  | 1d  | 0d        | true
    }

    @Unroll
    def "between float match #from < #testValue > #to == #isBetween"() {
        expect:
        operation.matches(testValue, Arrays.asList(from, to)) == isBetween
        where:
        from | to  | testValue | isBetween
        2f   | 10f | 5f        | true
        2f   | 10f | 1f        | false
        2f   | 10f | 11f       | false
        2f   | 10f | 2f        | true
        2f   | 10f | 10f       | true
        -2f  | 1f  | 0f        | true
    }

    @Unroll
    def "between integer match #from < #testValue > #to == #isBetween"() {
        expect:
        operation.matches(testValue, Arrays.asList(from, to)) == isBetween
        where:
        from | to | testValue | isBetween
        2    | 10 | 5         | true
        2    | 10 | 1         | false
        2    | 10 | 11        | false
        2    | 10 | 2         | true
        2    | 10 | 10        | true
        -2   | 1  | 0         | true

    }

    @Unroll
    def "between long match #from < #testValue > #to == #isBetween"() {
        expect:
        operation.matches(testValue, Arrays.asList(from, to)) == isBetween
        where:
        from | to  | testValue | isBetween
        2l   | 10l | 5l        | true
        2l   | 10l | 1l        | false
        2l   | 10l | 11l       | false
        2l   | 10l | 2l        | true
        2l   | 10l | 10l       | true
        -2l  | 1l  | 0l        | true
    }

    def "illegal argument false expected"() {
        expect:
        operation.matches("illegal", Arrays.asList(5, 10)) == false
    }
}
