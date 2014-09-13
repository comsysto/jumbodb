package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.JsonQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LtJsonOperationSearchSpec extends Specification {
    def operation = new LtJsonOperationSearch()

    @Unroll
    def "less than double match #testValue < #value == #isLessThan"() {
        expect:
        def query = new JsonQuery("field", QueryOperation.LT, value)
        operation.matches(query, testValue) == isLessThan
        where:
        value | testValue | isLessThan
        2d    | 5d        | false
        2d    | 1d        | true
        2d    | 2d        | false
        -2d   | 0d        | false
        -2d   | -3d       | true
    }

    @Unroll
    def "less than float match #testValue < #value == #isLessThan"() {
        expect:
        def query = new JsonQuery("field", QueryOperation.LT, value)
        operation.matches(query, testValue) == isLessThan
        where:
        value | testValue | isLessThan
        2f    | 5f        | false
        2f    | 1f        | true
        2f    | 2f        | false
        -2f   | 0f        | false
        -2f   | -3f       | true
    }

    @Unroll
    def "less than integer match #testValue < #value == #isLessThan"() {
        expect:
        def query = new JsonQuery("field", QueryOperation.LT, value)
        operation.matches(query, testValue) == isLessThan
        where:
        value | testValue | isLessThan
        2     | 5         | false
        2     | 1         | true
        2     | 2         | false
        -2    | 0         | false
        -2    | -3        | true
    }

    @Unroll
    def "less than long match #testValue < #value == #isLessThan"() {
        expect:
        def query = new JsonQuery("field", QueryOperation.LT, value)
        operation.matches(query, testValue) == isLessThan
        where:
        value | testValue | isLessThan
        2l    | 5l        | false
        2l    | 1l        | true
        2l    | 2l        | false
        -2l   | 0l        | false
        -2l   | -3l       | true
    }

    def "illegal argument exception expected"() {
        when:
        def query = new JsonQuery("field", QueryOperation.LT, 4)
        operation.matches(query, "illegal")
        then:
        thrown IllegalArgumentException
    }
}
