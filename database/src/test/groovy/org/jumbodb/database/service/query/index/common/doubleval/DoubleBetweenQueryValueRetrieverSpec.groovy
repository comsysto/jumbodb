package org.jumbodb.database.service.query.index.common.doubleval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.doubleval.DoubleBetweenQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleBetweenQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify double parsing #queryValue"() {
        expect:
        def retriever = new DoubleBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue     | converted
        [10d, 333.33d] | [10d, 333.33d]
        [11.11d, 44d]  | [11.11d, 44d]
    }

    def "expect exception on bullshit string"() {
        when:
        new DoubleBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}
