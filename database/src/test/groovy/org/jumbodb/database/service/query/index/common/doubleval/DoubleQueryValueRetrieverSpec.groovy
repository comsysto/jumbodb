package org.jumbodb.database.service.query.index.common.doubleval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.doubleval.DoubleQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify double parsing #queryValue"() {
        expect:
        def retriever = new DoubleQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        1234d      | 1234d
        1.123d     | 1.123d
    }

    def "expect exception on bullshit string"() {
        when:
        new DoubleQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, "bullshit"))
        then:
        thrown ClassCastException
    }
}