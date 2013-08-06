package org.jumbodb.database.service.query.index.hashcode64.snappy

import org.jumbodb.common.query.HashCode64
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class HashCode64QueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify hashcode parsing #queryValue"() {
        expect:
        def retriever = new HashCode64QueryValueRetriever(new QueryClause(QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue    | converted
        "HelloWorld"  | HashCode64.hash("HelloWorld")
        "HelloWorld3" | HashCode64.hash("HelloWorld3")
    }


    def "expect exception on non string"() {
        when:
        new HashCode64QueryValueRetriever(new QueryClause(QueryOperation.EQ, new Object()))
        then:
        thrown IllegalArgumentException
    }
}