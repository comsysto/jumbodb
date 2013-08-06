package org.jumbodb.database.service.query.index.hashcode32.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class HashCode32QueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify hashcode parsing #queryValue"() {
        expect:
        def retriever = new HashCode32QueryValueRetriever(new QueryClause(QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue            | converted
        "HelloWorld"          | "HelloWorld".hashCode()
        Arrays.asList(12, 34) | Arrays.asList(12, 34).hashCode()
        new Long(45)          | new Long(45).hashCode()
    }
}
