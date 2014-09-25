package org.jumbodb.database.service.query

import org.jumbodb.common.query.FieldType
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
// CARSTEN implement tests
class JumboQueryConverterServiceSpec extends Specification {
    def service = new JumboQueryConverterService()

    def "SQL conversion AND OR logic Case 1"() {

        //select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h') and (g = 'g' or y = 'y' or o = 'o')) or x = 'x'

    }

    def "SQL conversion AND OR logic Case 2"() {
//        select * from test a where ((idx(aaaa, ddd) = 'aaa' AND bb = 'bb') OR user.cc = 'bb') limit 10
    }

    def "SQL conversion AND OR logic Case 3"() {
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where ((a = 'b' or z = 'z') and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
    }

    def "SQL conversion AND OR logic Case 4"() {
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
    }

    def "SQL conversion 4"() {

    }

    def "verify EXISTS clause"() {
        when:
        def stmt = "select * from my_table where EXISTS(my_field)"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getDataQuery().get(0)
        where.queryOperation == QueryOperation.EXISTS
        where.left == 'my_field'
        where.leftType == FieldType.FIELD
    }

    def "verify NOT EXISTS clause"() {
        when:
        def stmt = "select * from my_table where NOT EXISTS(my_field)"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getDataQuery().get(0)
        where.queryOperation == QueryOperation.NOT_EXISTS
        where.left == 'my_field'
        where.leftType == FieldType.FIELD
    }

    def "verify simple IDX function with other value, idx left"() {
        when:
        def stmt = "select * from my_table where IDX(my_field) > 5"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getIndexQuery().get(0)
        query.getDataQuery().size() == 0
        where.queryOperation == QueryOperation.GT
        where.name == 'my_field'
        where.value == 5
    }

    def "verify simple IDX function with other value, idx right"() {
        when:
        def stmt = "select * from my_table where 5 < IDX(my_field)"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getIndexQuery().get(0)
        query.getDataQuery().size() == 0
        where.queryOperation == QueryOperation.GT
        where.name == 'my_field'
        where.value == 5
    }

    def "verify IDX function with more than one parameter"() {
        // CARSTEN is not supported
    }

    def "verify simple IDX function with other value, idx with other value"() {
        // CARSTEN is not supported
    }

    def "verify IDX function with OR to another IDX"() {
        // CARSTEN should be possible
    }

    def "verify IDX function with AND to another IDX"() {
        // CARSTEN should be possible
    }

    def "verify IDX function with OR to data field"() {
        // CARSTEN this is not allowed
    }


    def "verify IDX function with AND to data field"() {
        // CARSTEN this is allowed
    }

    def "verify IDX function with AND to data field and IDX function is written on the right"() {
        // CARSTEN this is allowed
    }

    def "verify IDX function embedded in and condition"() {
        // CARSTEN this is not allowed
    }

    def "verify supported SQL features"() {

    }


    def "verify unsupported SQL features"() {

    }
}
