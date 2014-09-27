package org.jumbodb.database.service.query

import org.jumbodb.common.query.FieldType
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.common.query.SelectFieldFunction
import org.jumbodb.database.service.query.sql.SQLParseException
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

    def "verify LIMIT clause with value"() {
        // CARSTEN
    }

    def "verify LIMIT clause without value"() {
        // CARSTEN
    }

    def "verify selected field *"() {
        when:
        def stmt = "select * from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == '*'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.NONE
        fields.get(0).alias == '*'
    }

    def "verify selected field count(*)"() {
        when:
        def stmt = "select count(*) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == '*'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.COUNT
        fields.get(0).alias == "count(*)"
    }

    def "verify select collect distinct field"() {
        when:
        def stmt = "select COLLECT(distinct my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.COLLECT
        fields.get(0).alias == "COLLECT(DISTINCT my_field)"
    }

    def "verify select distinct field"() {
        when:
        def stmt = "select distinct my_field from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.NONE
        fields.get(0).alias == "my_field"
    }

    def "verify selected field * with other field"() {
        when:
        def stmt = "select *, my_field from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 2
        fields.get(0).columnName == '*'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.NONE
        fields.get(0).alias == '*'
        fields.get(1).columnName == 'my_field'
        fields.get(1).distinct == false
        fields.get(1).function == SelectFieldFunction.NONE
        fields.get(1).alias == 'my_field'
    }

    def "verify selected field with field function"() {
        when:
        def stmt = "select field('my_field') from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.NONE
        fields.get(0).alias == "field('my_field')"
    }

    def "verify selected field my_field"() {
        when:
        def stmt = "select my_field from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.NONE
        fields.get(0).alias == 'my_field'
    }

    def "verify selected field COUNT(my_field)"() {
        when:
        def stmt = "select COUNT(my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.COUNT
        fields.get(0).alias == 'COUNT(my_field)'
    }

    def "verify selected field COUNT(DISTINCT my_field)"() {
        when:
        def stmt = "select COUNT(DISTINCT my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.COUNT
        fields.get(0).alias == 'COUNT(DISTINCT my_field)'
    }

    def "verify selected field COUNT(DISTINCT field('my_field')) with field helper function"() {
        when:
        def stmt = "select COUNT(DISTINCT field('my_field')) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.COUNT
        fields.get(0).alias == "COUNT(DISTINCT field('my_field'))"
    }

    def "verify selected field SUM(my_field)"() {
        when:
        def stmt = "select SUM(my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.SUM
        fields.get(0).alias == 'SUM(my_field)'
    }

    def "verify selected field SUM(DISTINCT my_field)"() {
        when:
        def stmt = "select SUM(DISTINCT my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.SUM
        fields.get(0).alias == 'SUM(DISTINCT my_field)'
    }

    def "verify selected field AVG(my_field)"() {
        when:
        def stmt = "select AVG(my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.AVG
        fields.get(0).alias == 'AVG(my_field)'
    }

    def "verify selected field AVG(DISTINCT my_field)"() {
        when:
        def stmt = "select AVG(DISTINCT my_field) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.AVG
        fields.get(0).alias == 'AVG(DISTINCT my_field)'
    }

    def "verify selected field alias with alias AVG(DISTINCT my_field)"() {
        when:
        def stmt = "select AVG(DISTINCT my_field) AS my_sum from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == true
        fields.get(0).function == SelectFieldFunction.AVG
        fields.get(0).alias == 'my_sum'
    }

    def "verify selected one function and one field without group by should end up with error"() {
        when:
        def stmt = "select key_field, SUM(DISTINCT my_field) from my_table"
        service.convertSqlToJumboQuery(stmt)
        then:
        def ex = thrown SQLParseException
        ex.message == "The selected fields [key_field] require a group by, if you want to collect alll values use COLLECT or COLLECT(DISTINCT ...)."
    }


    def "verify group by clause"() {
        when:
        def stmt = "select key_field, SUM(DISTINCT my_field) from my_table GROUP BY key_field"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getGroupByFields() == ['key_field']
    }

    def "verify group by clause with FIELD"() {
        when:
        def stmt = "select key_field, SUM(DISTINCT my_field) from my_table GROUP BY FIELD('key_field')"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getGroupByFields() == ['key_field']
    }

    def "verify order by clause"() {
        when:
        def stmt = "select * from my_table ORDER BY key_field"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getOrderBy().size() == 1
        query.getOrderBy().get(0).getName() == 'key_field'
        query.getOrderBy().get(0).isAsc() == true
    }

    def "verify order by clause desc"() {
        when:
        def stmt = "select * from my_table ORDER BY key_field DESC"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getOrderBy().size() == 1
        query.getOrderBy().get(0).getName() == 'key_field'
        query.getOrderBy().get(0).isAsc() == false
    }

    def "verify order by clause with FIELD function"() {
        when:
        def stmt = "select * from my_table ORDER BY FIELD('key_field')"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getOrderBy().size() == 1
        query.getOrderBy().get(0).getName() == 'key_field'
        query.getOrderBy().get(0).isAsc() == true
    }

    def "verify FIELD function on top level"() {
        when:
        def stmt = "select FIELD('my_field') from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.NONE
        fields.get(0).alias == "FIELD('my_field')"
    }

    def "verify FIELD function embedded in function"() {
        when:
        def stmt = "select SUM(FIELD('my_field')) from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        def fields = query.getSelectedFields();
        then:
        fields.size() == 1
        fields.get(0).columnName == 'my_field'
        fields.get(0).distinct == false
        fields.get(0).function == SelectFieldFunction.SUM
        fields.get(0).alias == "SUM(FIELD('my_field'))"
    }

    def "verify supported SQL features"() {

    }


    def "verify unsupported SQL features"() {

    }
}
