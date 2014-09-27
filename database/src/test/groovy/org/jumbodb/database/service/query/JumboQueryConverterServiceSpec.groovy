package org.jumbodb.database.service.query

import org.jumbodb.common.query.FieldType
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.common.query.SelectFieldFunction
import org.jumbodb.database.service.query.sql.SQLParseException
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
// CARSTEN implement tests
class JumboQueryConverterServiceSpec extends Specification {
    def service = new JumboQueryConverterService()

    def "SQL conversion AND OR logic Case 1"() {
        when:
        def stmt = "select * from test where (a = 'b' and z = p and (c = 'd' or c = 'f' or g = 'h') and (g = 'g' or y = 'y' or o = 'o')) or x = 'x'"
        def query = service.convertSqlToJumboQuery(stmt)
        def dataQuery = query.getDataQuery()
        def indexQuery = query.getIndexQuery()
        then:
        dataQuery.size() == 2
        indexQuery.size() == 0
        def where1 = query.getDataQuery().get(0)
        where1.queryOperation == QueryOperation.EQ
        where1.left == 'x'
        where1.leftType == FieldType.FIELD
        where1.right == 'x'
        where1.rightType == FieldType.VALUE
        def where2 = query.getDataQuery().get(1)
        where2.queryOperation == QueryOperation.EQ
        where2.left == 'a'
        where2.leftType == FieldType.FIELD
        where2.right == 'b'
        where2.rightType == FieldType.VALUE
        where2.getOrs().size() == 0
        def subAnd = where2.getAnd()
        subAnd.queryOperation == QueryOperation.EQ
        subAnd.left == 'z'
        subAnd.leftType == FieldType.FIELD
        subAnd.right == 'p'
        subAnd.rightType == FieldType.FIELD
        subAnd.getOrs().size() == 0
        def subAnd2 = subAnd.getAnd()
        subAnd2.queryOperation == QueryOperation.OR
        subAnd2.leftType == FieldType.NOT_SET
        subAnd2.rightType == FieldType.NOT_SET
        subAnd2.getOrs().size() == 3
        def subOrs3 = subAnd2.getOrs()
        subOrs3.get(0).left == 'c'
        subOrs3.get(0).queryOperation == QueryOperation.EQ
        subOrs3.get(0).right == 'd'
        subOrs3.get(1).left == 'c'
        subOrs3.get(1).queryOperation == QueryOperation.EQ
        subOrs3.get(1).right == 'f'
        subOrs3.get(2).left == 'g'
        subOrs3.get(2).queryOperation == QueryOperation.EQ
        subOrs3.get(2).right == 'h'
        def subAnd3 = subAnd2.getAnd()
        subAnd3.queryOperation == QueryOperation.OR
        subAnd3.leftType == FieldType.NOT_SET
        subAnd3.rightType == FieldType.NOT_SET
        subAnd3.getOrs().size() == 3
        def subOrs4 = subAnd3.getOrs()
        subOrs4.get(0).left == 'g'
        subOrs4.get(0).queryOperation == QueryOperation.EQ
        subOrs4.get(0).right == 'g'
        subOrs4.get(1).left == 'y'
        subOrs4.get(1).queryOperation == QueryOperation.EQ
        subOrs4.get(1).right == 'y'
        subOrs4.get(2).left == 'o'
        subOrs4.get(2).queryOperation == QueryOperation.EQ
        subOrs4.get(2).right == 'o'
    }

    def "SQL conversion AND OR logic Case 2"() {
        when:
        def stmt = "select * from test where ((a = 'b' or z = 'z') and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'"
        def query = service.convertSqlToJumboQuery(stmt)
        def dataQuery = query.getDataQuery()
        def indexQuery = query.getIndexQuery()
        then:
        dataQuery.size() == 2
        indexQuery.size() == 0
        def where1 = query.getDataQuery().get(0)
        where1.queryOperation == QueryOperation.EQ
        where1.left == 'x'
        where1.leftType == FieldType.FIELD
        where1.right == 'x'
        where1.rightType == FieldType.VALUE
        def where2 = query.getDataQuery().get(1)
        where2.queryOperation == QueryOperation.OR
        where2.leftType == FieldType.NOT_SET
        where2.rightType == FieldType.NOT_SET
        where2.getOrs().size() == 3
        def where2Ors = where2.getOrs();
        where2Ors.get(0).left == 'c'
        where2Ors.get(0).queryOperation == QueryOperation.EQ
        where2Ors.get(0).right == 'd'
        where2Ors.get(1).left == 'c'
        where2Ors.get(1).queryOperation == QueryOperation.EQ
        where2Ors.get(1).right == 'f'
        where2Ors.get(2).left == 'g'
        where2Ors.get(2).queryOperation == QueryOperation.EQ
        where2Ors.get(2).right == 'h'
        def subAnd = where2.getAnd()
        subAnd.queryOperation == QueryOperation.OR
        subAnd.leftType == FieldType.NOT_SET
        subAnd.rightType == FieldType.NOT_SET
        subAnd.getOrs().size() == 2
        def subAndOrs = subAnd.getOrs();
        subAndOrs.get(0).left == 'a'
        subAndOrs.get(0).queryOperation == QueryOperation.EQ
        subAndOrs.get(0).right == 'b'
        subAndOrs.get(1).left == 'z'
        subAndOrs.get(1).queryOperation == QueryOperation.EQ
        subAndOrs.get(1).right == 'z'
    }

    def "SQL conversion AND OR logic Case 3"() {
        when:
        def stmt = "select * from test where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'"
        def query = service.convertSqlToJumboQuery(stmt)
        def dataQuery = query.getDataQuery()
        def indexQuery = query.getIndexQuery()
        then:
        dataQuery.size() == 2
        indexQuery.size() == 0
        def where1 = query.getDataQuery().get(0)
        where1.queryOperation == QueryOperation.EQ
        where1.left == 'x'
        where1.leftType == FieldType.FIELD
        where1.right == 'x'
        where1.rightType == FieldType.VALUE
        def where2 = query.getDataQuery().get(1)
        where2.queryOperation == QueryOperation.EQ
        where2.left == 'a'
        where2.leftType == FieldType.FIELD
        where2.right == 'b'
        where2.rightType == FieldType.VALUE
        where2.getOrs().size() == 0
        def subAnd1 = where2.getAnd();
        subAnd1.queryOperation == QueryOperation.EQ
        subAnd1.left == 'z'
        subAnd1.leftType == FieldType.FIELD
        subAnd1.right == 'z'
        subAnd1.rightType == FieldType.VALUE
        subAnd1.getOrs().size() == 0
        def subAnd2 = subAnd1.getAnd();
        subAnd2.queryOperation == QueryOperation.OR
        subAnd2.leftType == FieldType.NOT_SET
        subAnd2.rightType == FieldType.NOT_SET
        subAnd2.getOrs().size() == 3
        def subAnd2Ors = subAnd2.getOrs()
        subAnd2Ors.get(0).left == 'c'
        subAnd2Ors.get(0).queryOperation == QueryOperation.EQ
        subAnd2Ors.get(0).right == 'd'
        subAnd2Ors.get(1).left == 'c'
        subAnd2Ors.get(1).queryOperation == QueryOperation.EQ
        subAnd2Ors.get(1).right == 'f'
        subAnd2Ors.get(2).left == 'g'
        subAnd2Ors.get(2).queryOperation == QueryOperation.EQ
        subAnd2Ors.get(2).right == 'h'

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
        //  diese query wird nicht supported, da index embedded und full scan außen
        //        select * from test a where ((idx(aaaa, ddd) = 'aaa' AND bb = 'bb') OR user.cc = 'bb') limit 10
        // CARSTEN this is not allowed
    }

    def "verify LIMIT clause with value"() {
        when:
        def stmt = "select * from my_table limit 1000"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getLimit() == 1000
    }

    def "verify LIMIT clause without value"() {
        when:
        def stmt = "select * from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.getLimit() == -1
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
        ex.message == "The selected fields [key_field] require a group by, if you want to collect all values use COLLECT or COLLECT(DISTINCT ...)."
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

    @Unroll
    def "verify where #operation condition"() {
        expect:
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataQuery().get(0);
        where.queryOperation == operation
        where.left == 'my_field'
        where.leftType == FieldType.FIELD
        where.right == 9
        where.rightType == FieldType.VALUE
        where:
        stmt                                         | operation
        "select * from my_table where my_field > 9"  | QueryOperation.GT
        "select * from my_table where my_field >= 9" | QueryOperation.GT_EQ
        "select * from my_table where my_field < 9"  | QueryOperation.LT
        "select * from my_table where my_field <= 9" | QueryOperation.LT_EQ
        "select * from my_table where my_field = 9"  | QueryOperation.EQ
        "select * from my_table where my_field != 9" | QueryOperation.NE
    }

    def "verify timestamp query"() {
        when:
        def stmt = "select * from my_table where my_date_field < {ts '2012-12-12 12:12:12'}"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataQuery().get(0);
        then:
        where.queryOperation == QueryOperation.LT
        where.left == 'my_date_field'
        where.leftType == FieldType.FIELD
        where.right == 1355310732000
        where.rightType == FieldType.VALUE
    }

    def "verify unsupported SQL feature: JOINS"() {
        when:
        def stmt = "select * from my_table, other_table"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "JOINS are currently not supported."
    }

    def "verify unsupported SQL feature: HAVING"() {
        when:
        def stmt = "select * from my_table where x = 1 group by x having a = 3"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "HAVING is not supported."
    }

    def "verify unsupported SQL feature: TOP"() {
        when:
        def stmt = "select TOP 3 * from my_table"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "TOP is not supported."
    }

    def "verify unsupported SQL feature: table aliases"() {
        when:
        def stmt = "select * from my_table e where e.aaa = 4"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "Table aliases are currently not supported."
    }

    def "verify unsupported SQL feature: SUBSELECT"() {
        when:
        def stmt = "select * from my_table where my_field = (Select id from other_tables)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "SUBSELECTS are currently not supported."
    }

    def "verify unsupported SQL feature: unsupported function"() {
        when:
        def stmt = "select * from my_table where my_field = function_does_not_exist(other_field)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "The function 'FUNCTION_DOES_NOT_EXIST' is not supported."
    }

    def "verify unsupported SQL feature: EXISTS on index"() {
        when:
        def stmt = "select * from my_table where exists IDX(my_field)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "EXISTS is not allowed with indexes."
    }

    def "verify unsupported SQL feature: DELETE"() {
        when:
        def stmt = "DELETE FROM my_collection"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "Only plain SELECT statements are allowed!"
    }

}
