package org.jumbodb.database.service.query

import org.jumbodb.common.query.FieldType
import org.jumbodb.common.query.HintType
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.common.query.SelectFieldFunction
import org.jumbodb.database.service.query.sql.SQLParseException
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class JumboQueryConverterServiceSpec extends Specification {
    def service = new JumboQueryConverterService()

    def "SQL conversion AND OR logic Case 1"() {
        when:
        def stmt = "select * from test where (a = 'b' and z = p and (c = 'd' or c = 'f' or g = 'h') and (g = 'g' or y = 'y' or o = 'o')) or x = 'x'"
        def query = service.convertSqlToJumboQuery(stmt)
        def dataQuery = query.getDataOrs()
        def indexQuery = query.getIndexOrs()
        then:
        dataQuery.size() == 2
        indexQuery.size() == 0
        def where1 = query.getDataOrs().get(0)
        where1.queryOperation == QueryOperation.EQ
        where1.left == 'x'
        where1.leftType == FieldType.FIELD
        where1.right == 'x'
        where1.rightType == FieldType.VALUE
        def where2 = query.getDataOrs().get(1)
        where2.queryOperation == QueryOperation.EQ
        where2.left == 'a'
        where2.leftType == FieldType.FIELD
        where2.right == 'b'
        where2.rightType == FieldType.VALUE
        where2.getDataOrs().size() == 0
        def subAnd = where2.getDataAnd()
        subAnd.queryOperation == QueryOperation.EQ
        subAnd.left == 'z'
        subAnd.leftType == FieldType.FIELD
        subAnd.right == 'p'
        subAnd.rightType == FieldType.FIELD
        subAnd.getDataOrs().size() == 0
        def subAnd2 = subAnd.getDataAnd()
        subAnd2.queryOperation == QueryOperation.OR
        subAnd2.leftType == FieldType.NOT_SET
        subAnd2.rightType == FieldType.NOT_SET
        subAnd2.getDataOrs().size() == 3
        def subOrs3 = subAnd2.getDataOrs()
        subOrs3.get(0).left == 'c'
        subOrs3.get(0).queryOperation == QueryOperation.EQ
        subOrs3.get(0).right == 'd'
        subOrs3.get(1).left == 'c'
        subOrs3.get(1).queryOperation == QueryOperation.EQ
        subOrs3.get(1).right == 'f'
        subOrs3.get(2).left == 'g'
        subOrs3.get(2).queryOperation == QueryOperation.EQ
        subOrs3.get(2).right == 'h'
        def subAnd3 = subAnd2.getDataAnd()
        subAnd3.queryOperation == QueryOperation.OR
        subAnd3.leftType == FieldType.NOT_SET
        subAnd3.rightType == FieldType.NOT_SET
        subAnd3.getDataOrs().size() == 3
        def subOrs4 = subAnd3.getDataOrs()
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
        def dataQuery = query.getDataOrs()
        def indexQuery = query.getIndexOrs()
        then:
        dataQuery.size() == 2
        indexQuery.size() == 0
        def where1 = query.getDataOrs().get(0)
        where1.queryOperation == QueryOperation.EQ
        where1.left == 'x'
        where1.leftType == FieldType.FIELD
        where1.right == 'x'
        where1.rightType == FieldType.VALUE
        def where2 = query.getDataOrs().get(1)
        where2.queryOperation == QueryOperation.OR
        where2.leftType == FieldType.NOT_SET
        where2.rightType == FieldType.NOT_SET
        where2.getDataOrs().size() == 3
        def where2Ors = where2.getDataOrs();
        where2Ors.get(0).left == 'c'
        where2Ors.get(0).queryOperation == QueryOperation.EQ
        where2Ors.get(0).right == 'd'
        where2Ors.get(1).left == 'c'
        where2Ors.get(1).queryOperation == QueryOperation.EQ
        where2Ors.get(1).right == 'f'
        where2Ors.get(2).left == 'g'
        where2Ors.get(2).queryOperation == QueryOperation.EQ
        where2Ors.get(2).right == 'h'
        def subAnd = where2.getDataAnd()
        subAnd.queryOperation == QueryOperation.OR
        subAnd.leftType == FieldType.NOT_SET
        subAnd.rightType == FieldType.NOT_SET
        subAnd.getDataOrs().size() == 2
        def subAndOrs = subAnd.getDataOrs();
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
        def dataQuery = query.getDataOrs()
        def indexQuery = query.getIndexOrs()
        then:
        dataQuery.size() == 2
        indexQuery.size() == 0
        def where1 = query.getDataOrs().get(0)
        where1.queryOperation == QueryOperation.EQ
        where1.left == 'x'
        where1.leftType == FieldType.FIELD
        where1.right == 'x'
        where1.rightType == FieldType.VALUE
        def where2 = query.getDataOrs().get(1)
        where2.queryOperation == QueryOperation.EQ
        where2.left == 'a'
        where2.leftType == FieldType.FIELD
        where2.right == 'b'
        where2.rightType == FieldType.VALUE
        where2.getDataOrs().size() == 0
        def subAnd1 = where2.getDataAnd();
        subAnd1.queryOperation == QueryOperation.EQ
        subAnd1.left == 'z'
        subAnd1.leftType == FieldType.FIELD
        subAnd1.right == 'z'
        subAnd1.rightType == FieldType.VALUE
        subAnd1.getDataOrs().size() == 0
        def subAnd2 = subAnd1.getDataAnd();
        subAnd2.queryOperation == QueryOperation.OR
        subAnd2.leftType == FieldType.NOT_SET
        subAnd2.rightType == FieldType.NOT_SET
        subAnd2.getDataOrs().size() == 3
        def subAnd2Ors = subAnd2.getDataOrs()
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
        def where = query.getDataOrs().get(0)
        where.queryOperation == QueryOperation.EXISTS
        where.left == 'my_field'
        where.leftType == FieldType.FIELD
    }

    def "verify NOT EXISTS clause"() {
        when:
        def stmt = "select * from my_table where NOT EXISTS(my_field)"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getDataOrs().get(0)
        where.queryOperation == QueryOperation.NOT_EXISTS
        where.left == 'my_field'
        where.leftType == FieldType.FIELD
    }

    def "verify simple IDX function with other value, idx left"() {
        when:
        def stmt = "select * from my_table where IDX(my_field) > 5"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getIndexOrs().get(0)
        query.getDataOrs().size() == 0
        where.queryOperation == QueryOperation.GT
        where.name == 'my_field'
        where.value == 5
    }

    def "verify simple IDX function with other value, idx right"() {
        when:
        def stmt = "select * from my_table where 5 < IDX(my_field)"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        def where = query.getIndexOrs().get(0)
        query.getDataOrs().size() == 0
        where.queryOperation == QueryOperation.GT
        where.name == 'my_field'
        where.value == 5
    }

    def "verify IDX function with more than one parameter"() {
        when:
        def stmt = "select * from my_table where 5 < IDX(my_field, second_field)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def ex = thrown SQLParseException
        ex.getMessage() == "IDX function requires exactly one parameter."
    }

    def "verify simple IDX function with other value, idx with other value"() {
        when:
        def stmt = "select * from my_table where IDX('other_value') < IDX(my_field)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def ex = thrown SQLParseException
        ex.getMessage() == "Indexes cannot be compared with other fields."
    }

    def "verify simple IDX function with other value with other field"() {
        when:
        def stmt = "select * from my_table where IDX('other_value') < my_field"
        service.convertSqlToJumboQuery(stmt)
        then:
        def ex = thrown SQLParseException
        ex.getMessage() == "Indexes cannot be compared with other fields."
    }

    def "verify IDX function with OR to another IDX"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') = 5 OR IDX('another_index') = 6"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 2
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        indexQuery.get(1).getName() == 'another_index'
        indexQuery.get(1).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(1).getValue() == 6
    }

    def "verify IDX function with IN"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') IN (1,2,3)"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 3
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 1
        indexQuery.get(1).getName() == 'my_index'
        indexQuery.get(1).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(1).getValue() == 2
        indexQuery.get(2).getName() == 'my_index'
        indexQuery.get(2).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(2).getValue() == 3
    }


    def "verify IDX function with AND to another IDX"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') = 5 AND IDX('another_index') = 6"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 1
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        def andIndex = indexQuery.get(0).getIndexAnd()
        andIndex.getName() == 'another_index'
        andIndex.getQueryOperation() == QueryOperation.EQ
        andIndex.getValue() == 6
    }

    def "verify IDX function with OR to data field"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') = 5 or my_field = 3"
        service.convertSqlToJumboQuery(stmt)
        then:
        def ex = thrown SQLParseException
        ex.getMessage() == "It is not allowed to combine a query on an index with OR to a field, because it results in a full scan and index resolution is not required and inperformant. Rebuild your query by not using indexes."
    }


    def "verify IDX function with AND to data field"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') = 5 AND my_field = 6"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 1
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        def andData = indexQuery.get(0).getDataAnd()
        andData.getQueryOperation() == QueryOperation.EQ
        andData.getLeft() == 'my_field'
        andData.getLeftType() == FieldType.FIELD
        andData.getRight() == 6
        andData.getRightType() == FieldType.VALUE
    }

    def "verify IDX function with AND two data fields"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') = 5 AND my_field = 6 and second_field = 7"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 1
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        def andData = indexQuery.get(0).getDataAnd()
        andData.getQueryOperation() == QueryOperation.EQ
        andData.getLeft() == 'my_field'
        andData.getLeftType() == FieldType.FIELD
        andData.getRight() == 6
        andData.getRightType() == FieldType.VALUE
        def andData2 = andData.getDataAnd()
        andData2.getQueryOperation() == QueryOperation.EQ
        andData2.getLeft() == 'second_field'
        andData2.getLeftType() == FieldType.FIELD
        andData2.getRight() == 7
        andData2.getRightType() == FieldType.VALUE
    }

    def "verify IDX function with AND to data field, data field is left and should be optimized as subAnd of the index"() {
        when:
        def stmt = "select * from my_table where my_field = 6 and IDX('my_index') = 5"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 1
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        def andData = indexQuery.get(0).getDataAnd()
        andData.getQueryOperation() == QueryOperation.EQ
        andData.getLeft() == 'my_field'
        andData.getLeftType() == FieldType.FIELD
        andData.getRight() == 6
        andData.getRightType() == FieldType.VALUE
    }

    def "verify IDX function with AND on two indexes"() {
        when:
        def stmt = "select * from my_table where IDX('my_index') = 6 and IDX('another_index') = 5"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 1
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 6
        def andIndex = indexQuery.get(0).getIndexAnd()
        andIndex.getQueryOperation() == QueryOperation.EQ
        andIndex.getName() == 'another_index'
        andIndex.getValue() == 5
    }

    def "verify IDX function embedded in and condition"() {
        when:
        def stmt = "select * from test where ((idx(aaaa) = 'aaa' AND bb = 'bb') OR user.cc = 'bb')"
        service.convertSqlToJumboQuery(stmt)
        then:
        def ex = thrown SQLParseException
        ex.getMessage() == "It is not allowed to combine a query on an index with OR to a field, because it results in a full scan and index resolution is not required and inperformant. Rebuild your query by not using indexes."
    }

    def "verify IDX function with IN and data and condition"() {
        when:
        def stmt = "select * from my_table where my_field = 6 and IDX('my_index') IN (5, 6, 7)"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 3
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        def andData1 = indexQuery.get(0).getDataAnd()
        andData1.getQueryOperation() == QueryOperation.EQ
        andData1.getLeft() == 'my_field'
        andData1.getLeftType() == FieldType.FIELD
        andData1.getRight() == 6
        andData1.getRightType() == FieldType.VALUE
        indexQuery.get(1).getName() == 'my_index'
        indexQuery.get(1).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(1).getValue() == 6
        def andData2 = indexQuery.get(1).getDataAnd()
        andData2.getQueryOperation() == QueryOperation.EQ
        andData2.getLeft() == 'my_field'
        andData2.getLeftType() == FieldType.FIELD
        andData2.getRight() == 6
        andData2.getRightType() == FieldType.VALUE
        indexQuery.get(2).getName() == 'my_index'
        indexQuery.get(2).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(2).getValue() == 7
        def andData3 = indexQuery.get(1).getDataAnd()
        andData3.getQueryOperation() == QueryOperation.EQ
        andData3.getLeft() == 'my_field'
        andData3.getLeftType() == FieldType.FIELD
        andData3.getRight() == 6
        andData3.getRightType() == FieldType.VALUE
    }

    def "verify IDX function with OR and data AND condition"() {
        when:
        def stmt = "select * from my_table where my_field = 4 and (IDX('my_index') = 5 OR IDX('my_index') = 6 OR IDX('my_index') = 7)"
        def query = service.convertSqlToJumboQuery(stmt)
        def indexQuery = query.getIndexOrs()
        then:
        query.getDataOrs().size() == 0
        indexQuery.size() == 3
        indexQuery.get(0).getName() == 'my_index'
        indexQuery.get(0).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(0).getValue() == 5
        def andData1 = indexQuery.get(0).getDataAnd()
        andData1.getQueryOperation() == QueryOperation.EQ
        andData1.getLeft() == 'my_field'
        andData1.getLeftType() == FieldType.FIELD
        andData1.getRight() == 4
        andData1.getRightType() == FieldType.VALUE
        indexQuery.get(1).getName() == 'my_index'
        indexQuery.get(1).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(1).getValue() == 6
        def andData2 = indexQuery.get(1).getDataAnd()
        andData2.getQueryOperation() == QueryOperation.EQ
        andData2.getLeft() == 'my_field'
        andData2.getLeftType() == FieldType.FIELD
        andData2.getRight() == 4
        andData2.getRightType() == FieldType.VALUE
        indexQuery.get(2).getName() == 'my_index'
        indexQuery.get(2).getQueryOperation() == QueryOperation.EQ
        indexQuery.get(2).getValue() == 7
        def andData3 = indexQuery.get(1).getDataAnd()
        andData3.getQueryOperation() == QueryOperation.EQ
        andData3.getLeft() == 'my_field'
        andData3.getLeftType() == FieldType.FIELD
        andData3.getRight() == 4
        andData3.getRightType() == FieldType.VALUE
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
        def where = query.getDataOrs().get(0);
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
        def where = query.getDataOrs().get(0);
        then:
        where.queryOperation == QueryOperation.LT
        where.left == 'my_date_field'
        where.leftType == FieldType.FIELD
        where.hintType == HintType.DATE
        where.right == 1355310732000
        where.rightType == FieldType.VALUE
    }

    def "verify TO_DATE function"() {
        when:
        def stmt = "select * from my_table where my_date_field < TO_DATE('2012-12-12 12:12:12', 'yyyy-MM-dd HH:mm:ss')"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs().get(0);
        then:
        where.queryOperation == QueryOperation.LT
        where.left == 'my_date_field'
        where.leftType == FieldType.FIELD
        where.hintType == HintType.DATE
        where.right == 1355310732000
        where.rightType == FieldType.VALUE
    }

    def "verify DATE_FIELD function"() {
        when:
        def stmt = "select * from my_table where DATE_FIELD('my_date_field') < DATE_FIELD('another_field')"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs().get(0);
        then:
        where.queryOperation == QueryOperation.LT
        where.left == 'my_date_field'
        where.leftType == FieldType.FIELD
        where.hintType == HintType.DATE
        where.right == 'another_field'
        where.rightType == FieldType.FIELD
    }

    def "verify GEO_BOUNDARY_BOX function"() {
        when:
        def stmt = "select * from my_table where GEO_BOUNDARY_BOX(my_point, 22.22, 33.33, 44.44, 55.55)"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs().get(0);
        then:
        where.queryOperation == QueryOperation.GEO_BOUNDARY_BOX
        where.left == 'my_point'
        where.leftType == FieldType.FIELD
        where.right == [[22.22, 33.33], [44.44, 55.55]]
        where.rightType == FieldType.VALUE
    }

    def "verify GEO_WITHIN_RANGE_METER function"() {
        when:
        def stmt = "select * from my_table where GEO_WITHIN_RANGE_METER(my_point, 22.22, 33.33, 1000)"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs().get(0);
        then:
        where.queryOperation == QueryOperation.GEO_WITHIN_RANGE_METER
        where.left == 'my_point'
        where.leftType == FieldType.FIELD
        where.right == [[22.22, 33.33], 1000]
        where.rightType == FieldType.VALUE
    }

    def "verify GEO_BOUNDARY_BOX function with IDX"() {
        when:
        def stmt = "select * from my_table where GEO_BOUNDARY_BOX(IDX(my_point), 22.22, 33.33, 44.44, 55.55)"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getIndexOrs().get(0);
        then:
        where.queryOperation == QueryOperation.GEO_BOUNDARY_BOX
        where.name == 'my_point'
        where.value == [[22.22, 33.33], [44.44, 55.55]]
    }

    def "verify GEO_WITHIN_RANGE_METER function with IDX"() {
        when:
        def stmt = "select * from my_table where GEO_WITHIN_RANGE_METER(IDX(my_point), 22.22, 33.33, 1000)"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getIndexOrs().get(0);
        then:
        where.queryOperation == QueryOperation.GEO_WITHIN_RANGE_METER
        where.name == 'my_point'
        where.value == [[22.22, 33.33], 1000]
    }

    def "verify where BETWEEN clause"() {
        when:
        def stmt = "select * from my_table where my_field BETWEEN 4 AND 9"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs().get(0);
        then:
        where.queryOperation == QueryOperation.BETWEEN
        where.left == 'my_field'
        where.leftType == FieldType.FIELD
        where.right == [4, 9]
        where.rightType == FieldType.VALUE
    }
    def "verify where BETWEEN clause with IDX"() {
        when:
        def stmt = "select * from my_table where IDX(my_field) BETWEEN 4 AND 9"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getIndexOrs().get(0);
        then:
        where.queryOperation == QueryOperation.BETWEEN
        where.name == 'my_field'
        where.value == [4, 9]
    }

    def "verify where IN clause with IDX"() {
        when:
        def stmt = "select * from my_table where IDX(my_field) IN (5, 9, 11)"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getIndexOrs()
        then:
        where.size() == 3
        where.get(0).queryOperation == QueryOperation.EQ
        where.get(0).name == 'my_field'
        where.get(0).value == 5
        where.get(1).queryOperation == QueryOperation.EQ
        where.get(1).name == 'my_field'
        where.get(1).value == 9
        where.get(2).queryOperation == QueryOperation.EQ
        where.get(2).name == 'my_field'
        where.get(2).value == 11
    }


    def "verify where IN clause"() {
        when:
        def stmt = "select * from my_table where my_field IN (5, 9, 11)"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs()
        then:
        where.size() == 3
        where.get(0).queryOperation == QueryOperation.EQ
        where.get(0).left == 'my_field'
        where.get(0).leftType == FieldType.FIELD
        where.get(0).right == 5
        where.get(0).rightType == FieldType.VALUE
        where.get(1).queryOperation == QueryOperation.EQ
        where.get(1).left == 'my_field'
        where.get(1).leftType == FieldType.FIELD
        where.get(1).right == 9
        where.get(1).rightType == FieldType.VALUE
        where.get(2).queryOperation == QueryOperation.EQ
        where.get(2).left == 'my_field'
        where.get(2).leftType == FieldType.FIELD
        where.get(2).right == 11
        where.get(2).rightType == FieldType.VALUE
    }

    def "verify where LIKE clause"() {
        when:
        def stmt = "select * from my_table where my_field LIKE '%xxx%'"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs()
        then:
        where.size() == 1
        where.get(0).queryOperation == QueryOperation.LIKE
        where.get(0).left == 'my_field'
        where.get(0).leftType == FieldType.FIELD
        where.get(0).right == '%xxx%'
        where.get(0).rightType == FieldType.VALUE
    }

    def "verify where LIKE clause with IDX"() {
        when:
        def stmt = "select * from my_table where IDX(my_field) LIKE '%xxx%'"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getIndexOrs()
        then:
        where.size() == 1
        where.get(0).queryOperation == QueryOperation.LIKE
        where.get(0).name == 'my_field'
        where.get(0).value == '%xxx%'
    }

    def "verify where NOT LIKE clause"() {
        when:
        def stmt = "select * from my_table where my_field NOT LIKE '%xxx%'"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs()
        then:
        where.size() == 1
        where.get(0).queryOperation == QueryOperation.NOT_LIKE
        where.get(0).left == 'my_field'
        where.get(0).leftType == FieldType.FIELD
        where.get(0).right == '%xxx%'
        where.get(0).rightType == FieldType.VALUE
    }


    def "verify where IS NULL clause"() {
        when:
        def stmt = "select * from my_table where my_field IS NULL"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs()
        then:
        where.size() == 1
        where.get(0).queryOperation == QueryOperation.IS_NULL
        where.get(0).left == 'my_field'
        where.get(0).leftType == FieldType.FIELD
        where.get(0).rightType == FieldType.NOT_SET
    }

    def "verify where IS NOT NULL clause"() {
        when:
        def stmt = "select * from my_table where my_field IS NOT NULL"
        def query = service.convertSqlToJumboQuery(stmt)
        def where = query.getDataOrs()
        then:
        where.size() == 1
        where.get(0).queryOperation == QueryOperation.IS_NOT_NULL
        where.get(0).left == 'my_field'
        where.get(0).leftType == FieldType.FIELD
        where.get(0).rightType == FieldType.NOT_SET
    }

    def "verify CACHE hint"() {
        when:
        def stmt = "select * from my_table CACHE"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.isResultCacheEnabled() == true
    }

    def "verify NOCACHE hint"() {
        when:
        def stmt = "select * from my_table NOCACHE"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.isResultCacheEnabled() == false
    }

    def "verify CACHE enabled by default"() {
        when:
        def stmt = "select * from my_table"
        def query = service.convertSqlToJumboQuery(stmt)
        then:
        query.isResultCacheEnabled() == true
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

    def "verify unsupported SQL feature: ANY clause"() {
        when:
        def stmt = "select * from my_table where my_field = ANY (select x from col)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "ANY is not supported."
    }

    def "verify unsupported SQL feature: ALL clause"() {
        when:
        def stmt = "select * from my_table where my_field = ALL (select x from col)"
        service.convertSqlToJumboQuery(stmt)
        then:
        def e = thrown SQLParseException
        e.getMessage() == "ALL is not supported."
    }

}
