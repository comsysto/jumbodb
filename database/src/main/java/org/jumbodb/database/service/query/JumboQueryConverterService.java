package org.jumbodb.database.service.query;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.sql.WhereVisitor;

import java.util.List;

/**
 * @author Carsten Hufe
 */
// CARSTEN unit test, beispiel abfragen in ParserHello
public class JumboQueryConverterService {

    public JumboQuery convertSqlToJumboQuery(String sql) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if(stmt instanceof Select) {
                Select select = (Select)stmt;
                PlainSelect selectBody = (PlainSelect) select.getSelectBody(); // nur plain select
                assertSqlFeatures(selectBody);
                return buildJumboQuery(selectBody);
            }
            throw new JumboCommonException("Only plain select statements are allowed!");
        } catch (JSQLParserException e) {
            throw new JumboCommonException(e.getCause().getMessage());
        }
    }

    private JumboQuery buildJumboQuery(PlainSelect selectBody) {
        JumboQuery jumboQuery = new JumboQuery();
        jumboQuery.setCollection(selectBody.getFromItem().toString());
        if(selectBody.getLimit() != null) {
            jumboQuery.setLimit((int)selectBody.getLimit().getRowCount());
        }
        WhereVisitor expressionVisitor = new WhereVisitor();
        Expression where = selectBody.getWhere();
        if(where != null) {
            where.accept(expressionVisitor);
            jumboQuery.setJsonQuery(expressionVisitor.getOrs());
        }
        return jumboQuery;
    }

    private void assertSqlFeatures(PlainSelect selectBody) {
        // CARSTEN throw exceptions when features like distinct etc are used!
    }

    public static void main(String[] args) throws JSQLParserException {
        // CARSTEN diese queries nicht suporten:
//  diese query wird nicht supported, da index embedded und full scan außen
//  "select * from test a where ((idx(aaaa, ddd) = 'aaa' OR bb = 'bb') AND user.cc = 'bb') limit 10"


        // no cache hint ans ende
        // CARSTEN column and column vergleich wär cool a = b
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where ((a = 'b' or z = 'z') and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h') and (g = 'g' or y = 'y' or o = 'o')) or x = 'x'");
        long start = System.currentTimeMillis();
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where aaa = 'bb'");



//        Select stmt = (Select)CCJSqlParserUtil.parse("select * from test a where user.cc = 'bb' limit 10");
//        Select stmt = (Select)CCJSqlParserUtil.parse("select * from test a where ((idx(aaaa, ddd) = 'aaa' AND bb = 'bb') OR user.cc = 'bb') limit 10");
        System.out.println();
        PlainSelect selectBody = (PlainSelect) stmt.getSelectBody(); // nur plain select
//        TablesNamesFinder selectVisitor = new TablesNamesFinder();
//        System.out.println("tables " + selectVisitor.getTableList(stmt));
        System.out.println("joins " + selectBody.getJoins()); // wenn != null error joins are not supported
        System.out.println("distinct " + selectBody.getDistinct()); // wenn != null, denn nicht supported
//        System.out.println("offset " + selectBody.getLimit().getOffset()); // offset kann nur 0 sein da parallel
//        System.out.println("row count " + selectBody.getLimit().getRowCount());
        System.out.println("group " + selectBody.getGroupByColumnReferences()); // muss null, wird supported
        System.out.println("having " + selectBody.getHaving()); // muss null, da nicht supported
        System.out.println("selected items " + selectBody.getSelectItems());
        System.out.println("from item " + selectBody.getFromItem().toString()); // muss null kein alias support
//        System.out.println("from item alias  " + selectBody.getFromItem().getAlias().getName()); // wenn kein alias getAlias == null
        WhereVisitor expressionVisitor = new WhereVisitor();
        selectBody.getWhere().accept(expressionVisitor);
        List<JsonQuery> jsonQueries = expressionVisitor.getOrs();
        System.out.println(jsonQueries);
        System.out.println("time " + ( System.currentTimeMillis() - start));
    }
}