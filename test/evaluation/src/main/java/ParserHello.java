import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jumbodb.common.query.JsonQuery;

import java.util.List;

public class ParserHello {
    public static void main(String[] args) throws Exception {
        // CARSTEN diese queries nicht suporten:
//  diese query wird nicht supported, da index embedded und full scan außen
//  "select * from test a where ((idx(aaaa, ddd) = 'aaa' OR bb = 'bb') AND user.cc = 'bb') limit 10"


        // no cache hint ans ende
        // CARSTEN column and column vergleich wär cool a = b
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where ((a = 'b' or z = 'z') and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h') and (g = 'g' or y = 'y' or o = 'o')) or x = 'x'");
        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where aaa(aa)");



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

    }
}