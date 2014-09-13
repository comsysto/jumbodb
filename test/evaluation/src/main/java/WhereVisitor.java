import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.QueryOperation;


import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 12.09.2014.
 */
public class WhereVisitor extends ExpressionVisitorAdapter {
    private QueryOperation operation;
    private Column column;
    private Object value;
    private List<JsonQuery> ors = new LinkedList<JsonQuery>();
    private List<JsonQuery> ands = new LinkedList<JsonQuery>();
//    private List<List<JsonQuery>> orInAnds = new LinkedList<List<JsonQuery>>();
//    private JsonQuery and;
    private JsonQuery current;



    public List<JsonQuery> getOrs() {
        return ors;
    }

//    public JsonQuery getAnd() {
//        return and;
//    }

    @Override
    public void visit(AndExpression expr) {
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        // CARSTEN and kann auch paranthesis sein
//                JsonQuery jsonQuery = new JsonQuery(column.getFullyQualifiedName(), Arrays.asList(new QueryClause(operation, value)));
//
//                System.out.println("and  " + jsonQuery);
//        List<JsonQuery> leftJsonQueries = leftWhereVisitor.jsonQueries;
//        List<JsonQuery> rightJsonQueries = rightWhereVisitor.jsonQueries;
//        // CARSTEN wird irgendwie immer überschrieben....
//        JsonQuery e = leftJsonQueries.get(0);
//        QueryClause queryClause = e.getClauses().get(0);
//        queryClause.setQueryClauses(rightJsonQueries);
//        jsonQueries.add(e);


        // was ist wenn left eine or expression ist?
//        current = leftWhereVisitor.current;
        if(rightWhereVisitor.current != null) {
            ands.add(rightWhereVisitor.current);
        }
        if(leftWhereVisitor.current != null) {
            ands.add(leftWhereVisitor.current);
        }

        if(!leftWhereVisitor.ors.isEmpty()) {
            ands.add(new JsonQuery(leftWhereVisitor.ors));
//            orInAnds.add(leftWhereVisitor.ors);
        }
        if(!rightWhereVisitor.ors.isEmpty()) {
            ands.add(new JsonQuery(rightWhereVisitor.ors));
//            orInAnds.add(rightWhereVisitor.ors);
        }
        ands.addAll(leftWhereVisitor.ands);
        ands.addAll(rightWhereVisitor.ands);

        System.out.println();
//        if(rightWhereVisitor.current != null) {
//            current.getAnds().add(rightWhereVisitor.current);
//            ands.add(current);
//
//        }
//        else {
//            current.getAnds().addAll(rightWhereVisitor.getOrs());
//        }
    }

    @Override
    public void visit(OrExpression expr) {
//        WhereVisitor whereVisitor = new WhereVisitor();
//        expr.accept(whereVisitor);
//        super.visit(expr);
        WhereVisitor leftWhereVisitor = new WhereVisitor();
        expr.getLeftExpression().accept(leftWhereVisitor);
        WhereVisitor rightWhereVisitor = new WhereVisitor();
        expr.getRightExpression().accept(rightWhereVisitor);
        List<JsonQuery> leftJsonQueries = leftWhereVisitor.ors;
        List<JsonQuery> rightJsonQueries = rightWhereVisitor.ors;
        ors.addAll(leftJsonQueries);
        ors.addAll(rightJsonQueries);
        if(leftWhereVisitor.current != null) {
            ors.add(leftWhereVisitor.current);
        }
        if(rightWhereVisitor.current != null) {
            ors.add(rightWhereVisitor.current);
        }
        concatAnd(leftWhereVisitor);
        concatAnd(rightWhereVisitor);
//        if(rightWhereVisitor.current != null) {
//            ors.add(rightWhereVisitor.current);
//        }
        // CARSTEN jsonQueries kann optimiert werden für zusammenfassung gleicher felder
//        QueryClause e = new QueryClause(operation, value);
//        jsonQueries.add(new JsonQuery(column.getFullyQualifiedName(), Arrays.asList(e)));
//        jsonQueries.addAll(whereVisitor.jsonQueries);
//        System.out.println("start: or  " + expr);
//
//        JsonQuery jsonQuery = new JsonQuery(column.getFullyQualifiedName(), Arrays.asList(new QueryClause(operation, value)));
////                System.out.println("or  " + jsonQuery);
        System.out.println("or  " + expr);
    }

    private void concatAnd(WhereVisitor leftWhereVisitor) {
        if(!leftWhereVisitor.ands.isEmpty())  {
            JsonQuery last = null;
            for (JsonQuery and : leftWhereVisitor.ands) {
                if(last != null) {
                    and.setAnd(last);
                }
                last = and;
            }
            if(last != null) {
                ors.add(last);
            }

//            // CARSTEN where ('b' = 'a' OR ...) AND ('b' = 'a' OR ...)
//            // CARSTEN last darf eigentlich nicht null sein?
//            if(!leftWhereVisitor.ands.isEmpty()) {
//                JsonQuery last2 = leftWhereVisitor.ands.get(0); // CARSTEN ineffizient linked list
//                for (List<JsonQuery> orInAnd : leftWhereVisitor.orInAnds) {
//
//                    JsonQuery orContainer = new JsonQuery(orInAnd);
//                    last2.setAnd(orContainer);
//                    last2 = orContainer;
//                }
//            }
        }
    }

    @Override
    public void visit(EqualsTo expr) {
        super.visit(expr);
        operation = QueryOperation.EQ;
        current = new JsonQuery(column.getFullyQualifiedName(), operation, value);
        System.out.print("equals on  ");
    }


    @Override
    public void visit(Column column) {
        System.out.print("col  " + column.getFullyQualifiedName());
        this.column = column;
        super.visit(column);
    }

    @Override
    public void visit(LikeExpression expr) {
        System.out.println("like " + expr); // CARSTEN same as EQ
        super.visit(expr);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        // Klammern....
        super.visit(parenthesis);
        System.out.println("parenthesis " + parenthesis);
    }



    @Override
    public void visit(StringValue value) {
        System.out.println("String " + value);
        this.value = value.getValue();
        super.visit(value);
    }

    @Override
    public void visit(GreaterThan expr) {
        super.visit(expr);
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        super.visit(expr);
    }

    @Override
    public void visit(DoubleValue value) {
        super.visit(value);
    }

    @Override
    public void visit(LongValue value) {
        super.visit(value);
    }

    @Override
    public void visit(DateValue value) {
        super.visit(value);
    }

    @Override
    public void visit(IsNullExpression expr) {
        super.visit(expr);
    }

    @Override
    public void visit(NotEqualsTo expr) {
        super.visit(expr);
    }

    @Override
    public void visit(InExpression expr) {
        super.visit(expr);
    }

    @Override
    public void visit(Between expr) {
        super.visit(expr);
    }

    @Override
    public void visit(NullValue value) {
        super.visit(value);
    }

    @Override
    public void visit(MinorThan expr) {
        super.visit(expr);
    }

    @Override
    public void visit(MinorThanEquals expr) {
        super.visit(expr);
    }

    @Override
    public void visit(ExistsExpression expr) {
        super.visit(expr);
    }





    //            @Override
//            public void visit(Function function) {
//                System.out.println("function name " + function.getName());
//                System.out.println("function params 1 " + function.getParameters().getExpressions().get(0));
//                System.out.println("function params 2 " + function.getParameters().getExpressions().get(1));
//            }
}
