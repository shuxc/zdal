package com.alipay.zdal.test.ut.sqlparser.mysql;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import junit.framework.Assert;

import com.alipay.zdal.parser.sql.ast.SQLStatement;
import com.alipay.zdal.parser.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alipay.zdal.parser.sql.stat.TableStat.Column;
import com.alipay.zdal.parser.visitor.ZdalMySqlSchemaStatVisitor;


public class MySqlSchemaStatVisitorTest_Select_Limit   {
@Test
    public void test_0() throws Exception {
        String sql = "select inst_id, finance_exchange_code"
                     + " from fin_retrieval_serial"
                     + " where  gmtcreate in (?,?,?) and gmtmodify<? and id not in (?,10) order by gmtcreate limit 5,9";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statemen = statementList.get(0);

        Assert.assertEquals(1, statementList.size());

        ZdalMySqlSchemaStatVisitor visitor = new ZdalMySqlSchemaStatVisitor();
        statemen.accept(visitor);

        System.out.println(sql);
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
        System.out.println("alias : " + visitor.getAliasMap());
        System.out.println("orderBy : " + visitor.getOrderByColumns());
        System.out.println("groupBy : " + visitor.getGroupByColumns());
        System.out.println("variant : " + visitor.getVariants());
        System.out.println("relationShip : " + visitor.getRelationships());
        System.out.println("bindColumns : " + visitor.getBindVarConditions());
        System.out.println("noBindColumns : " + visitor.getNoBindVarConditions());
        System.out.println("limits : " + visitor.getLimits());
        System.out.println("condition = " + visitor.getConditions());

        Assert.assertEquals(1, visitor.getTables().size());
        Assert.assertEquals(true, visitor.containsTable("fin_retrieval_serial"));

        Assert.assertEquals(5, visitor.getColumns().size());
        Assert.assertEquals(true, visitor.getColumns().contains(
            new Column("fin_retrieval_serial", "finance_exchange_code")));
        Assert.assertEquals(true, visitor.getColumns().contains(
            new Column("fin_retrieval_serial", "inst_id")));

        Set<Column> columns = visitor.getColumns();

        for (Column column : columns) {
            System.out.println("column=" + column);
        }

    }
}
