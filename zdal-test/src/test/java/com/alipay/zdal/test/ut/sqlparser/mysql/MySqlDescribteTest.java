package com.alipay.zdal.test.ut.sqlparser.mysql;

import java.util.List;

import org.junit.Test;

import junit.framework.Assert;

import com.alipay.zdal.parser.sql.ast.SQLStatement;
import com.alipay.zdal.parser.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alipay.zdal.parser.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alipay.zdal.parser.sql.stat.TableStat;

public class MySqlDescribteTest {
	@Test
	 public void test_0() throws Exception {
	        String sql = " DESCRIBE City";

	        MySqlStatementParser parser = new MySqlStatementParser(sql);
	        List<SQLStatement> statementList = parser.parseStatementList();
	        SQLStatement statemen = statementList.get(0);
	    

	        Assert.assertEquals(1, statementList.size());

	        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
	        statemen.accept(visitor);

	        System.out.println("Tables : " + visitor.getTables());
	        System.out.println("fields : " + visitor.getColumns());
	        System.out.println("coditions : " + visitor.getConditions());
	        System.out.println("orderBy : " + visitor.getOrderByColumns());

	        Assert.assertEquals(1, visitor.getTables().size());
	        Assert.assertEquals(0, visitor.getColumns().size());
	        Assert.assertEquals(0, visitor.getConditions().size());

	        Assert.assertTrue(visitor.getTables().containsKey(new TableStat.Name("City")));
	        //        Assert.assertTrue(visitor.getTables().containsKey(new TableStat.Name("t2")));

	        //        Assert.assertTrue(visitor.getColumns().contains(new Column("t2", "id")));
	    }

}
