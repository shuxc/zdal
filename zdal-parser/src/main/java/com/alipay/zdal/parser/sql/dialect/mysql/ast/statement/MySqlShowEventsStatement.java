/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.parser.sql.dialect.mysql.ast.statement;

import com.alipay.zdal.parser.sql.ast.SQLExpr;
import com.alipay.zdal.parser.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * 
 * @author 伯牙
 * @version $Id: MySqlShowEventsStatement.java, v 0.1 2012-11-17 下午3:36:28 Exp $
 */
public class MySqlShowEventsStatement extends MySqlStatementImpl {

    private static final long serialVersionUID = 1L;

    private SQLExpr           schema;
    private SQLExpr           like;
    private SQLExpr           where;

    public SQLExpr getSchema() {
        return schema;
    }

    public void setSchema(SQLExpr schema) {
        this.schema = schema;
    }

    public SQLExpr getLike() {
        return like;
    }

    public void setLike(SQLExpr like) {
        this.like = like;
    }

    public SQLExpr getWhere() {
        return where;
    }

    public void setWhere(SQLExpr where) {
        this.where = where;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, schema);
            acceptChild(visitor, like);
            acceptChild(visitor, where);
        }
        visitor.endVisit(this);
    }
}
