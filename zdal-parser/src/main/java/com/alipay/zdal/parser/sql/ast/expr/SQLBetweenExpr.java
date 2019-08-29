/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.parser.sql.ast.expr;

import java.io.Serializable;

import com.alipay.zdal.parser.sql.ast.SQLExpr;
import com.alipay.zdal.parser.sql.ast.SQLExprImpl;
import com.alipay.zdal.parser.sql.visitor.SQLASTVisitor;

/**
 * 
 * @author xiaoqing.zhouxq
 * @version $Id: SQLBetweenExpr.java, v 0.1 2012-11-17 下午3:14:49 xiaoqing.zhouxq Exp $
 */
public class SQLBetweenExpr extends SQLExprImpl implements Serializable {

    private static final long serialVersionUID = 1L;
    public SQLExpr            testExpr;
    private boolean           not;
    public SQLExpr            beginExpr;
    public SQLExpr            endExpr;

    public SQLBetweenExpr() {

    }

    public SQLBetweenExpr(SQLExpr testExpr, SQLExpr beginExpr, SQLExpr endExpr) {

        this.testExpr = testExpr;
        this.beginExpr = beginExpr;
        this.endExpr = endExpr;
    }

    public SQLBetweenExpr(SQLExpr testExpr, boolean not, SQLExpr beginExpr, SQLExpr endExpr) {

        this.testExpr = testExpr;
        this.not = not;
        this.beginExpr = beginExpr;
        this.endExpr = endExpr;
    }

    public void output(StringBuffer buf) {
        this.testExpr.output(buf);
        if (this.not)
            buf.append(" NOT BETWEEN ");
        else {
            buf.append(" BETWEEN ");
        }
        this.beginExpr.output(buf);
        buf.append(" AND ");
        this.endExpr.output(buf);
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.testExpr);
            acceptChild(visitor, this.beginExpr);
            acceptChild(visitor, this.endExpr);
        }
        visitor.endVisit(this);
    }

    public SQLExpr getTestExpr() {
        return this.testExpr;
    }

    public void setTestExpr(SQLExpr testExpr) {
        this.testExpr = testExpr;
    }

    public boolean isNot() {
        return this.not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public SQLExpr getBeginExpr() {
        return this.beginExpr;
    }

    public void setBeginExpr(SQLExpr beginExpr) {
        this.beginExpr = beginExpr;
    }

    public SQLExpr getEndExpr() {
        return this.endExpr;
    }

    public void setEndExpr(SQLExpr endExpr) {
        this.endExpr = endExpr;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((beginExpr == null) ? 0 : beginExpr.hashCode());
        result = prime * result + ((endExpr == null) ? 0 : endExpr.hashCode());
        result = prime * result + (not ? 1231 : 1237);
        result = prime * result + ((testExpr == null) ? 0 : testExpr.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SQLBetweenExpr other = (SQLBetweenExpr) obj;
        if (beginExpr == null) {
            if (other.beginExpr != null) {
                return false;
            }
        } else if (!beginExpr.equals(other.beginExpr)) {
            return false;
        }
        if (endExpr == null) {
            if (other.endExpr != null) {
                return false;
            }
        } else if (!endExpr.equals(other.endExpr)) {
            return false;
        }
        if (not != other.not) {
            return false;
        }
        if (testExpr == null) {
            if (other.testExpr != null) {
                return false;
            }
        } else if (!testExpr.equals(other.testExpr)) {
            return false;
        }
        return true;
    }
}
