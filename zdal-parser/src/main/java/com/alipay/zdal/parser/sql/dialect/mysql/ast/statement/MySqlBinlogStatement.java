package com.alipay.zdal.parser.sql.dialect.mysql.ast.statement;

import com.alipay.zdal.parser.sql.ast.SQLExpr;
import com.alipay.zdal.parser.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class MySqlBinlogStatement  extends MySqlStatementImpl{
    private SQLExpr expr;
    public SQLExpr getExpr() {
        return expr;
    }

    public void setExpr(SQLExpr expr) {
        this.expr = expr;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        //TODO 姝ょ被鏂板 by shuxc_dev
        if(visitor.visit(this)){
            acceptChild(visitor,expr);
            accept0((MySqlASTVisitor) visitor);
        }
        visitor.endVisit(this);
    }




}
