/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.parser.exceptions;

/**
 * sqlparser鐨勫紓甯哥被.
 * @author xiaoqing.zhouxq
 * @version $Id: SqlParserException.java, v 0.1 2012-5-29 涓婂崍09:34:44 xiaoqing.zhouxq Exp $
 */
public class SqlParserException extends RuntimeException {

    /**  */
    private static final long serialVersionUID = 1L;

    public SqlParserException() {
    }

    public SqlParserException(String message) {
        super(message);
    }

    public SqlParserException(String message, Throwable e) {
        super(message, e);
    }
}
