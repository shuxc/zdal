/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.jdbc.sorter;

import java.sql.SQLException;

/**
 * The ExceptionSorter interface allows for <code>java.sql.SQLException</code>
 * evaluation to determine if an error is fatal. 
 *
 * 
 * 
 * @author ����
 * @version $Id: ExceptionSorter.java, v 0.1 2014-1-6 ����05:20:01 Exp $
 */
public interface ExceptionSorter {

    /**
     * Evaluates a <code>java.sql.SQLException</code> to determine if
     * the error was fatal
     * 
     * @param e the <code>java.sql.SQLException</code>
     * 
     * @return whether or not the exception is vatal.
     */
    boolean isExceptionFatal(SQLException e);

    /** rollbackʧ�ܵ�ʱ��ֱ���׳����errorcode��zdal-datasource��������쳣ֱ���޳�����. */
    public static final int ROLLBACK_ERRORCODE = 999999;
}
