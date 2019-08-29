/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.sql.DataSource;

/**
 * ZDataSourc�ĳ����࣬��javax.sql.DataSource�еķ������е�һ��
 * 
 */
public abstract class AbstractDataSource implements DataSource {

    /**  ����zds  */
    protected static final ArrayList<ZDataSource> zdatasourceList = new ArrayList<ZDataSource>();

    /**
     * ����ʱ����Դ�����б�
     */
    public AbstractDataSource() {
        synchronized (zdatasourceList) {
            zdatasourceList.add((ZDataSource) this);
        }
    }

    /**
     * get current datasource
     * 
     * @return
     * @throws SQLException
     */
    protected abstract DataSource getDatasource() throws SQLException;

    /**
     * 
     * @see javax.sql.DataSource#getConnection()
     */
    public Connection getConnection() throws SQLException {
        return getDatasource().getConnection();
    }

    /**
     * 
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    public Connection getConnection(String username, String password) throws SQLException {
        return getDatasource().getConnection(username, password);
    }

    /**
     * 
     * @see javax.sql.CommonDataSource#getLogWriter()
     */
    public PrintWriter getLogWriter() throws SQLException {
        return getDatasource().getLogWriter();
    }

    /**
     * 
     * @see javax.sql.CommonDataSource#getLoginTimeout()
     */
    public int getLoginTimeout() throws SQLException {
        return getDatasource().getLoginTimeout();
    }

    /**
     * 
     * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter out) throws SQLException {
        getDatasource().setLogWriter(out);
    }

    /**
     * 
     * @see javax.sql.CommonDataSource#setLoginTimeout(int)
     */
    public void setLoginTimeout(int seconds) throws SQLException {
        getDatasource().setLoginTimeout(seconds);
    }

    public static ArrayList<ZDataSource> getZdatasourcelist() {
        return zdatasourceList;
    }

}
