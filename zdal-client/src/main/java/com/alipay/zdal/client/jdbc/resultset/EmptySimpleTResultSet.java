/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.client.jdbc.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.alipay.zdal.client.jdbc.ZdalStatement;

/**
 * 璋冪敤rs.next姘歌繙杩斿洖false鐨勭┖缁撴灉闆嗐��
 * 涓昏鐢ㄤ簬涓�浜涚壒娈婄殑鎯呭喌
 * 
 */
public class EmptySimpleTResultSet extends AbstractTResultSet {

    public EmptySimpleTResultSet(ZdalStatement statementProxy, List<ResultSet> resultSets) {
        super(statementProxy, resultSets);
    }

    @Override
    public boolean next() throws SQLException {
        return false;
    }

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return null;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return null;
	}
}
