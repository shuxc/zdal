/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.parser;

import com.alipay.zdal.common.DBType;
import com.alipay.zdal.parser.result.SqlParserResult;

/**
 * SQL瑙ｆ瀽鍣ㄥ熀绫�
 * 
 * @author xiaoqing.zhouxq
 * @version $Id: SQLParser.java, v 0.1 2012-5-22 涓婂崍09:59:15 xiaoqing.zhouxq Exp $
 */
public interface SQLParser {

    /**
     * 瑙ｆ瀽sql璇彞,瀵逛簬涓嶅悓dbType閲囩敤涓嶅悓鐨剆ql瑙ｆ瀽鍣�.
     * @param sql
     * @param dbType
     * @return
     */
    SqlParserResult parse(String sql, DBType dbType);
}
