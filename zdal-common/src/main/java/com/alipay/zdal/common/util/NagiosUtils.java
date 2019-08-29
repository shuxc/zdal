/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.util;

import org.apache.log4j.Logger;

/**
 * 
 * @author ����
 * @version $Id: NagiosUtils.java, v 0.1 2014-1-6 ����05:22:18 Exp $
 */
public class NagiosUtils {
    private static final Logger nagiosLog                  = Logger.getLogger("Nagios");

    public static final String  KEY_DB_NOT_AVAILABLE       = "DB_NOT_AVAILABLE";        //���ݿⲻ����,KEYǰ׺+dbindex
    public static final String  KEY_SQL_PARSE_FAIL         = "SQL_PARSE_FAIL";          //ҵ��ִ���������SQL��ɽ���ʧ��
    public static final String  KEY_REPLICATION_FAIL_RATE  = "REPLICATION_FAIL_RATE";   //�и���ʧ����
    public static final String  KEY_REPLICATION_TIME_AVG   = "REPLICATION_TIME_AVG";    //һ��ʱ���ڵ��и���ƽ����Ӧʱ��
    public static final String  KEY_INSERT_LOGDB_FAIL_RATE = "INSERT_LOGDB_FAIL_RATE";  //����־��ʧ����
    public static final String  KEY_INSERT_LOGDB_TIME_AVG  = "INSERT_LOGDB_TIME_AVG";   //һ��ʱ���ڵĲ���־��ƽ����Ӧʱ��

    public static void addNagiosLog(String key, String value) {
        key = key.replaceAll(":", "_");
        key = key.replaceAll(",", "|");
        value = value.replaceAll(":", "_");
        value = value.replaceAll(",", "|");
        innerAddNagiosLog(key, value);
    }

    public static void addNagiosLog(String key, int value) {
        key = key.replaceAll(":", "_");
        key = key.replaceAll(",", "|");
        innerAddNagiosLog(key, Integer.toString(value));
    }

    public static void addNagiosLog(String key, long value) {
        key = key.replaceAll(":", "_");
        key = key.replaceAll(",", "|");
        innerAddNagiosLog(key, Long.toString(value));
    }

    public static void addNagiosLog(String host, String key, String value) {
        host = host.replaceAll(":", "_");
        host = host.replaceAll(",", "|");
        key = key.replaceAll(":", "_");
        key = key.replaceAll(",", "|");
        value = value.replaceAll(":", "_");
        value = value.replaceAll(",", "|");
        innerAddNagiosLog(host, key, value);
    }

    public static void addNagiosLog(String host, String key, int value) {
        host = host.replaceAll(":", "_");
        host = host.replaceAll(",", "|");
        key = key.replaceAll(":", "_");
        key = key.replaceAll(",", "|");
        innerAddNagiosLog(host, key, Integer.toString(value));
    }

    public static void addNagiosLog(String host, String key, long value) {
        host = host.replaceAll(":", "_");
        host = host.replaceAll(",", "|");
        key = key.replaceAll(":", "_");
        key = key.replaceAll(",", "|");
        innerAddNagiosLog(host, key, Long.toString(value));
    }

    private static void innerAddNagiosLog(String key, String value) {
        StringBuilder sb = new StringBuilder();
        sb.append(key);
        sb.append(":");
        sb.append(value);
        if (nagiosLog.isDebugEnabled()) {
            nagiosLog.debug(sb.toString());
        }
    }

    private static void innerAddNagiosLog(String host, String key, String value) {
        StringBuilder sb = new StringBuilder();
        sb.append(host);
        sb.append("_");
        sb.append(key);
        sb.append(":");
        sb.append(value);
        if (nagiosLog.isDebugEnabled()) {
            nagiosLog.debug(sb.toString());
        }
    }
}
