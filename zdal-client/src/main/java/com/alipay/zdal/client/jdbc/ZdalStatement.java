/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.client.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.alipay.zdal.client.RouteCondition;
import com.alipay.zdal.client.ThreadLocalString;
import com.alipay.zdal.client.config.DataSourceConfigType;
import com.alipay.zdal.client.controller.RuleController;
import com.alipay.zdal.client.controller.TargetDBMeta;
import com.alipay.zdal.client.dispatcher.DispatcherResult;
import com.alipay.zdal.client.dispatcher.SqlDispatcher;
import com.alipay.zdal.client.jdbc.DBSelector.AbstractDataSourceTryer;
import com.alipay.zdal.client.jdbc.DBSelector.DataSourceTryer;
import com.alipay.zdal.client.jdbc.parameter.ParameterContext;
import com.alipay.zdal.client.jdbc.resultset.CountTResultSet;
import com.alipay.zdal.client.jdbc.resultset.DummyTResultSet;
import com.alipay.zdal.client.jdbc.resultset.EmptySimpleTResultSet;
import com.alipay.zdal.client.jdbc.resultset.MaxTResultSet;
import com.alipay.zdal.client.jdbc.resultset.MinTResultSet;
import com.alipay.zdal.client.jdbc.resultset.OrderByTResultSet;
import com.alipay.zdal.client.jdbc.resultset.SimpleTResultSet;
import com.alipay.zdal.client.jdbc.resultset.SumTResultSet;
import com.alipay.zdal.client.util.ExceptionUtils;
import com.alipay.zdal.client.util.ThreadLocalMap;
import com.alipay.zdal.common.Constants;
import com.alipay.zdal.common.SqlType;
import com.alipay.zdal.common.exception.checked.ZdalCheckedExcption;
import com.alipay.zdal.common.exception.sqlexceptionwrapper.ZdalCommunicationException;
import com.alipay.zdal.common.jdbc.sorter.ExceptionSorter;
import com.alipay.zdal.common.lang.StringUtil;
import com.alipay.zdal.parser.GroupFunctionType;
import com.alipay.zdal.parser.ParserCache;
import com.alipay.zdal.parser.result.DefaultSqlParserResult;
import com.alipay.zdal.parser.visitor.OrderByEle;
import com.alipay.zdal.rule.config.beans.AppRule;
import com.alipay.zdal.rule.ruleengine.entities.retvalue.TargetDB;
import com.alipay.zdal.rule.ruleengine.exception.RuleRuntimeExceptionWrapper;
import com.alipay.zdal.rule.ruleengine.exception.ZdalRuleCalculateException;
import com.alipay.zdal.rule.ruleengine.rule.EmptySetRuntimeException;

/**
 * 
 * @author 浼墮
 * @version $Id: ZdalStatement.java, v 0.1 2014-1-6 涓嬪崍01:19:26 Exp $
 */
public class ZdalStatement implements Statement {
    //TODO: 娣诲姞涓�涓�夐」boolean鍊硷紝鏉ュstatlog杩涜妫�娴�
    private static final Logger       log                            = Logger
                                                                         .getLogger(ZdalStatement.class);
    private static final Logger       sqlLog                         = Logger
                                                                         .getLogger(Constants.CONFIG_LOG_NAME_LOGNAME);

    /**
     * 鐢ㄤ簬鍒ゆ柇鏄惁鏄竴涓猻elect ... for update鐨剆ql
     */
    private static final Pattern      SELECT_FOR_UPDATE_PATTERN      = Pattern
                                                                         .compile(
                                                                             "^select\\s+.*\\s+for\\s+update.*$",
                                                                             Pattern.CASE_INSENSITIVE);

    /**浠嶥B2鐨勭郴缁熻〃涓幏鍙杝equence鍜屼簨浠剁殑sql.  */
    private static final Pattern      SELECT_FROM_SYSTEMIBM          = Pattern
                                                                         .compile(
                                                                             "^select\\s+.*\\s+from\\s+sysibm.*$",
                                                                             Pattern.CASE_INSENSITIVE);
    private static final Pattern      SELECT_FROM_DUAL_PATTERN       = Pattern
                                                                         .compile(
                                                                             "^select\\s+.*\\s+from\\s+dual.*$",
                                                                             Pattern.CASE_INSENSITIVE);

    /**
     * 榛樿鐨勬瘡涓〃鎵цsql鐨勮秴鏃舵椂闂�
     */
    public static final long          DEFAULT_TIMEOUT_FOR_EACH_TABLE = 100;

    private static final ParserCache  globalCache                    = ParserCache.instance();

    protected Map<String, DBSelector> dbSelectors;
    protected DBSelector              groupDBSelector                = null;
    protected RuleController          ruleController;
    protected final SqlDispatcher     writeDispatcher;
    protected final SqlDispatcher     readDispatcher;

    //璁板綍褰撳墠鐨勬搷浣滄槸鍐欏簱鎿嶄綔杩樻槸璇诲簱鎿嶄綔
    protected DB_OPERATION_TYPE       operation_type;

    public enum DB_OPERATION_TYPE {
        WRITE_INTO_DB, READ_FROM_DB;
    }

    private ZdalConnection         connectionProxy;

    protected List<Statement>      actualStatements     = new ArrayList<Statement>();
    protected ResultSet            results;
    protected boolean              moreResults;
    protected int                  updateCount;
    protected boolean              closed;
    /*
     *  鏄惁鏇挎崲hint涓殑閫昏緫琛ㄥ悕锛岄粯璁ゆ槸涓嶆浛鎹�
     */
    private boolean                isHintReplaceSupport = false;
    /**
     * 閲嶈瘯娆℃暟锛屽閮ㄦ寚瀹�
     */
    protected int                  retryingTimes;
    /**
     * fetchsize 榛樿涓�10 
     */
    private int                    fetchSize            = 10;

    private int                    resultSetType        = -1;
    private int                    resultSetConcurrency = -1;
    private int                    resultSetHoldability = -1;

    protected boolean              autoCommit           = true;

    /**
     * 缂撳瓨鐨刡atch鎿嶄綔鐨刣bId
     */
    private String                 batchDataBaseId      = null;

    private boolean                readOnly;

    /**
     * 灏嗗師鏉esultSet鎺ュ彛涓嬫斁鍒癉ummy绾у埆銆傝繖鏍锋墠鑳芥敮鎸佽嚜瀹氫箟鏂规硶
     */
    protected Set<ResultSet>       openResultSets       = new HashSet<ResultSet>();

    protected List<Object>         batchedArgs;

    private long                   timeoutForEachTable  = DEFAULT_TIMEOUT_FOR_EACH_TABLE;

    protected DataSourceConfigType dbConfigType         = null;

    private int                    autoGeneratedKeys;
    private int[]                  columnIndexes;
    private String[]               columnNames;

    /**鏁版嵁婧愮殑鍚嶇О.  */
    protected String               appDsName            = null;

    protected static void dumpSql(String originalSql, Map<String, SqlAndTable[]> targets) {
        dumpSql(originalSql, targets, null);
    }

    public ZdalStatement(SqlDispatcher writeDispatcher, SqlDispatcher readDispatcher) {
        this.writeDispatcher = writeDispatcher;
        this.readDispatcher = readDispatcher;
    }

    protected static void dumpSql(String originalSql, Map<String, SqlAndTable[]> targets,
                                  Map<Integer, ParameterContext> parameters) {
        if (sqlLog.isDebugEnabled()) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("\n[original sql]:").append(originalSql.trim()).append("\n");
            for (Entry<String, SqlAndTable[]> entry : targets.entrySet()) {
                for (SqlAndTable targetSql : entry.getValue()) {
                    buffer.append(" [").append(entry.getKey()).append(".").append(targetSql.table)
                        .append("]:").append(targetSql.sql.trim()).append("\n");
                }
            }

            if (parameters != null && !parameters.isEmpty() && !parameters.values().isEmpty()) {
                buffer.append("[parameters]:").append(parameters.values().toString());
            }

            sqlLog.debug(buffer.toString());
        }
    }

    /**
     * 鑾峰緱SQL璇彞绉嶇被
     *
     * @param sql SQL璇彞
     * @throws SQLException 褰揝QL璇彞涓嶆槸SELECT銆両NSERT銆乁PDATE銆丏ELETE璇彞鏃讹紝鎶涘嚭寮傚父銆�
     */
    protected static SqlType getSqlType(String sql) throws SQLException {
        SqlType sqlType = globalCache.getSqlType(sql);
        if (sqlType == null) {
            String noCommentsSql = StringUtil.stripComments(sql, "'\"", "'\"", true, false, true,
                true).trim();

            if (StringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "select")) {
                if (SELECT_FROM_DUAL_PATTERN.matcher(noCommentsSql).matches()) {
                    sqlType = SqlType.SELECT_FROM_DUAL;
                } else if (SELECT_FOR_UPDATE_PATTERN.matcher(noCommentsSql).matches()) {
                    sqlType = SqlType.SELECT_FOR_UPDATE;
                } else if (SELECT_FROM_SYSTEMIBM.matcher(noCommentsSql).matches()) {
                    sqlType = SqlType.SELECT_FROM_SYSTEMIBM;
                } else {
                    sqlType = SqlType.SELECT;
                }
            } else if (StringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "insert")) {
                sqlType = SqlType.INSERT;
            } else if (StringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "update")) {
                sqlType = SqlType.UPDATE;
            } else if (StringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "delete")) {
                sqlType = SqlType.DELETE;
            } else {
                throw new SQLException("only select, insert, update, delete sql is supported");
            }
            sqlType = globalCache.setSqlTypeIfAbsent(sql, sqlType);

        }
        return sqlType;
    }

    /**
     * 鏇挎崲SQL璇彞涓櫄鎷熻〃鍚嶄负瀹為檯琛ㄥ悕銆� 浼� 鏇挎崲_tableName$ 鏇挎崲_tableName_ 鏇挎崲tableName.
     * 鏇挎崲tableName(
     * 澧炲姞鏇挎崲 _tableName, ,tableName, ,tableName_
     * 
     * @param originalSql
     *            SQL璇彞
     * @param virtualName
     *            铏氭嫙琛ㄥ悕
     * @param actualName
     *            瀹為檯琛ㄥ悕
     * @return 杩斿洖鏇挎崲鍚庣殑SQL璇彞銆�
     */
    protected String replaceTableName(String originalSql, String virtualName, String actualName) {
        if (log.isDebugEnabled()) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("virtualName = ").append(virtualName).append(", ");
            buffer.append("actualName = ").append(actualName);
            log.debug(buffer.toString());
        }

        if (virtualName.equalsIgnoreCase(actualName)) {
            return originalSql;
        }
        //add by boya for testcase for schemaname.tablename to ignore replaceTablename.
        if (virtualName.contains(actualName)) {
            return originalSql;
        }

        List<String> sqlPieces = globalCache.getTableNameReplacement(originalSql);
        if (sqlPieces == null) {
            //鏇挎崲   tableName$ 
            Pattern pattern1 = Pattern.compile(new StringBuilder("\\s").append(virtualName).append(
                "$").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces1 = new ArrayList<String>();
            Matcher matcher1 = pattern1.matcher(originalSql);
            int start1 = 0;
            while (matcher1.find(start1)) {
                pieces1.add(originalSql.substring(start1, matcher1.start() + 1));
                start1 = matcher1.end();
            }
            pieces1.add(originalSql.substring(start1));
            //鏇挎崲   tableName  
            Pattern pattern2 = Pattern.compile(new StringBuilder("\\s").append(virtualName).append(
                "\\s").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces2 = new ArrayList<String>();
            for (String piece : pieces1) {
                Matcher matcher2 = pattern2.matcher(piece);
                int start2 = 0;
                while (matcher2.find(start2)) {
                    pieces2.add(piece.substring(start2 - 1 < 0 ? 0 : start2 - 1,
                        matcher2.start() + 1));
                    start2 = matcher2.end();
                }
                pieces2.add(piece.substring(start2 - 1 < 0 ? 0 : start2 - 1));
            }
            //鏇挎崲   tableName. 
            Pattern pattern3 = Pattern.compile(new StringBuilder().append(virtualName)
                .append("\\.").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces3 = new ArrayList<String>();
            for (String piece : pieces2) {
                Matcher matcher3 = pattern3.matcher(piece);
                int start3 = 0;
                while (matcher3.find(start3)) {
                    pieces3.add(piece.substring(start3 - 1 < 0 ? 0 : start3 - 1, matcher3.start()));
                    start3 = matcher3.end();
                }
                pieces3.add(piece.substring(start3 - 1 < 0 ? 0 : start3 - 1));
            }
            //鏇挎崲  tablename(
            Pattern pattern4 = Pattern.compile(new StringBuilder("\\s").append(virtualName).append(
                "\\(").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces4 = new ArrayList<String>();
            for (String piece : pieces3) {
                Matcher matcher4 = pattern4.matcher(piece);
                int start4 = 0;
                while (matcher4.find(start4)) {
                    pieces4.add(piece.substring(start4 - 1 < 0 ? 0 : start4 - 1,
                        matcher4.start() + 1));
                    start4 = matcher4.end();
                }
                pieces4.add(piece.substring(start4 - 1 < 0 ? 0 : start4 - 1));
            }

            //鏇挎崲_tableName,
            Pattern pattern5 = Pattern.compile(new StringBuilder("\\s").append(virtualName).append(
                "\\,").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces5 = new ArrayList<String>();
            for (String piece : pieces4) {
                Matcher matcher5 = pattern5.matcher(piece);
                int start5 = 0;
                while (matcher5.find(start5)) {
                    pieces5.add(piece.substring(start5 - 1 < 0 ? 0 : start5 - 1,
                        matcher5.start() + 1));
                    start5 = matcher5.end();
                }
                pieces5.add(piece.substring(start5 - 1 < 0 ? 0 : start5 - 1));
            }

            //鏇挎崲,tableName
            Pattern pattern6 = Pattern.compile(new StringBuilder("\\,").append(virtualName).append(
                "\\s").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces6 = new ArrayList<String>();
            for (String piece : pieces5) {
                Matcher matcher6 = pattern6.matcher(piece);
                int start6 = 0;
                while (matcher6.find(start6)) {
                    pieces6.add(piece.substring(start6 - 1 < 0 ? 0 : start6 - 1,
                        matcher6.start() + 1));
                    start6 = matcher6.end();
                }
                pieces6.add(piece.substring(start6 - 1 < 0 ? 0 : start6 - 1));
            }
            //鏇挎崲 ,tableName,
            Pattern pattern7 = Pattern.compile(new StringBuilder("\\,").append(virtualName).append(
                "\\,").toString(), Pattern.CASE_INSENSITIVE);
            List<String> pieces7 = new ArrayList<String>();
            for (String piece : pieces6) {
                Matcher matcher7 = pattern7.matcher(piece);
                int start7 = 0;
                while (matcher7.find(start7)) {
                    pieces7.add(piece.substring(start7 - 1 < 0 ? 0 : start7 - 1,
                        matcher7.start() + 1));
                    start7 = matcher7.end();
                }
                pieces7.add(piece.substring(start7 - 1 < 0 ? 0 : start7 - 1));
            }

            sqlPieces = globalCache.setTableNameReplacementIfAbsent(originalSql, pieces7);

        }

        // 鐢熸垚鏈�缁圫QL
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (String piece : sqlPieces) {
            if (!first) {
                buffer.append(actualName);
            } else {
                first = false;
            }
            buffer.append(piece);
        }
        String sql_replace = buffer.toString();

        /*
         * added by fanzeng
         * 鏀粯瀹濋粯璁や笉鏇挎崲HINT閲岀殑琛ㄥ悕锛屽鏋滈渶瑕佹浛鎹紝鍒欏繀椤诲湪閰嶇疆鏂囦欢涓寚瀹�
         * <property name="isHintReplaceSupport" value="true"/>
         * */
        if (log.isDebugEnabled()) {
            log.debug("鏄惁鏀寔鏇挎崲hint鐨勯�昏緫琛ㄥ悕锛歩sHintSupport = " + this.isHintReplaceSupport);
        }
        //鏇挎崲  hint锛屾牸寮忎笉鍐嶈繘琛岄檺鍒� 
        if (isHintReplaceSupport) {
            Pattern pattern8 = Pattern.compile(new StringBuilder("/\\s?\\*\\s?.*").append(
                virtualName).append(".*\\s?\\*\\s?/").toString(), Pattern.CASE_INSENSITIVE);
            String sql_pieces[] = new String[2];
            String hint = "";
            Matcher matcher8 = pattern8.matcher(sql_replace);

            int start8 = 0;
            if (matcher8.find(start8)) {
                sql_pieces[0] = sql_replace.substring(start8 - 1 < 0 ? 0 : start8 - 1, matcher8
                    .start());
                sql_pieces[1] = sql_replace.substring(matcher8.end());
                hint = sql_replace.substring(matcher8.start(), matcher8.end()).toUpperCase();

                hint = hint.replace(virtualName.toUpperCase(), actualName.toUpperCase());

                sql_replace = sql_pieces[0] + hint + sql_pieces[1];
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("鏇挎崲琛ㄥ悕鍚庣殑sql涓猴細" + sql_replace);
        }

        return sql_replace;
    }

    protected SqlExecutionContext getExecutionContext(String originalSql, List<Object> parameters)
                                                                                                  throws SQLException {
        SqlExecutionContext executionContext = null;
        try {
            executionContext = getExecutionContext1(originalSql, parameters); //鏂拌鍒� 
        } catch (RuleRuntimeExceptionWrapper e) {
            //鍥犱负RUleRuntimeException涔熸槸涓猂untimeException,鎵�浠ユ帓鍦ㄥ悗缁璻untimeException鍓嶉潰
            SQLException sqlException = e.getSqlException();
            if (sqlException instanceof ZdalCommunicationException) {
                //涓嶉噸澶嶇殑杩涜鍖呰锛岃繖涓紓甯告槸zdal鏌ヨ璧板垎搴撴椂锛屽垎搴撻噸璇曟鏁板埌杈句笂闄愭椂锛岃嚜宸变細鎶涘嚭鐨勩�備笟鍔￠渶瑕佽繖涓紓甯�
                throw e;
            } else {
                //瀵逛簬闈瀦dal浣滀负瑙勫垯寮曟搸涓璵apping rule 浣庡眰鏁版嵁搴撴煡璇㈢殑鍦烘櫙锛岃瀵箂qlException杩涜鍖呰鍚庢姏鍑�
                throw new ZdalCommunicationException("rule sql exceptoin.", sqlException);
            }

        } catch (ZdalRuleCalculateException e) {
            log.error("瑙勫垯寮曟搸璁＄畻閿欒锛宻ql=" + originalSql, e);
            throw e;
        } catch (RuntimeException e) {
            String context = ExceptionUtils.getErrorContext(originalSql, parameters,
                "An error occerred on  routing or getExecutionContext,sql is :");
            //log.error(context, e);
            throw new RuntimeException(context, e);
        }
        return executionContext;
    }

    /**
     * @param dbSelectorID
     * @param retringContext
     * @throws SQLException
     */
    public void createConnectionByID(String dbSelectorID) throws SQLException {
        DBSelector dbSelector = this.dbSelectors.get(dbSelectorID);
        //			retringContext.setDbSelector(dbSelector);
        createConnection(dbSelector, dbSelectorID);
    }

    /**
     * 鑾峰彇鏂扮殑Connection鍜屼粬瀵瑰簲鐨凞atasource
     * 
     * datasource涓昏鏄敤浜庡湪闅忔満閲嶈瘯鐨勬椂鍊欐帓闄ゅ凡缁忔寕鎺夌殑鏁版嵁婧�
     * 
     * 涓嶆彁渚涢噸璇�
     * @param ds
     * @return
     * @throws SQLException
     */
    private ConnectionAndDatasource getNewConnectionAndDataSource(DataSource ds,
                                                                  DBSelector dbSelector)
                                                                                        throws SQLException {
        ConnectionAndDatasource connectionAndDatasource = new ConnectionAndDatasource();
        connectionAndDatasource.parentDataSource = ds;
        connectionAndDatasource.dbSelector = dbSelector;
        long begin = System.currentTimeMillis();
        Connection conn = ds.getConnection();
        conn.setAutoCommit(autoCommit);
        long elapsed = System.currentTimeMillis() - begin;
        if (log.isDebugEnabled()) {
            log.debug("get the connection, elapsed time=" + elapsed + ",thread="
                      + Thread.currentThread().getName());
        }

        connectionAndDatasource.connection = conn;
        return connectionAndDatasource;
    }

    protected SqlDispatcher selectSqlDispatcher(boolean autoCommit, SqlType sqlType)
                                                                                    throws SQLException {
        SqlDispatcher sqlDispatcher;
        if (sqlType != SqlType.SELECT) {
            if (this.writeDispatcher == null) {
                throw new SQLException("鍒嗗簱涓嶆敮鎸佸啓鍏ャ�傝妫�鏌ラ厤缃垨SQL");
            }
            sqlDispatcher = this.writeDispatcher;
        } else {
            if (autoCommit) {
                String rc = (String) ThreadLocalMap.get(ThreadLocalString.SELECT_DATABASE);
                if (rc != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("rc=" + rc);
                    }
                    sqlDispatcher = this.writeDispatcher;
                } else {
                    sqlDispatcher = this.readDispatcher != null ? this.readDispatcher
                        : this.writeDispatcher;
                }
            } else {
                sqlDispatcher = this.writeDispatcher;
                if (sqlDispatcher == null) {
                    throw new SQLException("鍒嗗簱涓嶆敮鎸佸啓鍏ャ�傝涓嶈浣跨敤浜嬪姟");
                }
            }
        }
        if (sqlDispatcher == null) {
            throw new SQLException("娌℃湁鍒嗗簱瑙勫垯鎴杝qlDispatcher涓簄ull锛岃妫�鏌ラ厤缃垨SQL");
        }

        if (sqlDispatcher == this.writeDispatcher) {
            this.setOperation_type(DB_OPERATION_TYPE.WRITE_INTO_DB);
        } else if (sqlDispatcher == this.readDispatcher) {
            this.setOperation_type(DB_OPERATION_TYPE.READ_FROM_DB);
        } else {
            throw new SQLException("鎿嶄綔绫诲瀷鍙戠敓寮傚父锛岃秴鍑烘甯歌寖鍥达紒");
        }
        return sqlDispatcher;
    }

    /**
     * 1. 鍙敮鎸佸崟搴撶殑浜嬪姟, 鏈変簨鍔″悓鏃剁洰鏍囧簱涓哄涓椂鎶ラ敊
     * 2. 鍙敮鎸佸崟搴撶殑浜嬪姟, 鏈変簨鍔″苟涓斿綋鍓嶄簨鍔′腑宸茬粡鍏宠仈鐨勫垎搴撳拰鏈瑙ｆ瀽鐨勭洰鏍囧簱涓嶆槸鍚屼竴涓簱鏃舵姤閿�
     */
    protected SqlExecutionContext getExecutionContext1(String originalSql, List<Object> parameters)
                                                                                                   throws SQLException {
        SqlExecutionContext context = new SqlExecutionContext();

        SqlType sqlType = getSqlType(originalSql);

        RouteCondition rc = (RouteCondition) ThreadLocalMap.get(ThreadLocalString.ROUTE_CONDITION);

        ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, null);

        DispatcherResult metaData = null;
        List<TargetDB> targets = null;

        SqlDispatcher sqlDispatcher = selectSqlDispatcher(autoCommit, sqlType);

        /*
         * 鏌ョ湅sqlDispatcher鏄惁涓簑riteDispatcher.
         * 
         * writeDispatcher涓昏澶勭悊锛歩nsert ,update ,select for update,浜嬪姟涓璼elect杩�
         * 4绉嶆儏鍐碉紝閮戒笉闇�瑕佽閲嶈瘯銆�
         * 
         * 涓嶄负鐨勬儏鍐垫湁涓ょ锛岀涓�绉嶆槸鍐呭瓨涓璞＄姸鎬佷笉涓�鏍凤紝杩欑搴旇鏄痳eadDispatcher銆�
         * 璁＄畻鍑虹殑dispatcher涓嶄細涓洪櫎浜唕ead鍜寃riteDispatcher浠ュ鐨勫叾浠栨儏鍐碉紝
         * 鐢眘electSqlDispatcher鏂规硶淇濊瘉銆�
         * 
         * 杩樻湁涓�绉嶆槸writeDispatcher涓簄ull 杩欐椂鍊� 锛屽張鍥犱负璁＄畻鍑虹殑
         * dispatcher鍙彲鑳戒负 read鎴杦rite銆傛墍浠ヤ篃鍙互淇濊瘉姝ｇ‘鐨勭粨鏋溿��
         */
        boolean ruleReturnNullValue = false;
        /**
         * 璁剧疆浜哸utoCommit灞炴�у�硷紝鐢ㄤ簬鍦ㄦ牴鎹暟鎹簮key鏉冮噸閰嶇疆闅忔満閫夋嫨缁勫唴db鏃讹紝鍒ゅ畾鏄笉鏄湪浜嬪姟涓�
         * 濡傛灉鍦ㄤ簨鍔′腑锛屽氨灏嗘娆¤绠楃殑鍊肩紦瀛樿捣鏉ワ紝鐒跺悗璇ヤ簨鍔′腑鐨勫叾浠杝ql鎵ц闅忔満绠楁硶鏃讹紝鐩存帴灏嗚鍊艰繑鍥�
         * 浠ヨ揪鍒板湪涓�涓簨鍔′腑绂佹涓ゆ闅忔満鑰屾湁鍙兘閫夋嫨涓嶅悓鐨刣b
         */
        //        ThreadLocalMap.put(ThreadLocalString.GET_AUTOCOMMIT_PROPERTY, autoCommit);
        try {
            metaData = getExecutionMetaData(originalSql, parameters, rc, sqlDispatcher);
            targets = metaData.getTarget();
        } catch (EmptySetRuntimeException e) {
            ruleReturnNullValue = true;
        }

        if (targets == null || targets.isEmpty()) {
            if (!ruleReturnNullValue) {
                throw new SQLException("鎵句笉鍒扮洰鏍囧簱锛岃妫�鏌ラ厤缃�,the originalSql = " + originalSql
                                       + " ,the parameters = " + parameters);
            } else {
                //濡傛灉鏄痬apping rule鍒欏湪璁＄畻涓鏋滆繑鍥瀗ull鍒欑洿鎺ヨ繑鍥瀍mptyResultSet
                context.setRuleReturnNullValue(ruleReturnNullValue);
            }
        } else {
            buildExecutionContext(originalSql, context, sqlType, metaData, targets);
        }

        return context;
    }

    @SuppressWarnings("unchecked")
    protected final ResultSet getEmptyResultSet() {
        return new EmptySimpleTResultSet(this, Collections.EMPTY_LIST);
    }

    private void buildExecutionContext(String originalSql, SqlExecutionContext context,
                                       SqlType sqlType, DispatcherResult metaData,
                                       List<TargetDB> targets) throws SQLException {
        // 鍙敮鎸佸崟搴撶殑浜嬪姟
        if (!autoCommit && targets.size() != 1 && sqlType != SqlType.SELECT) {
            throw new SQLException("鍙敮鎸佸崟搴撶殑浜嬪姟锛歵arget.size=" + targets.size());
        }

        // 浜嬪姟鍚姩
        setTransaction(targets, originalSql);

        for (TargetDB target : targets) {

            String dbIndex = target.getDbIndex();

            Set<String> actualTables = target.getTableNames();

            if (dbIndex == null || dbIndex.length() < 1) {
                throw new SQLException("invalid dbIndex:" + dbIndex);
            }

            if (actualTables == null || actualTables.isEmpty()) {
                throw new SQLException("鎵句笉鍒扮洰鏍囪〃");
            }

            // 閲嶇敤杩炴帴
            //			杩欓噷澶勭悊鑾峰彇骞堕噸鐢ㄨ繛鎺ョ殑闂
            DBSelector dbselector = dbSelectors.get(dbIndex);
            if (dbselector == null) {
                throw new IllegalStateException("娌℃湁涓篸bIndex[" + dbIndex + "]閰嶇疆鏁版嵁婧�");
            }
            createConnection(dbselector, dbIndex); //杩欓噷濡傛灉鏄噸璇曞悗鎴愬姛锛屽悗缁噸璇曟椂娌℃湁鎺掗櫎鏈閲嶈瘯杩囩殑ds

            if (sqlType == SqlType.INSERT) {
                if (actualTables.size() != 1) {
                    if (actualTables.isEmpty()) {
                        throw new SQLException(
                            "Zdal need at least one table, but There is none selected ");
                    }

                    throw new SQLException("mapping many actual tables");
                }
            }

            if (!autoCommit && !dbIndex.equals(getConnectionProxy().getTxTarget())
                && sqlType != SqlType.SELECT) {
                throw new SQLException("zdal鍙敮鎸佸崟搴撶殑浜嬪姟锛歞bIndex=" + dbIndex + ",txTarget="
                                       + getConnectionProxy().getTxTarget() + ",originalSql="
                                       + originalSql);
            }

            SqlAndTable[] targetSqls = new SqlAndTable[actualTables.size()];
            if (!metaData.allowReverseOutput()) {
                int i = 0;
                for (String tab : actualTables) {
                    SqlAndTable sqlAndTable = new SqlAndTable();
                    targetSqls[i] = sqlAndTable;
                    sqlAndTable.sql = replaceTableName(originalSql, metaData.getVirtualTableName(),
                        tab);
                    //濡傛灉metaData(涔熷氨鏄疍ispatcherResult)閲岄潰鏈塲oin琛ㄥ悕锛岄偅涔堝氨鏇挎崲鎺�;
                    sqlAndTable.sql = replaceJoinTableName(metaData.getVirtualTableName(), metaData
                        .getVirtualJoinTableNames(), tab, sqlAndTable.sql);
                    sqlAndTable.table = tab;
                    i++;
                }
            } else {
                int i = 0;
                for (String tab : actualTables) {
                    SqlAndTable targetSql = new SqlAndTable();
                    targetSql.sql = replaceTableName(originalSql, metaData.getVirtualTableName(),
                        tab);
                    targetSql.table = tab;
                    targetSqls[i] = targetSql;
                    i++;
                }
                // 鍥犱负鎵�鏈塖QL缁戝畾鍙傛暟閮戒竴鏍凤紝鎵�浠ュ彧瑕佸彇绗竴涓��
                context.setChangedParameters(target.getChangedParams());
            }
            context.getTargetSqls().put(dbIndex, targetSqls);

        }

        context.setOrderByColumns(metaData.getOrderByMessages().getOrderbyList());
        context.setSkip(metaData.getSkip() == DefaultSqlParserResult.DEFAULT_SKIP_MAX ? 0
            : metaData.getSkip());
        context.setMax(metaData.getMax() == DefaultSqlParserResult.DEFAULT_SKIP_MAX ? -1 : metaData
            .getMax());
        context.setGroupFunctionType(metaData.getGroupFunctionType());
        context.setVirtualTableName(metaData.getVirtualTableName());
        //杩欓噷闇�瑕佹敞鎰忕殑
        // boolean needRetry = SqlType.SELECT.equals(sqlType) && this.autoCommit;
        boolean needRetry = this.autoCommit;
        //boolean isMySQL = sqlDispatcher.getDBType() == DBType.MYSQL?true:false;
        //RetringContext retringContext = new RetringContext(isMySQL);
        //retringContext.setNeedRetry(needRetry);
        //context.setRetringContext(retringContext);
        Map<DataSource, SQLException> failedDataSources = needRetry ? new LinkedHashMap<DataSource, SQLException>(
            0)
            : null;
        context.setFailedDataSources(failedDataSources);
    }

    /**
     * @param tab 瀹為檯琛ㄥ悕
     * @param vtab 铏氭嫙琛ㄥ悕
     * @return
     */
    private String getSuffix(String tab, String vtab) {
        String result = tab.substring(vtab.length());
        return result;
    }

    /**
     * 鑾峰彇sql鎵ц淇℃伅
     * @param originalSql
     * @param parameters
     * @param rc
     * @param metaData
     * @param sqlDispatcher
     * @return
     * @throws SQLException
     */
    protected DispatcherResult getExecutionMetaData(String originalSql, List<Object> parameters,
                                                    RouteCondition rc, SqlDispatcher sqlDispatcher)
                                                                                                   throws SQLException {
        DispatcherResult metaData;
        if (rc != null) {
            //涓嶈蛋瑙ｆ瀽SQL锛岀敱ThreadLocal浼犲叆鐨勬寚瀹氬璞★紙RouteCondition锛夛紝鍐冲畾搴撹〃鐩殑鍦�
            metaData = sqlDispatcher.getDBAndTables(rc);
        } else {
            // 閫氳繃瑙ｆ瀽SQL鏉ュ垎搴撳垎琛�
            try {
                metaData = sqlDispatcher.getDBAndTables(originalSql, parameters);
            } catch (ZdalCheckedExcption e) {
                throw new SQLException(e.getMessage());
            }
        }
        return metaData;
    }

    /**
     * 浜嬪姟鍚姩
     * @param targets
     */
    private void setTransaction(List<TargetDB> targets, String originalSql) {
        if (!autoCommit && getConnectionProxy().getTxStart()) {
            getConnectionProxy().setTxStart(false);
            //getConnectionProxy().setTxTarget(targets.get(0).getWritePool()[0]);
            getConnectionProxy().setTxTarget(targets.get(0).getDbIndex());
            if (log.isDebugEnabled()) {
                log.debug("缂撳瓨浜嬪姟鏁版嵁搴撴爣璇�:Set the txStart property to false, and the dbIndex="
                          + targets.get(0).getDbIndex() + ",originalSql=" + originalSql);
            }
        }
    }

    private final DataSourceTryer<ConnectionAndDatasource> createConnectionTryer = new AbstractDataSourceTryer<ConnectionAndDatasource>() {
                                                                                     public ConnectionAndDatasource tryOnDataSource(
                                                                                                                                    DataSource ds,
                                                                                                                                    String name,
                                                                                                                                    Object... args)
                                                                                                                                                   throws SQLException {
                                                                                         DBSelector dbSelector = (DBSelector) args[0];
                                                                                         dbSelector
                                                                                             .setSelectedDSName(name);
                                                                                         return getNewConnectionAndDataSource(
                                                                                             ds,
                                                                                             dbSelector);
                                                                                     }

                                                                                     @Override
                                                                                     public ConnectionAndDatasource onSQLException(
                                                                                                                                   java.util.List<SQLException> exceptions,
                                                                                                                                   ExceptionSorter exceptionSorter,
                                                                                                                                   Object... args)
                                                                                                                                                  throws SQLException {
                                                                                         int size = exceptions
                                                                                             .size();
                                                                                         if (size <= 0) {
                                                                                             throw new IllegalArgumentException(
                                                                                                 "should not be here");
                                                                                         } else {
                                                                                             //姝ｅ父鎯呭喌涓嬬殑澶勭悊
                                                                                             int lastElementIndex = size - 1;
                                                                                             //鍙栨渶鍚庝竴涓猠xception.鍒ゆ柇鏄惁鏄暟鎹簱涓嶅彲鐢ㄥ紓甯�.濡傛灉鏄紝鍘绘帀鏈�鍚庝竴涓紓甯革紝骞跺皢澶村紓甯稿寘瑁呬负ZdalCommunicationException鎶涘嚭
                                                                                             SQLException lastSQLException = exceptions
                                                                                                 .get(lastElementIndex);
                                                                                             if (exceptionSorter
                                                                                                 .isExceptionFatal(lastSQLException)) {
                                                                                                 exceptions
                                                                                                     .remove(lastElementIndex);
                                                                                                 exceptions
                                                                                                     .add(
                                                                                                         0,
                                                                                                         new ZdalCommunicationException(
                                                                                                             "zdal communicationException ",
                                                                                                             lastSQLException));
                                                                                             }
                                                                                         }
                                                                                         return super
                                                                                             .onSQLException(
                                                                                                 exceptions,
                                                                                                 exceptionSorter,
                                                                                                 args);
                                                                                     };
                                                                                 };

    /**
     * 濡傛灉閲嶇敤杩炴帴 鍒欓噸鏂拌缃產utoCommit鐒跺悗鎺ㄩ�併��
     * 
     * 濡傛灉涓嶉噸鐢ㄨ繛鎺ワ紝鍒欎粠Datasource閲岄潰閫夋嫨涓�涓狣atasource鍚庡缓绔嬭繛鎺�
     * 
     * 鐒跺悗灏嗛摼鎺ユ斁鍏arentConnection鐨勫彲閲嶇敤杩炴帴map閲�(getConnectionProxy.getAuctualConnections)
     * @param dbIndex
     * @return 
     * @throws SQLException
     */
    protected void createConnection(DBSelector dbSelector, String dbIndex) throws SQLException {
        this.createConnection(dbSelector, dbIndex, new LinkedHashMap<DataSource, SQLException>(0));
    }

    protected void createConnection(DBSelector dbSelector, String dbIndex,
                                    Map<DataSource, SQLException> failedDataSources)
                                                                                    throws SQLException {
        //		Map<String, ConnectionAndDatasource> connections = getConnectionProxy().getActualConnections();
        ConnectionAndDatasource connectionAndDatasource = getConnectionProxy()
            .getConnectionAndDatasourceByDBSelectorID(dbIndex);

        //DataSource datasource = null;
        //datasource = dbSelector.select();
        if (connectionAndDatasource == null) {
            if (dbIndex == null) {
                throw new SQLException(new StringBuilder("data source ").append(dbIndex).append(
                    " does not exist").toString());
            }
            //娌℃湁connection
            //try {
            //connectionAndDatasource = getNewConnectionAndDataSource(datasource,dbSelector);
            connectionAndDatasource = dbSelector.tryExecute(failedDataSources,
                createConnectionTryer, retryingTimes, operation_type, dbSelector);
            getConnectionProxy().put(dbIndex, connectionAndDatasource);
            //} catch (NullPointerException e) {
            //	throw new SQLException(new StringBuilder("data source ").append(dbIndex).append(" does not exist")
            //			.toString());
            //}catch (SQLException e) {
            //	throw new RetrySQLException(e, datasource);
            //}
        } else {
            //鍙噸鐢╟onnection
            //datasource = connectionAndDatasource.parentDataSource;
            try {
                connectionAndDatasource.connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                //throw new RetrySQLException(e, datasource);
                failedDataSources.put(connectionAndDatasource.parentDataSource, e);
                getConnectionProxy().removeConnectionAndDatasourceByID(dbIndex);
                createConnection(dbSelector, dbIndex, failedDataSources);
            }

        }
    }

    /**
     * 鐢ㄥ綋鍓嶈繛鎺ュ缓绔媠tatementd
     * 
     * @param connection 褰撳墠姝ｅ湪鐢ㄧ殑connection,鏈潵鍙互浠巑ap涓彇浣嗗洜涓烘晥鐜囦笂鐨勮�冭檻鎵�浠ヨ繕鏄笉杩欐牱鍋�
     * @param connections 
     * @param dbIndex
     * @param retringContext
     * @return
     * @throws SQLException
     */
    protected Statement createStatementInternal(Connection connection, String dbIndex,
                                                Map<DataSource, SQLException> failedDataSources)
                                                                                                throws SQLException {
        Statement stmt;
        if (this.resultSetType != -1 && this.resultSetConcurrency != -1
            && this.resultSetHoldability != -1) {
            stmt = connection.createStatement(this.resultSetType, this.resultSetConcurrency,
                this.resultSetHoldability);
        } else if (this.resultSetType != -1 && this.resultSetConcurrency != -1) {
            stmt = connection.createStatement(this.resultSetType, this.resultSetConcurrency);
        } else {
            stmt = connection.createStatement();
        }

        return stmt;
    }

    public Connection getConnection() throws SQLException {
        return connectionProxy;
    }

    private boolean executeInternal(String sql, int autoGeneratedKeys, int[] columnIndexes,
                                    String[] columnNames) throws SQLException {
        SqlType sqlType = getSqlType(sql);
        if (sqlType == SqlType.SELECT || sqlType == SqlType.SELECT_FROM_DUAL
            || sqlType == SqlType.SELECT_FOR_UPDATE) {
            if (this.dbConfigType == DataSourceConfigType.GROUP) {
                executeQuery0(sql, sqlType);
            } else {
                executeQuery(sql);
            }
            return true;
        } else if (sqlType == SqlType.INSERT || sqlType == SqlType.UPDATE
                   || sqlType == SqlType.DELETE) {
            if (autoGeneratedKeys == -1 && columnIndexes == null && columnNames == null) {
                executeUpdate(sql);
            } else if (autoGeneratedKeys != -1) {
                executeUpdate(sql, autoGeneratedKeys);
            } else if (columnIndexes != null) {
                executeUpdate(sql, columnIndexes);
            } else if (columnNames != null) {
                executeUpdate(sql, columnNames);
            } else {
                executeUpdate(sql);
            }

            return false;
        } else {
            throw new SQLException("only select, insert, update, delete sql is supported");
        }
    }

    public boolean execute(String sql) throws SQLException {
        return executeInternal(sql, -1, null, null);
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return executeInternal(sql, autoGeneratedKeys, null, null);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return executeInternal(sql, -1, columnIndexes, null);
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return executeInternal(sql, -1, null, columnNames);
    }

    private ResultSet executeQuery0(String sql, SqlType sqlType) throws SQLException {
        checkClosed();
        this.setOperation_type(DB_OPERATION_TYPE.READ_FROM_DB);
        //鑾峰彇杩炴帴
        DBSelector dbselector = getGroupDBSelector(sqlType);
        if (dbselector == null) {
            throw new IllegalStateException("load balance鏁版嵁婧愰厤缃被鍨嬮敊璇�");
        }

        //杩斿洖鎵ц缁撴灉
        return dbselector.tryExecute(new LinkedHashMap<DataSource, SQLException>(0),
            this.executeQueryTryer, retryingTimes, operation_type, sql, sqlType);
    }

    protected DataSourceTryer<ResultSet> executeQueryTryer = new AbstractDataSourceTryer<ResultSet>() {
                                                               public ResultSet tryOnDataSource(
                                                                                                DataSource ds,
                                                                                                String name,
                                                                                                Object... args)
                                                                                                               throws SQLException {
                                                                   String sql = (String) args[0];
                                                                   SqlType sqlType = (SqlType) args[1];
                                                                   //鑾峰彇杩炴帴
                                                                   Connection conn = ZdalStatement.this
                                                                       .getGroupConnection(ds,
                                                                           sqlType, name);
                                                                   return ZdalStatement.this
                                                                       .executeQueryOnConnection(
                                                                           conn, sql);
                                                               }

                                                           };

    private ResultSet executeQueryOnConnection(Connection conn, String sql) throws SQLException {
        Statement stmt = createStatementInternal(conn, null, null);
        actualStatements.add(stmt);
        List<ResultSet> actualResultSets = new ArrayList<ResultSet>();

        actualResultSets.add(stmt.executeQuery(sql));

        DummyTResultSet currentResultSet = new SimpleTResultSet(this, actualResultSets);

        openResultSets.add(currentResultSet);

        this.results = currentResultSet;
        this.moreResults = false;
        this.updateCount = -1;

        return this.results;

    }

    /**
     * 濡傛灉缂撳瓨鐨勮繛鎺ヤ笉绌猴紝鐩存帴杩斿洖璇ヨ繛鎺ワ紝鍚﹀垯鍒涘缓鏂拌繛鎺�
     * @param ds
     * @return
     * @throws SQLException
     */
    public Connection getGroupConnection(DataSource ds, SqlType sqlType, String name)
                                                                                     throws SQLException {
        Connection conn = null;
        //閬垮厤鍦ㄤ簨鍔′腑澶氭select鏃讹紝杩炴帴鏃犳硶閲婃斁锛屾墍浠ュ叕鐢ㄥ悓涓�涓猻elect鐨勮繛鎺�.
        if (this.getConnectionProxy().get(name) != null) {
            conn = this.getConnectionProxy().get(name).connection;
        } else {
            ConnectionAndDatasource connectionAndDatasource = new ConnectionAndDatasource();
            connectionAndDatasource.parentDataSource = ds;
            connectionAndDatasource.dbSelector = null;
            conn = ds.getConnection();
            connectionAndDatasource.connection = conn;
            this.getConnectionProxy().put(name, connectionAndDatasource);
        }
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    /**
     * 璇bSelector鏄loadbalance鐨勯厤缃皟鐢�
     * @return
     */
    public DBSelector getGroupDBSelector(SqlType sqlType) {
        DBSelector rGroupDBSelector = null, wGroupDBSelector = null;
        for (Map.Entry<String, DBSelector> dbSelector : dbSelectors.entrySet()) {
            if (dbSelector.getKey().endsWith(AppRule.DBINDEX_SUFFIX_READ)
                && dbSelector.getValue() != null) {
                rGroupDBSelector = dbSelector.getValue();
            } else if (dbSelector.getKey().endsWith(AppRule.DBINDEX_SUFFIX_WRITE)
                       && dbSelector.getValue() != null) {
                wGroupDBSelector = dbSelector.getValue();
            } else {
                throw new IllegalArgumentException("The dbSelector set error!");
            }
        }
        //濡傛灉鏄簨鍔★紝鍒欑洿鎺ュ埌鍐欏簱
        if (sqlType != SqlType.SELECT) {
            return wGroupDBSelector;
        } else if (!autoCommit) {
            return wGroupDBSelector;
        } else {
            return rGroupDBSelector;
        }
    }

    /**
     * 璇bSelector鏄loadbalance鐨勯厤缃皟鐢�
     * @return
     */
    public String getGroupDBSelectorID(SqlType sqlType) {
        String rGroupDBSelectorID = null, wGroupDBSelectorID = null;
        for (Map.Entry<String, DBSelector> dbSelector : dbSelectors.entrySet()) {
            if (dbSelector.getKey().endsWith(AppRule.DBINDEX_SUFFIX_READ)
                && dbSelector.getValue() != null) {
                rGroupDBSelectorID = dbSelector.getKey();
            } else if (dbSelector.getKey().endsWith(AppRule.DBINDEX_SUFFIX_WRITE)
                       && dbSelector.getValue() != null) {
                wGroupDBSelectorID = dbSelector.getKey();
            } else {
                throw new IllegalArgumentException("The dbSelector set error!");
            }
        }
        if (sqlType != SqlType.SELECT) {
            return wGroupDBSelectorID;
        } else {
            return rGroupDBSelectorID;
        }
    }

    public ResultSet executeQuery(String sql) throws SQLException {

        if (this.dbConfigType == DataSourceConfigType.GROUP) {
            SqlType sqlType = getSqlType(sql);
            return this.executeQuery0(sql, sqlType);
        }

        checkClosed();

        SqlExecutionContext context = getExecutionContext(sql, null);
        /*
         * modified by shenxun:
         * 杩欓噷涓昏鏄鐞唌appingRule杩斿洖绌虹殑鎯呭喌涓嬶紝搴旇杩斿洖绌虹粨鏋滈泦
         */
        if (context.mappingRuleReturnNullValue()) {
            ResultSet emptyResultSet = getEmptyResultSet();
            this.results = emptyResultSet;
            return emptyResultSet;
        }

        //        int tablesSize = 0;
        dumpSql(sql, context.getTargetSqls());

        List<SQLException> exceptions = null;
        List<ResultSet> actualResultSets = new ArrayList<ResultSet>();
        // int databaseSize = context.getTargetSqls().size();
        for (Entry<String/*dbSelectorID*/, SqlAndTable[]> entry : context.getTargetSqls()
            .entrySet()) {
            for (SqlAndTable targetSql : entry.getValue()) {
                //                tablesSize++;
                try {
                    //RetringContext retringContext = context.getRetringContext();
                    String dbSelectorId = entry.getKey();
                    Statement stmt = createStatementByDataSourceSelectorID(dbSelectorId, context
                        .getFailedDataSources());
                    //閾炬帴閲嶇敤
                    actualStatements.add(stmt);

                    queryAndAddResultToCollection(dbSelectorId, actualResultSets, targetSql, stmt,
                        context.getFailedDataSources());

                } catch (SQLException e) {

                    //寮傚父澶勭悊
                    if (exceptions == null) {
                        exceptions = new ArrayList<SQLException>();
                    }
                    exceptions.add(e);
                    ExceptionUtils.throwSQLException(exceptions, sql, Collections.emptyList()); //鐩存帴鎶涘嚭
                }
            }
        }

        ExceptionUtils.throwSQLException(exceptions, sql, Collections.emptyList());

        DummyTResultSet resultSet = mergeResultSets(context, actualResultSets);
        openResultSets.add(resultSet);

        this.results = resultSet;
        this.moreResults = false;
        this.updateCount = -1;

        return this.results;
    }

    protected void queryAndAddResultToCollection(String dbSelectorId,
                                                 List<ResultSet> actualResultSets,
                                                 SqlAndTable targetSql, Statement stmt,
                                                 Map<DataSource, SQLException> failedDataSources)
                                                                                                 throws SQLException {
        //added by fanzeng.
        //鏍规嵁dbSelectorId鑾峰彇瀵瑰簲鐨勬暟鎹簮鐨勬爣璇嗙浠ュ強鏁版嵁婧愶紝鐒跺悗鏀惧埌threadlocal閲�
        try {
            actualResultSets.add(stmt.executeQuery(targetSql.sql));
        } finally {
            Map<String, DataSource> map = getActualIdAndDataSource(dbSelectorId);
            ThreadLocalMap.put(ThreadLocalString.GET_ID_AND_DATABASE, map);
        }
    }

    protected Connection getActualConnection(String key) {
        ConnectionAndDatasource connectionAndDatasource = getConnectionProxy()
            .getConnectionAndDatasourceByDBSelectorID(key);
        Connection conn = connectionAndDatasource.connection;
        return conn;
    }

    // 鑾峰彇鐪熸鐨� 鏁版嵁婧愮殑鏍囪瘑浠ュ強鏁版嵁婧�
    protected Map<String, DataSource> getActualIdAndDataSource(String key) {
        ConnectionAndDatasource connectionAndDatasource = getConnectionProxy()
            .getConnectionAndDatasourceByDBSelectorID(key);
        Map<String, DataSource> map = new HashMap<String, DataSource>();
        if (connectionAndDatasource != null) {
            DBSelector dbSelector = connectionAndDatasource.dbSelector;
            DataSource ds = connectionAndDatasource.parentDataSource;
            if (dbSelector == null || ds == null) {
                throw new IllegalArgumentException("鏁版嵁婧愪笉鑳戒负绌猴紝璇锋鏌ワ紒");
            }
            //鍔犱笂鏁版嵁婧愬悕绉板墠缂�锛宐y鍐伴瓊 20130903
            map.put(appDsName + "." + dbSelector.getSelectedDSName(), ds);
        }
        return map;
    }

    protected DummyTResultSet mergeResultSets(SqlExecutionContext context,
                                              List<ResultSet> actualResultSets) throws SQLException {
        if (context.getOrderByColumns() != null && !context.getOrderByColumns().isEmpty()
            && context.getGroupFunctionType() != GroupFunctionType.NORMAL) {
            throw new SQLException("'group function' and 'order by' can't be together!");
        }
        if (context.getGroupFunctionType() == GroupFunctionType.AVG) {
            throw new SQLException("The group function 'AVG' is not supported now!");
        } else if (context.getGroupFunctionType() == GroupFunctionType.COUNT) {
            return new CountTResultSet(this, actualResultSets);
        } else if (context.getGroupFunctionType() == GroupFunctionType.MAX) {
            return new MaxTResultSet(this, actualResultSets);
        } else if (context.getGroupFunctionType() == GroupFunctionType.MIN) {
            return new MinTResultSet(this, actualResultSets);
        } else if (context.getGroupFunctionType() == GroupFunctionType.SUM) {
            return new SumTResultSet(this, actualResultSets);
        } else if (context.getOrderByColumns() != null && !context.getOrderByColumns().isEmpty()) {
            OrderByColumn[] orderByColumns = new OrderByColumn[context.getOrderByColumns().size()];
            int i = 0;
            for (OrderByEle element : context.getOrderByColumns()) {
                orderByColumns[i] = new OrderByColumn();
                orderByColumns[i].setColumnName(element.getName());
                orderByColumns[i++].setAsc(element.isASC());
            }
            OrderByTResultSet orderByTResultSet = new OrderByTResultSet(this, actualResultSets);
            orderByTResultSet.setOrderByColumns(orderByColumns);
            orderByTResultSet.setLimitFrom(context.getSkip());
            orderByTResultSet.setLimitTo(context.getMax());
            return orderByTResultSet;
        } else {
            SimpleTResultSet simpleTResultSet = new SimpleTResultSet(this, actualResultSets);
            simpleTResultSet.setLimitFrom(context.getSkip());
            simpleTResultSet.setLimitTo(context.getMax());
            return simpleTResultSet;
        }
    }

    protected Statement createStatementByDataSourceSelectorID(
                                                              String id,
                                                              Map<DataSource, SQLException> failedDataSources)
                                                                                                              throws SQLException {

        Connection connection = getActualConnection(id);
        Statement stmt = createStatementInternal(connection, id, failedDataSources);
        return stmt;
    }

    private int executeUpdateInternal0(String sql, int autoGeneratedKeys, int[] columnIndexes,
                                       String[] columnNames) throws SQLException {
        checkClosed();
        //鑾峰彇鏁版嵁婧�
        this.setOperation_type(DB_OPERATION_TYPE.WRITE_INTO_DB);
        //鑾峰彇杩炴帴
        DBSelector dbselector = getGroupDBSelector(SqlType.DEFAULT_SQL_TYPE);
        if (dbselector == null) {
            throw new IllegalStateException("load balance鏁版嵁婧愰厤缃被鍨嬮敊璇�");
        }
        this.autoGeneratedKeys = autoGeneratedKeys;
        this.columnIndexes = columnIndexes;
        this.columnNames = columnNames;
        boolean needRetry = this.autoCommit;
        Map<DataSource, SQLException> failedDataSources = needRetry ? new LinkedHashMap<DataSource, SQLException>(
            0)
            : null;
        //杩斿洖鎵ц缁撴灉
        return dbselector.tryExecute(failedDataSources, this.executeUpdateTryer, retryingTimes,
            operation_type, sql, SqlType.DEFAULT_SQL_TYPE);
    }

    private DataSourceTryer<Integer> executeUpdateTryer = new AbstractDataSourceTryer<Integer>() {
                                                            public Integer tryOnDataSource(
                                                                                           DataSource ds,
                                                                                           String name,
                                                                                           Object... args)
                                                                                                          throws SQLException {
                                                                SqlType sqlType = (SqlType) args[1];
                                                                //鑾峰彇杩炴帴
                                                                Connection conn = ZdalStatement.this
                                                                    .getGroupConnection(ds,
                                                                        sqlType, name);
                                                                return ZdalStatement.this
                                                                    .executeUpdateOnConnection(
                                                                        conn, args);
                                                            }
                                                        };

    private int executeUpdateOnConnection(Connection conn, Object... args) throws SQLException {
        Statement stmt = createStatementInternal(conn, null, null);
        String sql = (String) args[0];
        int affectedRows = 0;
        if (autoGeneratedKeys == -1 && columnIndexes == null && columnNames == null) {
            affectedRows += stmt.executeUpdate(sql);
        } else if (autoGeneratedKeys != -1) {
            affectedRows += stmt.executeUpdate(sql, autoGeneratedKeys);
        } else if (columnIndexes != null) {
            affectedRows += stmt.executeUpdate(sql, columnIndexes);
        } else if (columnNames != null) {
            affectedRows += stmt.executeUpdate(sql, columnNames);
        } else {
            affectedRows += stmt.executeUpdate(sql);
        }
        return affectedRows;
    }

    private int executeUpdateInternal(String sql, int autoGeneratedKeys, int[] columnIndexes,
                                      String[] columnNames) throws SQLException {
        checkClosed();

        SqlExecutionContext context = getExecutionContext(sql, null);

        if (context.mappingRuleReturnNullValue()) {
            return 0;
        }

        dumpSql(sql, context.getTargetSqls());

        int affectedRows = 0;
        List<SQLException> exceptions = null;

        for (Entry<String, SqlAndTable[]> entry : context.getTargetSqls().entrySet()) {
            for (SqlAndTable targetSql : entry.getValue()) {
                //                tablesSize++;
                try {
                    String dbSelectorId = entry.getKey();
                    Statement stmt = createStatementByDataSourceSelectorID(dbSelectorId, context
                        .getFailedDataSources());
                    actualStatements.add(stmt);
                    //added by fanzeng.
                    //鏍规嵁dbSelectorId鑾峰彇瀵瑰簲鐨勬暟鎹簮鐨勬爣璇嗙浠ュ強鏁版嵁婧愶紝鐒跺悗鏀惧埌threadlocal閲�
                    Map<String, DataSource> map = getActualIdAndDataSource(dbSelectorId);
                    ThreadLocalMap.put(ThreadLocalString.GET_ID_AND_DATABASE, map);
                    if (autoGeneratedKeys == -1 && columnIndexes == null && columnNames == null) {
                        affectedRows += stmt.executeUpdate(targetSql.sql);
                    } else if (autoGeneratedKeys != -1) {
                        affectedRows += stmt.executeUpdate(targetSql.sql, autoGeneratedKeys);
                    } else if (columnIndexes != null) {
                        affectedRows += stmt.executeUpdate(targetSql.sql, columnIndexes);
                    } else if (columnNames != null) {
                        affectedRows += stmt.executeUpdate(targetSql.sql, columnNames);
                    } else {
                        affectedRows += stmt.executeUpdate(targetSql.sql);
                    }

                } catch (SQLException e) {
                    if (exceptions == null) {
                        exceptions = new ArrayList<SQLException>();
                    }
                    exceptions.add(e);
                }
            }
        }

        this.results = null;
        this.moreResults = false;
        this.updateCount = affectedRows;

        ExceptionUtils.throwSQLException(exceptions, sql, Collections.emptyList());

        return affectedRows;
    }

    public int executeUpdate(String sql) throws SQLException {
        if (this.dbConfigType == DataSourceConfigType.GROUP) {
            return executeUpdateInternal0(sql, -1, null, null);
        }
        return executeUpdateInternal(sql, -1, null, null);
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (this.dbConfigType == DataSourceConfigType.GROUP) {
            return executeUpdateInternal0(sql, autoGeneratedKeys, null, null);
        }
        return executeUpdateInternal(sql, autoGeneratedKeys, null, null);
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (this.dbConfigType == DataSourceConfigType.GROUP) {
            return executeUpdateInternal0(sql, -1, columnIndexes, null);
        }
        return executeUpdateInternal(sql, -1, columnIndexes, null);
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (this.dbConfigType == DataSourceConfigType.GROUP) {
            return executeUpdateInternal0(sql, -1, null, columnNames);
        }
        return executeUpdateInternal(sql, -1, null, columnNames);
    }

    public void addBatch(String sql) throws SQLException {
        checkClosed();

        if (batchedArgs == null) {
            batchedArgs = new ArrayList<Object>();
        }

        if (sql != null) {
            batchedArgs.add(sql);
        }
    }

    /**
     * @param targetSqls: key:鏈�缁堟暟鎹簮ID; value:鏈�缁堟暟鎹簮涓婃墽琛岀殑鐗╃悊琛ㄥ悕鐨凷QL
     * @throws ZdalCheckedExcption
     */
    protected void sortBatch0(String originalSql, Map<String, List<String>> targetSqls)
                                                                                       throws SQLException {
        SqlType sqlType = getSqlType(originalSql);
        String dbselectorID = getGroupDBSelectorID(sqlType);
        if (!targetSqls.containsKey(dbselectorID)) {
            targetSqls.put(dbselectorID, new ArrayList<String>());
        }
        List<String> sqls = targetSqls.get(dbselectorID);
        sqls.add(originalSql);
    }

    /**
     * @param targetSqls: key:鏈�缁堟暟鎹簮ID; value:鏈�缁堟暟鎹簮涓婃墽琛岀殑鐗╃悊琛ㄥ悕鐨凷QL
     * @throws ZdalCheckedExcption
     */
    protected void sortBatch(String originalSql, Map<String, List<String>> targetSqls)
                                                                                      throws SQLException {
        //TODO:batch涓鏋滀娇鐢ㄤ簡鏄犲皠瑙勫垯锛屾槧灏勮鍒欐病鏈夎繑鍥炵粨鏋滄椂锛屼細鏈夐敊璇��
        try {
            List<TargetDB> targets;
            String virtualTableName;
            List<String> virtualJoinTableNames;
            if (ruleController != null) {
                TargetDBMeta metaData = ruleController.getDBAndTables(originalSql, null);
                targets = metaData.getTarget();
                virtualTableName = metaData.getVirtualTableName();
                virtualJoinTableNames = metaData.getVirtualJoinTableNames();
            } else {
                SqlType sqlType = getSqlType(originalSql);
                SqlDispatcher sqlDispatcher = selectSqlDispatcher(autoCommit, sqlType);
                DispatcherResult dispatcherResult = getExecutionMetaData(originalSql, Collections
                    .emptyList(), null, sqlDispatcher);
                targets = dispatcherResult.getTarget();
                virtualTableName = dispatcherResult.getVirtualTableName();
                virtualJoinTableNames = dispatcherResult.getVirtualJoinTableNames();
            }
            for (TargetDB target : targets) {
                //杩欓噷鍋氫簡鏂版棫瑙勫垯鍏煎
                String targetName = ruleController != null ? target.getWritePool()[0] : target
                    .getDbIndex();
                if (!targetSqls.containsKey(targetName)) {
                    targetSqls.put(targetName, new ArrayList<String>());
                }

                List<String> sqls = targetSqls.get(targetName);

                Set<String> actualTables = target.getTableNames();
                for (String tab : actualTables) {
                    String targetSql = replaceTableName(originalSql, virtualTableName, tab);
                    //濡傛灉metaData(涔熷氨鏄疍ispatcherResult)閲岄潰鏈塲oin琛ㄥ悕锛岄偅涔堝氨鏇挎崲鎺�;
                    targetSql = replaceJoinTableName(virtualTableName, virtualJoinTableNames, tab,
                        targetSql);
                    sqls.add(targetSql);
                }
            }
        } catch (ZdalCheckedExcption e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @param virtualTableName
     * @param virtualJoinTableNames
     * @param realTableName
     * @param targetSql
     * @return
     */
    private String replaceJoinTableName(String virtualTableName,
                                        List<String> virtualJoinTableNames, String realTableName,
                                        String targetSql) {
        if (virtualJoinTableNames.size() > 0) {
            String suffix = getSuffix(realTableName, virtualTableName);
            for (String vtab : virtualJoinTableNames) {
                //鐪熷疄琛ㄥ悕鍙互鐢�,鎸囧畾
                String repTab = vtab + suffix;
                String[] tabs = vtab.split(",");
                if (tabs.length == 2) {
                    vtab = tabs[0];
                    repTab = tabs[1];
                }
                targetSql = replaceTableName(targetSql, vtab, repTab);
            }
        }
        return targetSql;
    }

    public int[] executeBatch() throws SQLException {
        checkClosed();

        if (batchedArgs == null || batchedArgs.isEmpty()) {
            return new int[0];
        }

        List<SQLException> exceptions = null;

        try {
            Map<String/*鏁版嵁婧怚D*/, List<String>/*鏁版嵁婧愪笂鎵ц鐨凷QL*/> targetSqls = new HashMap<String, List<String>>();

            for (Object arg : batchedArgs) {
                if (this.dbConfigType == DataSourceConfigType.GROUP) {
                    sortBatch0((String) arg, targetSqls);
                } else {
                    sortBatch((String) arg, targetSqls);
                }

            }

            //Map<String, ConnectionAndDatasource> connections = getConnectionProxy().getActualConnections();

            for (Entry<String, List<String>> entry : targetSqls.entrySet()) {
                //濡傛灉娌″彇鍒版暟鎹簮
                String dbSelectorID = entry.getKey();
                //鏍￠獙鏄惁鍏佽batch浜嬪姟
                checkBatchDataBaseID(dbSelectorID);
                //retryContext涓簄ull鐨勬椂鍊欎細鐩存帴鎶涘嚭寮傚父銆�
                createConnectionByID(dbSelectorID);
                try {

                    Statement stmt = createStatementByDataSourceSelectorID(dbSelectorID, null);

                    actualStatements.add(stmt);

                    for (String targetSql : entry.getValue()) {
                        stmt.addBatch(targetSql);
                    }

                    // TODO: 蹇界暐杩斿洖鍊�
                    stmt.executeBatch();

                    stmt.clearBatch();
                } catch (SQLException e) {
                    if (exceptions == null) {
                        exceptions = new ArrayList<SQLException>();
                    }
                    exceptions.add(e);
                }
            }
        } finally {
            batchedArgs.clear();
        }

        ExceptionUtils.throwSQLException(exceptions, null, Collections.emptyList());

        // TODO: 蹇界暐杩斿洖鍊�
        return new int[0];
    }

    public void clearBatch() throws SQLException {
        checkClosed();

        if (batchedArgs != null) {
            batchedArgs.clear();
        }
    }

    public ResultSet getResultSet() throws SQLException {
        return results;
    }

    /**
     * 涓嶆敮鎸佸缁撴灉闆嗘煡璇紝鎬绘槸杩斿洖false
     */
    public boolean getMoreResults() throws SQLException {
        return moreResults;
    }

    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("getMoreResults");
    }

    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("getGeneratedKeys");
    }

    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("cancel");
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after statement closed.");
        }
    }

    public void closeInternal(boolean removeThis) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("invoke close");
        }

        if (closed) {
            return;
        }

        List<SQLException> exceptions = null;

        try {
            for (ResultSet resultSet : openResultSets) {
                try {
                    //bug fix by shenxun :鍐呴儴涓嶈浠杛emove,鍦═Statment涓粺涓�clear鎺変粬浠�
                    ((DummyTResultSet) resultSet).closeInternal(false);
                } catch (SQLException e) {
                    if (exceptions == null) {
                        exceptions = new ArrayList<SQLException>();
                    }
                    exceptions.add(e);
                }
            }

            for (Statement stmt : actualStatements) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    if (exceptions == null) {
                        exceptions = new ArrayList<SQLException>();
                    }
                    exceptions.add(e);
                }
            }
        } finally {
            closed = true;
            openResultSets.clear();
            actualStatements.clear();

            results = null;
            if (removeThis) {
                if (!getConnectionProxy().getOpenStatements().remove(this)) {
                    log.warn("open statement does not exist");
                }
            }
        }

        ExceptionUtils.throwSQLException(exceptions, null, Collections.emptyList());
    }

    public void close() throws SQLException {
        closeInternal(true);
    }

    /**
     * 鍦╞atch鐨勪簨鍔￠噷锛屾瘡娆￠兘瑕佹鏌ユ槸鍚﹀悓涓�涓暟鎹簮鏍囪瘑,鍙繘琛岄�昏緫搴撶殑鍒ゅ畾
     * added by fanzeng锛屼互鏀寔batch鐨勫崟搴撲簨鍔�
     * @param dbSelectorID  閫昏緫鏁版嵁婧愭爣璇�
     * @throws SQLException
     */
    public void checkBatchDataBaseID(String dbSelectorID) throws SQLException {
        if (StringUtil.isBlank(dbSelectorID)) {
            throw new SQLException("The dbSelectorID can't be null!");
        }
        //濡傛灉鍦ㄤ簨鍔′腑锛岀涓�娆″氨璁剧疆batchDataBaseId鐨勫��,鐒跺悗鐩存帴杩斿洖
        if (!isAutoCommit() && getBatchDataBaseId() == null) {
            setBatchDataBaseId(dbSelectorID);
            return;
        }
        //濡傛灉鍦ㄤ簨鍔′腑锛屽苟涓斿綋鍓嶇殑dbId鍜岀紦瀛樼殑dbId涓嶅悓锛屽嵆鎶涘嚭寮傚父锛�         
        if (!isAutoCommit() && !dbSelectorID.equals(getBatchDataBaseId())) {
            throw new SQLException("batch鎿嶄綔鍙敮鎸佸崟搴撶殑浜嬪姟,褰撳墠dbSelectorID=" + dbSelectorID + ",缂撳瓨鐨刣bId="
                                   + getBatchDataBaseId());
        }
    }

    /**
     * 浠ヤ笅涓轰笉鏀寔鐨勬柟娉�
     */
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("getFetchDirection");
    }

    public void setFetchDirection(int fetchDirection) throws SQLException {
        throw new UnsupportedOperationException("setFetchDirection");
    }

    public int getFetchSize() throws SQLException {
        return this.fetchSize;
        //throw new UnsupportedOperationException("getFetchSize");
    }

    public void setFetchSize(int fetchSize) throws SQLException {
        this.fetchSize = fetchSize;
        //throw new UnsupportedOperationException("setFetchSize");
    }

    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("getMaxFieldSize");
    }

    public void setMaxFieldSize(int maxFieldSize) throws SQLException {
        throw new UnsupportedOperationException("setMaxFieldSize");
    }

    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("getMaxRows");
    }

    public void setMaxRows(int maxRows) throws SQLException {
        throw new UnsupportedOperationException("setMaxRows");
    }

    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("getQueryTimeout");
    }

    public void setQueryTimeout(int queryTimeout) throws SQLException {
        throw new UnsupportedOperationException("setQueryTimeout");
    }

    public void setCursorName(String cursorName) throws SQLException {
        throw new UnsupportedOperationException("setCursorName");
    }

    public void setEscapeProcessing(boolean escapeProcessing) throws SQLException {
        throw new UnsupportedOperationException("setEscapeProcessing");
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    /**
     * 浠ヤ笅涓烘棤閫昏緫鐨刧etter/setter
     */
    public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

    public int getResultSetConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    public void setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
    }

    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    public void setResultSetHoldability(int resultSetHoldability) {
        this.resultSetHoldability = resultSetHoldability;
    }

    public Map<String, DBSelector> getDataSourcePool() {
        return dbSelectors;
    }

    public void setDataSourcePool(Map<String, DBSelector> dbSelectors) {
        this.dbSelectors = dbSelectors;
    }

    public ZdalConnection getConnectionProxy() {
        return connectionProxy;
    }

    public void setConnectionProxy(ZdalConnection connectionProxy) {
        this.connectionProxy = connectionProxy;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Set<ResultSet> getTResultSets() {
        return openResultSets;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public long getTimeoutForEachTable() {
        return timeoutForEachTable;
    }

    public void setTimeoutForEachTable(long timeoutForEachTable) {
        this.timeoutForEachTable = timeoutForEachTable;
    }

    public int getRetryingTimes() {
        return retryingTimes;
    }

    public void setRetryingTimes(int retryingTimes) {
        this.retryingTimes = retryingTimes;
    }

    public void setOperation_type(DB_OPERATION_TYPE operation_type) {
        this.operation_type = operation_type;
    }

    public DB_OPERATION_TYPE getOperation_type() {
        return operation_type;
    }

    public String getBatchDataBaseId() {
        return batchDataBaseId;
    }

    public void setBatchDataBaseId(String batchDataBaseId) {
        this.batchDataBaseId = batchDataBaseId;
    }

    public boolean isHintReplaceSupport() {
        return isHintReplaceSupport;
    }

    public void setHintReplaceSupport(boolean isHintReplaceSupport) {
        this.isHintReplaceSupport = isHintReplaceSupport;
    }

    public DataSourceConfigType getDbConfigType() {
        return dbConfigType;
    }

    public void setDbConfigType(DataSourceConfigType dbConfigType) {
        this.dbConfigType = dbConfigType;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public String getAppDsName() {
        return appDsName;
    }

    public void setAppDsName(String appDsName) {
        this.appDsName = appDsName;
    }

	@Override
	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
