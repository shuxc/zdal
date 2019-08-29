/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.parser.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.alipay.zdal.common.lang.StringUtil;
import com.alipay.zdal.common.sqljep.function.Comparative;
import com.alipay.zdal.common.sqljep.function.ComparativeAND;
import com.alipay.zdal.common.sqljep.function.ComparativeOR;
import com.alipay.zdal.parser.GroupFunctionType;
import com.alipay.zdal.parser.exceptions.SqlParserException;
import com.alipay.zdal.parser.sql.parser.ParserException;
import com.alipay.zdal.parser.sql.stat.TableStat;
import com.alipay.zdal.parser.sql.stat.TableStat.Column;
import com.alipay.zdal.parser.sql.stat.TableStat.Mode;
import com.alipay.zdal.parser.sql.stat.TableStat.SELECTMODE;
import com.alipay.zdal.parser.sqlobjecttree.ComparativeMapChoicer;
import com.alipay.zdal.parser.visitor.BindVarCondition;
import com.alipay.zdal.parser.visitor.OrderByEle;
import com.alipay.zdal.parser.visitor.ZdalSchemaStatVisitor;

/**
 * 
 * @author xiaoqing.zhouxq
 * @version $Id: AbstractSqlParserResult.java, v 0.1 2012-5-21 涓嬪崍03:11:27 xiaoqing.zhouxq Exp $
 */
public abstract class DefaultSqlParserResult implements SqlParserResult, ComparativeMapChoicer {

    protected ZdalSchemaStatVisitor visitor;

    /**
     * 濡傛灉娌℃湁skip鍜宮ax浼氳繑鍥炴鍊�
     */
    public final static int         DEFAULT_SKIP_MAX = -1000;

    protected String                tableName        = null;

    public DefaultSqlParserResult(ZdalSchemaStatVisitor visitor) {
        if (visitor == null) {
            throw new SqlParserException("ERROR the visitor is null ");
        }
        this.visitor = visitor;
    }

    public List<OrderByEle> getGroupByEles() {
        Set<Column> columns = visitor.getGroupByColumns();
        List<OrderByEle> results = Collections.emptyList();
        for (Column column : columns) {
            OrderByEle orderByEle = new OrderByEle(column.getTable(), column.getName());
            orderByEle.setAttributes(column.getAttributes());
            results.add(orderByEle);
        }
        return results;
    }

    public GroupFunctionType getGroupFuncType() {
        if (SELECTMODE.COUNT == visitor.getSelectMode()) {
            return GroupFunctionType.COUNT;
        } else if (SELECTMODE.MAX == visitor.getSelectMode()) {
            return GroupFunctionType.MAX;
        } else if (SELECTMODE.MIN == visitor.getSelectMode()) {
            return GroupFunctionType.MIN;
        } else if (SELECTMODE.SUM == visitor.getSelectMode()) {
            return GroupFunctionType.SUM;
        }
        return GroupFunctionType.NORMAL;
    }

    public boolean isDML() {
        return (visitor.getSqlMode() == Mode.Delete) || (visitor.getSqlMode() == Mode.Insert)
               || (visitor.getSqlMode() == Mode.Select) || (visitor.getSqlMode() == Mode.Update);
    }

    public List<OrderByEle> getOrderByEles() {
        List<Column> columns = visitor.getOrderByColumns();
        List<OrderByEle> results = new ArrayList<OrderByEle>();
        for (Column column : columns) {
            OrderByEle orderByEle = new OrderByEle(column.getTable(), column.getName());
            orderByEle.setAttributes(column.getAttributes());
            results.add(orderByEle);
        }
        return results;
    }

    /**
     * 鑾峰彇琛ㄥ悕
     * @return
     * @see com.alipay.zdal.parser.result.SqlParserResult#getTableName()
     */
    public String getTableName() {
        if (visitor.getTables() == null || visitor.getTables().isEmpty()) {
            throw new SqlParserException("ERROR ## the tableName is null");
        }
        if (StringUtil.isBlank(tableName)) {
            for (Entry<TableStat.Name, TableStat> entry : visitor.getTables().entrySet()) {
                String temp = entry.getKey().getName();
                if (tableName == null) {
                    if (temp != null) {
                        tableName = temp.toLowerCase();
                    }
                } else {
                    if (temp != null && !tableName.equals(entry.getKey().getName().toLowerCase())) {
                        throw new IllegalArgumentException("sql璇彞涓殑琛ㄥ悕涓嶅悓锛岃淇濊瘉鎵�鏈塻ql璇彞鐨勮〃鍚�"
                                                           + "浠ュ強浠栦滑鐨剆chemaName鐩稿悓锛屽寘鎷唴宓宻ql");
                    }
                }
            }
        }
        return tableName;
    }

    /**
     * 鑾峰彇ComparativeMap.
     * map鐨刱ey 鏄垪鍚� value鏄粦瀹氬彉閲忓悗鐨剓@link Comparative}
     * 濡傛灉鏄釜涓嶅彲璧嬪�肩殑鍙橀噺锛屽垯涓嶄細杩斿洖銆�
     * 涓嶅彲璧嬪�兼寚鐨勬槸锛岃櫧鐒跺彲浠ヨВ鏋愶紝浣嗚В鏋愪互鍚庣殑缁撴灉涓嶈兘杩涜璁＄畻銆�
     * 濡倃here col = concat(str,str);
     * 杩欑SQL铏界劧鍙互瑙ｆ瀽锛屼絾鍥犱负瀵瑰簲鐨勫鐞嗗嚱鏁版病鏈夊畬鎴愶紝鎵�浠ユ槸涓嶈兘璧嬪�肩殑銆傝繖绉嶆儏鍐典笅col
     * 鏄笉浼氳鏀惧埌杩斿洖鐨刴ap涓殑銆�
     * @param arguments 鍙傛暟鍊煎垪琛�.
     * @param partnationSet 鎷嗗垎瀛楁鍒楄〃.
     * @return
     */
    public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
        Map<String, Comparative> copiedMap = new LinkedHashMap<String, Comparative>();
        for (String partnation : partnationSet) {
            Comparative comparative = getComparativeOf(partnation, arguments);
            if (comparative != null) {
                copiedMap.put(partnation, comparative);
            }
        }
        return copiedMap;
    }

    /**
     * 鏍规嵁鎷嗗垎瀛楁浠巗ql鐨勫瓧娈典腑鑾峰彇瀵瑰簲鐨勫垪鍚嶅拰鍒楀��.
     * @param partinationKey
     * @param arguments
     * @return
     */
    private Comparative getComparativeOf(String partinationKey, List<Object> arguments) {
        List<BindVarCondition> bindColumns = visitor.getBindVarConditions();

        List<BindVarCondition> conditions = new ArrayList<BindVarCondition>();
        for (BindVarCondition tmp : bindColumns) {
            if (tmp.getColumnName().equalsIgnoreCase(partinationKey)) {
                conditions.add(tmp);
            }
        }
        if (!conditions.isEmpty()) { //鍏堜粠缁戝畾鍙傛暟鍒楄〃涓煡鎵�.
            Comparative comparative = null;
            int index = 1;
            for (BindVarCondition bindVarCondition : conditions) {
                String op = bindVarCondition.getOperator();
                int function = Comparative.getComparisonByIdent(op);
                if (function == Comparative.NotSupport || op.trim().equalsIgnoreCase("in")) {//鏀寔鎷嗗垎瀛楁鏄痠n鐨勬ā寮�.
                    Object arg = arguments.get(bindVarCondition.getIndex());
                    Comparable<?> value = null;
                    if (arg instanceof Comparable<?>) {
                        value = (Comparable<?>) arg;
                    } else {
                        throw new ParserException("ERROR ## can not use this type of partination");
                    }

                    if (comparative == null) {
                        comparative = new Comparative(Comparative.Equivalent, value);
                        if (index == conditions.size()) {
                            return comparative;
                        }
                    } else {
                        Comparative next = new Comparative(Comparative.Equivalent, value);
                        ComparativeOR comparativeOR = new ComparativeOR();
                        comparativeOR.addComparative(comparative);
                        comparativeOR.addComparative(next);
                        comparative = comparativeOR;
                        if (index == conditions.size()) {
                            return comparativeOR;
                        }
                    }
                    index++;
                }
            }

            index = -1;
            for (BindVarCondition condition : conditions) {
                Object arg = arguments.get(condition.getIndex());
                Comparable<?> value = null;
                if (arg instanceof Comparable<?>) {
                    value = (Comparable<?>) arg;
                } else {
                    throw new ParserException("ERROR ## can not use this type of partination");
                }
                int function = Comparative.getComparisonByIdent(condition.getOperator());

                if (comparative == null) {
                    comparative = new Comparative(function, value);
                    index = condition.getIndex();
                } else {
                    Comparative next = new Comparative(function, value);
                    if (index == condition.getIndex()) {//鍦ㄥ瓙鏌ヨ涓紝瀛樺湪鎷嗗垎瀛楁鐨刬ndex鐩稿悓鐨勬儏鍐碉紝濡傛灉鐩稿悓灏变笉闇�瑕乤nd/or 杩涜鍖归厤浜�.
                        return comparative;
                    }
                    if (condition.getOp() == 1) {
                        ComparativeAND comparativeAND = new ComparativeAND();
                        comparativeAND.addComparative(comparative);
                        comparativeAND.addComparative(next);
                        return comparativeAND;
                    } else if (condition.getOp() == -1) {
                        ComparativeOR comparativeOR = new ComparativeOR();
                        comparativeOR.addComparative(comparative);
                        comparativeOR.addComparative(next);
                        return comparativeOR;
                    }
                }
            }
            return comparative;
        } else {
            List<BindVarCondition> noBindConditions = visitor.getNoBindVarConditions();

            if (noBindConditions.isEmpty()) {
                return null;
            }
            List<BindVarCondition> noBinditions = new ArrayList<BindVarCondition>();
            for (BindVarCondition tmp : noBindConditions) {
                if (tmp.getColumnName().equalsIgnoreCase(partinationKey)) {
                    int function = Comparative.getComparisonByIdent(tmp.getOperator());
                    if (function == Comparative.NotSupport) {
                        if (tmp.getOperator().trim().equalsIgnoreCase("in")) {
                            noBinditions.add(tmp);
                        } else {
                            continue;
                        }
                    } else {
                        noBinditions.add(tmp);
                    }
                }
            }
            Comparative comparative = null;
            for (BindVarCondition condition : noBinditions) {
                Comparable<?> value = condition.getValue();
                if (value == null) {
                    throw new SqlParserException(
                        "ERROR ## parse from no-bind-column of this partination is error,the partination name = "
                                + partinationKey);
                }
                if (!(value instanceof Comparable<?>)) {
                    throw new ParserException("ERROR ## can not use this type of partination");
                }
                if (condition.getOperator().trim().equalsIgnoreCase("in")) {
                    if (comparative == null) {
                        comparative = new Comparative(Comparative.Equivalent, value);
                    } else {
                        Comparative next = new Comparative(Comparative.Equivalent, value);
                        ComparativeOR comparativeOR = new ComparativeOR();
                        comparativeOR.addComparative(comparative);
                        comparativeOR.addComparative(next);
                        comparative = comparativeOR;
                    }
                } else {
                    int function = Comparative.getComparisonByIdent(condition.getOperator());

                    if (comparative == null) {
                        comparative = new Comparative(function, value);
                    } else {
                        Comparative next = new Comparative(function, value);
                        if (condition.getOp() == 1) {
                            ComparativeAND comparativeAND = new ComparativeAND();
                            comparativeAND.addComparative(comparative);
                            comparativeAND.addComparative(next);
                            return comparativeAND;
                        } else if (condition.getOp() == -1) {
                            ComparativeOR comparativeOR = new ComparativeOR();
                            comparativeOR.addComparative(comparative);
                            comparativeOR.addComparative(next);
                            return comparativeOR;
                        }
                    }
                }
            }
            return comparative;
        }
    }

    /**
     * @param tables
     * @param args
     * @param skip
     *            闂尯闂达紝浠庡摢寮�濮�
     * @param max
     *            寮�鍖洪棿锛岃嚦鍝�
     * @return
     */
    public void getSqlReadyToRun(Set<String> tables, List<Object> args, Number skip, Number max,
                                 Map<Integer, Object> modifiedMap) {
        if (tables == null) {
            throw new IllegalArgumentException("寰呮浛鎹㈣〃鍚嶄负绌�");
        }

        //濡傛灉鏄痵kip 鍜� max 閮藉瓨鍦紝骞朵笖鏄粦瀹氬彉閲忕殑鎯呭喌锛屽垯杩涜鍙傛暟鐨勬浛鎹�
        if (this.isSkipBind() < 0 && this.isRowCountBind() < 0) {
            throw new IllegalArgumentException("The limit skip or rowCount set error!");
        }
        modifyParam(skip, isSkipBind(), modifiedMap);
        modifyParam(max, isRowCountBind(), modifiedMap);
    }

    protected void modifyParam(Number num, int index, Map<Integer, Object> changeParam) {

        Object obj = null;
        if (num instanceof Long) {
            obj = (Long) num;
        } else if (num instanceof Integer) {
            obj = (Integer) num;
        } else {
            throw new IllegalArgumentException("鍙敮鎸乮nt long鐨勬儏鍐�");
        }
        changeParam.put(index, obj);
    }

    protected String toColumns(Set<Column> columns) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        for (Column column : columns) {
            result.append(column);
            if (i != (columns.size() - 1)) {
                result.append(",");
            }
        }
        return result.toString();
    }

    public ComparativeMapChoicer getComparativeMapChoicer() {
        return this;
    }

}
