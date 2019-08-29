/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.parser.result;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alipay.zdal.parser.GroupFunctionType;
import com.alipay.zdal.parser.sqlobjecttree.ComparativeMapChoicer;
import com.alipay.zdal.parser.visitor.OrderByEle;
/**
 * sql-parser瑙ｆ瀽鍚庣殑缁撴灉.
 * @author xiaoqing.zhouxq
 * @version $Id: SqlParserResult.java, v 0.1 2012-5-21 涓嬪崍03:29:18 xiaoqing.zhouxq Exp $
 */
public interface SqlParserResult {

    /**
     * 鑾峰彇褰撳墠琛ㄥ悕,濡傛灉sql涓寘鍚寮犺〃锛岄粯璁ゅ彧杩斿洖绗竴寮犺〃.
     * @return
     */
    String getTableName();

    /**
     * 鑾峰彇order by 鐨勪俊鎭�
     * @return
     */
    List<OrderByEle> getOrderByEles();

    /**
     * 鑾峰彇group by 淇℃伅
     * @return
     */
    List<OrderByEle> getGroupByEles();

    /**
     * insert/update/delete/select.
     * @return
     */
    boolean isDML();

    /**
     * 鑾峰彇sql鐨凷KIP鍊煎鏋滄湁鐨勮瘽锛屾病鏈夌殑鎯呭喌涓嬩細杩斿洖DEFAULT鍊�
     * @param arguments 鍙傛暟鍊煎垪琛�.
     * @return
     */
    int getSkip(List<Object> arguments);

    /**
     * 杩斿洖skip缁戝畾鍙橀噺鐨勪笅鏍�,濡傛灉娌℃湁灏辫繑鍥�-1.
     * @return
     */
    int isSkipBind();

    /**
     * 鑾峰彇sql鐨刴ax鍊煎鏋滄湁鐨勮瘽锛屾病鏈夌殑璇濅細杩斿洖DEFAULT鍊�
     * @param arguments 鍙傛暟鍊煎垪琛�.
     * @return
     */
    int getMax(List<Object> arguments);

    /**
     * 杩斿洖rowCount缁戝畾鍙橀噺鐨勪笅鏍�,濡傛灉娌℃湁灏辫繑鍥�-1.
     * @return
     */
    int isRowCountBind();

    /**
     * 鎴栬褰撳墠sql鐨勬渶澶栧眰鐨刧roup function.濡傛灉鏈変笖浠呮湁涓�涓猤roup function,閭ｄ箞浣跨敤璇unction
     * 濡傛灉娌℃湁group function鎴栬�呮湁澶氫釜group function.鍒欒繑鍥濶ORMAL
     * 
     * @return
     */
    GroupFunctionType getGroupFuncType();

    /**
     * 鍙嶅悜杈撳嚭鐨勬帴鍙�
     * @param tables
     * @param args
     * @param skip
     * @param max
     * @param outputType
     * @param modifiedMap
     * @return
     */
    void getSqlReadyToRun(Set<String> tables, List<Object> args, Number skip,
                                               Number max, 
                                               Map<Integer, Object> modifiedMap);
    


    /**
     * 鑾峰彇缁撴灉闆嗙瓫閫夊櫒
    * @return
    */
    ComparativeMapChoicer getComparativeMapChoicer();
    
    
}
