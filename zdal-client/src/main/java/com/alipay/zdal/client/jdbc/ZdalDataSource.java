/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.client.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.alipay.zdal.client.exceptions.ZdalClientException;
import com.alipay.zdal.common.Closable;

/**
 * Zdal 瀵瑰鍏竷鐨勬暟鎹簮,鏀寔鍔ㄦ�佽皟鏁存暟鎹簮鐨勯厤缃俊鎭紝鍒囨崲绛夊姛鑳�<br>
 * 娉ㄦ剰锛� 1,浣跨敤鍓嶈鍔″繀鍏堣缃產ppName,appDsName,dbmode,configPath鐨勫�硷紝骞朵笖璋冪敤init鏂规硶杩涜鍒濆鍖�;
 * 2,浠巆onfigPath鑾峰彇閰嶇疆淇℃伅: <br>
 * <bean id="testZdalDataSource" class="com.alipay.zdal.client.jdbc.ZdalDataSource" init-method="init" destroy-method="close"> 
 *      <property name="appName" value="appName"/> 
 *      <property name="appDsName" value="appDsName"/> 
 *      <property name="dbmode" value="dev"/> 
 *      <property name="configPath" value="/home/admin/appName-run/jboss/deploy"/> 
 * </bean>
 * 
 * @author 浼墮
 * @version $Id: ZdalDataSource.java, v 0.1 2012-11-17 涓嬪崍4:08:43 Exp $
 */
public class ZdalDataSource extends AbstractZdalDataSource implements DataSource, Closable {

    public void init() {
        if (super.inited.get() == true) {
            throw new ZdalClientException("ERROR ## init twice");
        }
        try {
            super.initZdalDataSource();
        } catch (Exception e) {
            CONFIG_LOGGER.error("zdal init fail,config:" + this.toString(), e);
            throw new ZdalClientException(e);
        }

    }

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return Logger.getLogger(ZdalDataSource.class.getName());
	}
}
