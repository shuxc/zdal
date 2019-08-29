/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.exception.checked;

/**
 * 
 * @author ����
 * @version $Id: CantLoadRowJepRuleException.java, v 0.1 2014-1-6 ����05:17:59 Exp $
 */
public class CantLoadRowJepRuleException extends ZdalCheckedExcption {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1765363763147779906L;

    public CantLoadRowJepRuleException(String expression, String vtable, String parameter) {
        super("�޷�ͨ��param:" + parameter + "|virtualTableName:" + vtable + "|expression:"
              + expression + "�ҵ�ָ���Ĺ����ж�����");
    }

}
