/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.exception.runtime;

/**
 * 
 * @author ����
 * @version $Id: CantFindTargetTabRuleTypeHandlerException.java, v 0.1 2014-1-6 ����05:18:30 Exp $
 */
public class CantFindTargetTabRuleTypeHandlerException extends ZdalRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -4073830327289870566L;

    public CantFindTargetTabRuleTypeHandlerException(String msg) {
        super("�޷��ҵ�" + msg + "��Ӧ�Ĵ�����");
    }
}
