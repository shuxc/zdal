/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.exception.runtime;

/**
 * 
 * @author ����
 * @version $Id: CantFindTargetTabRuleTypeException.java, v 0.1 2014-1-6 ����05:18:24 Exp $
 */
public class CantFindTargetTabRuleTypeException extends ZdalRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -7179888759169646552L;

    public CantFindTargetTabRuleTypeException(String msg) {
        super("�޷����������tableRule:" + msg + "�ҵ���Ӧ�Ĵ�������");
    }
}
