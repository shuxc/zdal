/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.exception.runtime;

/**
 * 
 * @author ����
 * @version $Id: CantIdentifyNumberExcpetion.java, v 0.1 2014-1-6 ����05:18:39 Exp $
 */
public class CantIdentifyNumberExcpetion extends ZdalRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 7861250013675710468L;

    public CantIdentifyNumberExcpetion(String input, String input1, Throwable e) {
        super("�ؼ��֣�" + input + "��" + input1 + "����ʶ��Ϊһ�������������趨", e);
    }
}
