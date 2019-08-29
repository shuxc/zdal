/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.sqljep.function;

/**
 * AND�ڵ�
 * ��ʵ�ʵ�SQL�У�ʵ����������
 * [Comparative]              [comparative]
 * 			\                  /
 * 			  \				  /
 *             [ComparativeOR]
 *             
 * ���������Ľڵ����
 * 
 *
 */
public class ComparativeOR extends ComparativeBaseList {

    public ComparativeOR(int function, Comparable<?> value) {
        super(function, value);
    }

    public ComparativeOR() {
    };

    public ComparativeOR(Comparative item) {
        super(item);
    }

    public ComparativeOR(int capacity) {
        super(capacity);
    }

    @Override
    protected String getRelation() {
        return " or ";
    }
}
