/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2012 All Rights Reserved.
 */
package com.alipay.zdal.common.exception.checked;

/**
 * 
 * @author ����
 * @version $Id: CantFindPositionByParamException.java, v 0.1 2014-1-6 ����05:17:50 Exp $
 */
public class CantFindPositionByParamException extends ZdalCheckedExcption {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 3682437768303903330L;

    public CantFindPositionByParamException(String param) {
        super("���ܸ���" + param + "�����ҵ����Ӧ��λ�ã���ע��ֱ����֧����Ϲ����벻Ҫʹ����Ϲ��������зֱ��ѯ");
    }
}
