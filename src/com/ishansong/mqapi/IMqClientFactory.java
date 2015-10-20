package com.ishansong.mqapi;

import com.ishansong.mqapi.impl.DefaultIMqClientFactory;

/**
 * the IMqClient get factory
 * Created by KaiQiang on 2015/10/18.
 */
public abstract class IMqClientFactory {
    /** ��ȡһ���ǻ����client */
    public abstract IMqClient getMqClient(String host, int port);
    /** ��ȡһ�������cleint */
    public abstract IMqClient getPooledMqClient(String host, int port, int maxPoolSize) throws MqException;

    /** �����Ծٷ��� */
    public final static IMqClientFactory newInstance(){
        //TODO ������Ҫ��չ�£�����META-INF����������ļ���̬ȷ��ʹ���ĸ�����ʵ��
        return new DefaultIMqClientFactory();
    }
}
