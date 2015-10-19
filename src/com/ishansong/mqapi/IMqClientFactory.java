package com.ishansong.mqapi;

import com.ishansong.mqapi.impl.DefaultIMqClientFactory;

/**
 * the IMqClient get factory
 * Created by KaiQiang on 2015/10/18.
 */
public abstract class IMqClientFactory {

    public abstract IMqClient getMqClient(String host, int port);
    public abstract IMqClient getPooledMqClient(String host, int port);
    public abstract void closePooledMqClient(IMqClient client);
    public abstract void closeAllPooledMqClient();

    public final static IMqClientFactory newInstance(){
        //TODO ������Ҫ��չ�£�����META-INF����������ļ���̬ȷ��ʹ���ĸ�����ʵ��
        return new DefaultIMqClientFactory();
    }
}
