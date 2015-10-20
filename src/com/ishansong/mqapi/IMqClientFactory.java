package com.ishansong.mqapi;

import com.ishansong.mqapi.impl.DefaultIMqClientFactory;

/**
 * the IMqClient get factory
 * Created by KaiQiang on 2015/10/18.
 */
public abstract class IMqClientFactory {
    /** 获取一个非缓冲的client */
    public abstract IMqClient getMqClient(String host, int port);
    /** 获取一个缓冲的cleint */
    public abstract IMqClient getPooledMqClient(String host, int port, int maxPoolSize) throws MqException;

    /** 工厂自举方法 */
    public final static IMqClientFactory newInstance(){
        //TODO 这里需要扩展下，根据META-INF里面的配置文件动态确定使用哪个工厂实现
        return new DefaultIMqClientFactory();
    }
}
