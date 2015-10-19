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
        //TODO 这里需要扩展下，根据META-INF里面的配置文件动态确定使用哪个工厂实现
        return new DefaultIMqClientFactory();
    }
}
