package com.ishansong.mqapi;

import com.ishansong.mqapi.impl.DefaultIMqClientFactory;

/**
 * the IMqClient get factory
 * Created by zhai on 2015/10/18.
 */
public abstract class IMqClientFactory {
    /** ????????client */
    public abstract IMqClient getMqClient(String host, int port);
    /** ???????cleint */
    public abstract IMqClient getPooledMqClient(String host, int port, int maxPoolSize) throws MqException;

    /** ?????? */
    public final static IMqClientFactory newInstance(){
        //TODO ??????????META-INF???????????????????
        return new DefaultIMqClientFactory();
    }
}
