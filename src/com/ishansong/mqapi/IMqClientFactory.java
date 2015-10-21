package com.ishansong.mqapi;

import com.ishansong.mqapi.impl.DefaultIMqClientFactory;

/**
 * the IMqClient get factory
 * Created by zhai on 2015/10/18.
 */
public abstract class IMqClientFactory {
    private static IMqClientFactory instance;

    /** ????????client */
    public abstract IMqClient getMqClient(String host, int port);
    public abstract IMqClient getMqClient(String host, int port, String tube);
    /** ???????cleint */
    public abstract IMqClient getPooledMqClient(String host, int port, int maxPoolSize) throws MqException;
    public abstract IMqClient getPooledMqClient(String host, int port, String tube, int maxPoolSize) throws MqException;

    /** ?????? */
    public synchronized final static IMqClientFactory newInstance(){
        //TODO ??????????META-INF???????????????????
        if(instance == null)
            instance = new DefaultIMqClientFactory();
        return instance;
    }
}
