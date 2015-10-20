package com.ishansong.mqapi.impl;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.IMqClientFactory;
import com.ishansong.mqapi.MqException;
import trendrr.beanstalk.BeanstalkException;
import trendrr.beanstalk.BeanstalkPool;

import java.util.HashMap;
import java.util.Map;

/**
 * the default factory impl
 * Created by KaiQiang on 2015/10/19.
 */
public class DefaultIMqClientFactory extends IMqClientFactory {
    private final static Map<String, BeanstalkPool> bkPools = new HashMap<>();

    @Override
    public IMqClient getMqClient(String host, int port) {
        return new MqClientBeanStalkdImpl(host, port);
    }

    @Override
    public IMqClient getPooledMqClient(String host, int port, int maxPoolSize) throws MqException {
        String key = host + ':' + port;
        BeanstalkPool pool = bkPools.get(key);
        if (pool == null) {
            synchronized (DefaultIMqClientFactory.class) {
                pool = bkPools.get(key);
                if (pool == null) {
                    pool = new BeanstalkPool(host, port, maxPoolSize);
                    bkPools.put(key, pool);
                }
            }
        }
        try {
            return new MqClientBeanStalkdImpl(pool.getClient());
        } catch (BeanstalkException e) {
            throw new MqException(e);
        }
    }
}
