package com.ishansong.mqapi.impl;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.IMqClientFactory;
import com.ishansong.mqapi.MqException;
import trendrr.beanstalk.BeanstalkClient;
import trendrr.beanstalk.BeanstalkException;
import trendrr.beanstalk.BeanstalkPool;

import java.util.HashMap;
import java.util.Map;

/**
 * the default factory impl
 * Created by zhai on 2015/10/19.
 */
public class DefaultIMqClientFactory extends IMqClientFactory {
    private final static Map<String, BeanstalkPool> bkPools = new HashMap<>();

    @Override
    public IMqClient getMqClient(String host, int port) {
        return new MqClientBeanStalkdImpl(host, port);
    }

    public IMqClient getMqClient(String host, int port, String tube) {
        return new MqClientBeanStalkdImpl(host, port, tube);
    }

    @Override
    public IMqClient getPooledMqClient(String host, int port, int maxPoolSize) throws MqException {
        return getPooledMqClient(host, port, null, maxPoolSize);
    }
        @Override
    public IMqClient getPooledMqClient(String host, int port, String tube, int maxPoolSize) throws MqException {
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
            BeanstalkClient bkc = pool.getClient();
            if(tube == null)
                tube = MqClientBeanStalkdImpl.DEFAULT_TUBE;
            bkc.useTube(tube);
            return new MqClientBeanStalkdImpl(bkc);
        } catch (BeanstalkException e) {
            throw new MqException(e);
        }
    }
}
