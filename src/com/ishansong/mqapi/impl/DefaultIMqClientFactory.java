package com.ishansong.mqapi.impl;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.IMqClientFactory;

/**
 * Created by KaiQiang on 2015/10/19.
 */
public class DefaultIMqClientFactory extends IMqClientFactory {

    @Override
    public IMqClient getMqClient(String host, int port) {
        return null;
    }

    @Override
    public IMqClient getPooledMqClient(String host, int port) {
        return null;
    }

    @Override
    public void closePooledMqClient(IMqClient client) {

    }

    @Override
    public void closeAllPooledMqClient() {

    }
}
