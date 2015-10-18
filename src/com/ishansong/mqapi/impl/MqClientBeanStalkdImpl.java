package com.ishansong.mqapi.impl;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.MqException;
import com.ishansong.mqapi.MqMessage;

/**
 * Created by zhai on 15/10/14.
 */
public class MqClientBeanStalkdImpl implements IMqClient {

    @Override
    public void close() {
    }

    @Override
    public void useTube(String tube) throws MqException {
    }

    @Override
    public void watchTube(String tube) throws MqException {

    }

    @Override
    public void ignoreTube(String tube) throws MqException {

    }

    @Override
    public String tubeStats() throws MqException {
        return null;
    }

    @Override
    public String tubeStats(String tube) throws MqException {
        return null;
    }

    @Override
    public long put(long priority, int delay, int ttr, byte[] data) throws MqException {
        return 0;
    }

    @Override
    public void deleteJob(MqMessage job) throws MqException {

    }

    @Override
    public void deleteJob(long id) throws MqException {

    }

    @Override
    public MqMessage reserve(Integer timeoutSeconds) throws MqException {
        return null;
    }

    @Override
    public void release(long id, int priority, int delay) throws MqException {

    }

    @Override
    public void release(MqMessage job, int priority, int delay) throws MqException {

    }

    @Override
    public void bury(MqMessage job, int priority) throws MqException {

    }
}
