package com.ishansong.mqapi.impl;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.MqException;
import com.ishansong.mqapi.MqMessage;
import trendrr.beanstalk.BeanstalkClient;
import trendrr.beanstalk.BeanstalkException;
import trendrr.beanstalk.BeanstalkJob;

/**
 * the BeanStalkd implement of the MqClient
 * Created by zhai on 15/10/14.
 */
public class MqClientBeanStalkdImpl implements IMqClient {
    static final String DEFAULT_TUBE =  "default";
    private BeanstalkClient bkc;

    MqClientBeanStalkdImpl(String host, int port){
        this.bkc = new BeanstalkClient(host, port, DEFAULT_TUBE);
    }

    MqClientBeanStalkdImpl(String host, int port, String tube){
        this.bkc = new BeanstalkClient(host, port, tube);
    }

    MqClientBeanStalkdImpl(BeanstalkClient bkc){
        this.bkc = bkc;
    }

    @Override
    public void close() {
        if(this.bkc != null) {
            this.bkc.close();
            this.bkc = null;
        }
    }

    @Override
    public void useTube(final String tube) throws MqException {
        convertException(new CalleeVoid() {
            @Override
            public void calls() throws BeanstalkException {
                MqClientBeanStalkdImpl.this.bkc.useTube(tube);
            }
        });

    }

    @Override
    public void watchTube(final String tube) throws MqException {
        convertException(new CalleeVoid() {
            @Override
            public void calls() throws BeanstalkException {
                MqClientBeanStalkdImpl.this.bkc.watchTube(tube);
            }
        });
    }

    @Override
    public void ignoreTube(String tube) throws MqException {
        convertException(() -> this.bkc.ignoreTube(tube));
    }

    @Override
    public String tubeStats() throws MqException {
        return (String)convertException(() -> this.bkc.tubeStats());
    }

    @Override
    public String tubeStats(String tube) throws MqException {
        return (String)convertException(() -> this.bkc.tubeStats(tube));
    }

    @Override
    public long put(long priority, int delay, int ttr, byte[] data) throws MqException {
        return (Long)convertException(() -> this.bkc.put(priority, delay, ttr, data));
    }

    @Override
    public void deleteJob(MqMessage job) throws MqException {
        convertException(() -> this.bkc.deleteJob(convertToBeanstalkJob(job)));
    }

    private BeanstalkJob convertToBeanstalkJob(MqMessage job) {
        BeanstalkJob bjob = new BeanstalkJob();
        bjob.setClient(bkc);
        bjob.setData(job.getData());
        bjob.setId(job.getId());
        return bjob;
    }

    private MqMessage convertToMqMessage(BeanstalkJob bkJob){
        MqMessage mm = new MqMessage();
        mm.setId(bkJob.getId());
        mm.setData(bkJob.getData());
        mm.setCleint(this);
        return mm;
    }

    @Override
    public void deleteJob(long id) throws MqException {
        convertException(() -> this.bkc.deleteJob(id));
    }

    @Override
    public MqMessage reserve(Integer timeoutSeconds) throws MqException {
        BeanstalkJob bkJob = (BeanstalkJob)
                convertException(() -> this.bkc.reserve(timeoutSeconds));
        return convertToMqMessage(bkJob);
    }

    @Override
    public void release(long id, int priority, int delay) throws MqException {
        convertException(() -> this.bkc.release(id, priority, delay));
    }

    @Override
    public void release(MqMessage job, int priority, int delay) throws MqException {
        convertException(() -> this.bkc.release(convertToBeanstalkJob(job), priority, delay));
    }

    @Override
    public void bury(MqMessage job, int priority) throws MqException {
        convertException(() -> this.bkc.bury(convertToBeanstalkJob(job), priority));
    }

    private interface Callee{
        Object calls() throws BeanstalkException;
    }
    private interface CalleeVoid{
        void calls() throws BeanstalkException;
    }

    private Object convertException(Callee call) throws MqException {
        try {
            return call.calls();
        }catch(BeanstalkException bke){
            throw new MqException(bke);
        }
    }
    private void convertException(CalleeVoid call) throws MqException {
        try {
            call.calls();
        }catch(BeanstalkException bke){
            throw new MqException(bke);
        }
    }
}
