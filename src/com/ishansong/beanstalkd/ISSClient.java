package com.ishansong.beanstalkd;

import com.trendrr.beanstalk.BeanstalkClient;
import com.trendrr.beanstalk.BeanstalkException;
import com.trendrr.beanstalk.BeanstalkJob;

/**
 * Created by zhai on 15/10/14.
 */
public class ISSClient implements ISSBeanstalkClient{

    @Override
    public void close() {
    }

    @Override
    public void useTube(String tube) throws BeanstalkException {
    }

    @Override
    public void watchTube(String tube) throws BeanstalkException {

    }

    @Override
    public void ignoreTube(String tube) throws BeanstalkException {

    }

    @Override
    public String tubeStats() throws BeanstalkException {
        return null;
    }

    @Override
    public String tubeStats(String tube) throws BeanstalkException {
        return null;
    }

    @Override
    public long put(long priority, int delay, int ttr, byte[] data) throws BeanstalkException {
        return 0;
    }

    @Override
    public void deleteJob(BeanstalkJob job) throws BeanstalkException {

    }

    @Override
    public void deleteJob(long id) throws BeanstalkException {

    }

    @Override
    public BeanstalkJob reserve(Integer timeoutSeconds) throws BeanstalkException {
        return null;
    }

    @Override
    public void release(long id, int priority, int delay) throws BeanstalkException {

    }

    @Override
    public void release(BeanstalkJob job, int priority, int delay) throws BeanstalkException {

    }

    @Override
    public void bury(BeanstalkJob job, int priority) throws BeanstalkException {

    }
}
