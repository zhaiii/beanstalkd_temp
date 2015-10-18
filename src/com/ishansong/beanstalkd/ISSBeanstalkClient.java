package com.ishansong.beanstalkd;

import com.trendrr.beanstalk.BeanstalkException;
import com.trendrr.beanstalk.BeanstalkJob;

/**
 * Created by zhai on 15/10/14.
 */
public interface ISSBeanstalkClient {
    void close();

    void useTube(String tube) throws BeanstalkException;

    void watchTube(String tube) throws BeanstalkException;

    void ignoreTube(String tube) throws BeanstalkException;

    String tubeStats() throws BeanstalkException;

    String tubeStats(String tube) throws BeanstalkException;

    long put(long priority, int delay, int ttr, byte[] data) throws BeanstalkException;

    void deleteJob(BeanstalkJob job) throws BeanstalkException;

    void deleteJob(long id) throws BeanstalkException;

    BeanstalkJob reserve(Integer timeoutSeconds) throws BeanstalkException;

    void release(long id, int priority, int delay) throws BeanstalkException;

    void release(BeanstalkJob job, int priority, int delay) throws BeanstalkException;

    void bury(BeanstalkJob job, int priority) throws BeanstalkException;
}
