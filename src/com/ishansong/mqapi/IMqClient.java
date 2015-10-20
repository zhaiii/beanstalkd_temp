package com.ishansong.mqapi;

/**
 * mq ¿Í»§¶Ëapi
 * Created by zhai on 15/10/14.
 */
public interface IMqClient {
    void close();

    void useTube(String tube) throws MqException;

    void watchTube(String tube) throws MqException;

    void ignoreTube(String tube) throws MqException;

    String tubeStats() throws MqException;

    String tubeStats(String tube) throws MqException;

    long put(long priority, int delay, int ttr, byte[] data) throws MqException;

    void deleteJob(MqMessage msg) throws MqException;

    void deleteJob(long id) throws MqException;

    MqMessage reserve(Integer timeoutSeconds) throws MqException;

    void release(long id, int priority, int delay) throws MqException;

    void release(MqMessage job, int priority, int delay) throws MqException;

    void bury(MqMessage job, int priority) throws MqException;
}
