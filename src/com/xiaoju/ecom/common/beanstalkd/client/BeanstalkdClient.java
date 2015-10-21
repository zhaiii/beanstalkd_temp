/*
 * Copyright 2015 Didichuxing.com, Inc. or its affiliates. All Rights Reserved.
 */
package com.xiaoju.ecom.common.beanstalkd.client;

import com.surftools.BeanstalkClient.BeanstalkException;
import com.surftools.BeanstalkClient.BeanstalkExceptionType;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.JobImpl;

import java.lang.reflect.Constructor;
import java.security.MessageDigest;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Benastalk client proxy for who want to use a cluster of beanstalks as a single queue service. The client is
 * not threadsafe, you should not use it crossing threads. A job must be released from the connection it has
 * been reserved.
 *
 * @author hanli
 */
public class BeanstalkdClient {

    /**
     * The Beanstalk cluster hosts ip:port pairs. If you write you own DistributeStrategy implementation for
     * your producer, the strategy.shard() result is the index of the host you want to use. the arraylist is
     * passed from the constructor, so you know the host order.
     */
    private ArrayList<Client> hosts = null;
    private static final Logger logger = Logger.getLogger("BeanstalkdClient");

    /**
     * the strategy used for client reserve-timeout cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyReserveTO = null;
    /**
     * the strategy used for client reserve cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyReserve = null;
    /**
     * the strategy used for client put cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyPut = null;
    /**
     * the strategy used for client peek cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyPeekBuried = null;
    /**
     * the strategy used for client peek-delayed cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyPeekDelayed = null;
    /**
     * the strategy used for client peek-ready cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyPeekReady = null;
    /**
     * the strategy used for client kick cmd. the default strategy is RoundRobinStrategy.
     */
    private DistributeStrategy strategyKick = null;

    /**
     * the default connect time out, 20 milliseconds
     */
    private static final int DEFAULT_TIMEOUT = 20;

    /**
     * Construct the BeanstalkClient Proxy with the server endpoints.
     *
     * @param servers servers are the host:ip pairs that your beanstalk instances setup. the order of the
     * hosts is important, because the intenal implementation will use the specific host according the
     * strategy sharding result, which is the index of the host in the host arraylist.
     *
     */
    public BeanstalkdClient(ArrayList<String> servers) {
        this(servers, com.surftools.BeanstalkClientImpl.ClientImpl.class);
    }

    /**
     * Construct the BeanstalkClient Proxy with the server endpoints.
     *
     * @param servers servers are the host:ip pairs that your beanstalk instances setup. the order of the
     * hosts is important, because the intenal implementation will use the specific host according the
     * strategy sharding result, which is the index of the host in the host arraylist.
     * @param connectTimeout with the connect timeout
     *
     */
    public BeanstalkdClient(ArrayList<String> servers, Integer connectTimeout) throws IllegalArgumentException, BeanstalkException {
        this(servers, com.surftools.BeanstalkClientImpl.ClientImpl.class, connectTimeout);
    }

    /**
     * Constuct the BeanstalkClient Proxy with the server endpoints and an implementation class of the
     * low-layer client. Most of the time, it will be the com.surftools.BeanstalkClientImpl.ClientImpl.class.
     * Other time, it will be a mock class for testing.
     *
     * @param servers the instance endpoints of all u want to use as a single service.
     * @param beanstalkImplClass the implementation class use as the real beanstalk client.
     * @throws IllegalArgumentException
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public BeanstalkdClient(ArrayList<String> servers, Class<?> beanstalkImplClass) throws IllegalArgumentException, BeanstalkException {
        this(servers, beanstalkImplClass, DEFAULT_TIMEOUT);
    }

    /**
     * Constuct the BeanstalkClient Proxy with the server endpoints and an implementation class of the
     * low-layer client. Most of the time, it will be the com.surftools.BeanstalkClientImpl.ClientImpl.class.
     * Other time, it will be a mock class for testing.
     *
     * @param servers the instance endpoints of all u want to use as a single service.
     * @param beanstalkImplClass the implementation class use as the real beanstalk client.
     * @param connectTimeout with the connect timeout
     * @throws IllegalArgumentException
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public BeanstalkdClient(ArrayList<String> servers, Class<?> beanstalkImplClass, Integer connectTimeout) throws IllegalArgumentException, BeanstalkException {
        for (String server : servers) {
            Boolean res = checkServerAddress(server);
            if (res == false) {
                throw new IllegalArgumentException("invalid service endpoint");
            }
        }
        hosts = new ArrayList<Client>();
        for (String server : servers) {
            String[] parts = server.split(":");
            String ip = parts[0];
            int port = Integer.valueOf(parts[1]);
            hosts.add(new Client(beanstalkImplClass, ip, port, connectTimeout));
        }
        if (hosts.isEmpty()) {
            throw new BeanstalkException(BeanstalkExceptionType.ADDRESS);
        }
        this.strategyPut = new RoundRobinStrategy(hosts.size());
        this.strategyReserveTO = new RoundRobinStrategy(hosts.size());
        this.strategyReserve = new RoundRobinStrategy(hosts.size(), true);
        this.strategyPeekBuried = new RoundRobinStrategy(hosts.size());
        this.strategyPeekDelayed = new RoundRobinStrategy(hosts.size());
        this.strategyPeekReady = new RoundRobinStrategy(hosts.size());
        this.strategyKick = new RoundRobinStrategy(hosts.size());

    }

    /**
     * Check an instance endpoint if is valid. A valid endpint format is like this, "192.168.199.200:11300"
     *
     * @param addr the address which will be checked.
     * @return if the addr is in valid format, return true, else false.
     */
    private Boolean checkServerAddress(String addr) {
        String[] parts = addr.split(":");
        return parts.length == 2;
    }

    /**
     * Close the client. Explicit close the bottom client that has the socket resource.
     */
    public void close() {
        for (Client client : hosts) {
            client.release();
        }
        hosts.clear();
    }

    /**
     * The producer use cmd. The producer should use a specific tube before put anything into the beanstalk
     * queue. If not appoint any tube, the client will use the 'default' tube, which maybe misused.
     *
     * @param tubeName the tube name u want to put the data into. the tube can only use these characters
     * "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+/;.$_()", all others not in the set is
     * invalid.
     * @throws IllegalArgumentException
     */
    public void use(String tubeName) throws IllegalArgumentException {
        if (tubeName == null) {
            throw new IllegalArgumentException("invalid tube name");
        }
        for (Client client : hosts) {
            client.setUsedTube(tubeName);
            try {
                if (client.getClientImpl() != null) {
                    client.getClientImpl().useTube(tubeName);
                }
            } catch (BeanstalkException e) {
                client.resetClientImpl();
            }
        }
    }

    /**
     * The producer put cmd. The producer use this cmd to put a job into the specific tube(which already
     * specified in the use cmd). The JobBuilder is a job build class that can combines some parameters
     * together in a single argv. A strategy, which can determines where the job will be put, can also be
     * assigned to this JobBuilder.
     *
     * @param builder a builder that build a put job.
     * @return the jobid if put successfully.
     * @throws IllegalArgumentException
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public long put(JobBuilder builder) throws IllegalArgumentException, BeanstalkException {
        if (builder == null) {
            throw new IllegalArgumentException("invlaid JobBuilder");
        }

        long priority = builder.getPriority();
        int delaySeconds = builder.getDelaySeconds();
        int timeToRun = builder.getTimeToRun();
        byte[] data = builder.getData();
        DistributeStrategy strategy = builder.getStrategy();
        if (strategy == null) {
            strategy = this.strategyPut;
        }

        long jobid = -1;
        if (priority > JobImpl.DEFAULT_PRIORITY) {
            throw new IllegalArgumentException("invalid priority");
        }
        if (delaySeconds < 0) {
            throw new IllegalArgumentException("invalid delay time");
        }
        if (data == null) {
            throw new IllegalArgumentException("invalid data");
        }
        strategy.reset();

        do {
            int server_idx = (int) (Math.abs(strategy.shard()) % hosts.size());
            Client client = hosts.get(server_idx);
            try {

                if (client.getClientImpl() != null) {
                    jobid = client.getClientImpl().put(priority, delaySeconds, timeToRun, data);
                    if (jobid >= 0) {
                        return jobid;
                    }
                }
            } catch (BeanstalkException e) {
                BeanstalkExceptionType etype = e.getReason();
                if (etype == BeanstalkExceptionType.JOBTOOBIG) {
                    throw e;
                } else {
                    // maybe there is io error
                    client.resetClientImpl();
                }
            }
        } while (strategy.tryNextServer());
        throw new BeanstalkException(BeanstalkExceptionType.ALLFAILD);
    }

    /**
     * The consumer watch cmd. The consumer use this cmd to watch a specific tube. A consumer can watch many
     * tubes as it want. A consumer is always watch the 'default' tube when connects to the beanstalk service,
     * so the caller should explicitly call the 'ignore' cmd below to unwatch the 'default' tube.
     *
     * @param tubeName the tube name that the comsumer want to watch
     * @return the already wathed tube count. ideally, all the service under the proxy are return the same
     * count number, but we don't know what kind exceptions will be happend, so the count number is not
     * trustable.
     * @throws IllegalArgumentException
     */
    public int watch(String tubeName) throws IllegalArgumentException {
        if (tubeName == null) {
            throw new IllegalArgumentException("invalid tubeName");
        }
        int watched_count = 0;
        for (Client client : hosts) {
            client.addWatchedTubes(tubeName);
            try {
                if (client.getClientImpl() != null) {
                    watched_count = client.getClientImpl().watch(tubeName);
                }
            } catch (Exception e) {
                client.resetClientImpl();
            }
        }

        return watched_count;
    }

    /**
     * The consumer ignore cmd. The consumer use this cmd to ignore the tube it not interested in. After
     * ignore the tube, the consumer will never receive the job from that tube.
     *
     * @param tubeName the tube name that the consumer want to ignore.
     * @return the watched count after ignore the specific tube.
     * @throws IllegalArgumentException
     */
    public int ignore(String tubeName) throws IllegalArgumentException {
        if (tubeName == null) {
            throw new IllegalArgumentException("invalid tubeName");
        }
        int watched_count = 0;
        for (Client client : hosts) {
            client.removeWatchedTubes(tubeName);
            if (client.getClientImpl() != null) {
                try {
                    watched_count = client.getClientImpl().ignore(tubeName);
                } catch (Exception e) {
                    client.resetClientImpl();
                }
            }
        }
        return watched_count;
    }

    /**
     * The consumer reserve cmd. The consumer use this cmd got get job from the tubes which already been
     * watched. this method use the default reserveTimeOut strategy.
     *
     * @param timeoutSeconds the waiting time that the consumer can wait before get a job.
     * @return the job reserved from the beanstalk service.
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public Job reserve(int timeoutSeconds) throws BeanstalkException {
        return this.reserve(timeoutSeconds, this.strategyReserveTO);
    }

    /**
     * The consumer reserve-timeout cmd. The consumer use this cmd got get job from the tubes which already
     * been watched, with a timeout. this method use a user defined strategy.
     *
     * @param timeoutSeconds the waiting time that the consumer can wait before get a job.
     * @param strategy the user defined strategy, the proxy reserve the job according the strategy sharding
     * result.
     * @return the job reserved from the beanstalk service.
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public Job reserve(int timeoutSeconds, DistributeStrategy strategy) throws BeanstalkException {
        if (timeoutSeconds < 0) {
            timeoutSeconds = 0;
        } else {
            timeoutSeconds /= hosts.size();
        }
        Job job = null;
        strategy.reset();
        do {
            int server_idx = (int) (Math.abs(strategy.shard()) % hosts.size());
            Client client = hosts.get(server_idx);
            try {
                if (client.getClientImpl() != null) {
                    job = client.getClientImpl().reserve(timeoutSeconds);

                    if (job != null) {
                        job.setServerIndex(server_idx);
                        job.setClient(client.getClientImpl());
                        return job;
                    }
                }
            } catch (BeanstalkException e) {
                BeanstalkExceptionType etype = e.getReason();
                if (etype == BeanstalkExceptionType.DEADLINESOON) {
                    throw e;
                } else {
                    client.resetClientImpl();
                }
            }

        } while (strategy.tryNextServer());

        return job;
    }

    /**
     * Sleep utility method.
     *
     * @param millis
     */
    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
        }
    }

    /**
     * The consumer reserve cmd. Consumer use this cmd to reserve the job, consumer will be blocked until
     * there is a valid job reserved from any beanstalk service.
     *
     * @return the job reserved from the beanstalk service.
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public Job reserve() throws BeanstalkException {
        return this.reserve(strategyReserve);
    }

    /**
     * The consumer reserve cmd. Consumer use this cmd to reserve the job, consumer will be blocked until
     * there is a valid job reserved from any beanstalk service. use the specific strategy
     *
     * @param strategy the consumer specified strategy.
     * @return the job reserved from the beanstalk service.
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public Job reserve(DistributeStrategy strategy) throws BeanstalkException {
        final int sleepMillis = 10;
        Job job = null;
        strategy.reset();
        do {
            int server_idx = (int) (Math.abs(strategy.shard()) % hosts.size());
            Client client = hosts.get(server_idx);
            try {
                if (client.getClientImpl() != null) {
                    job = client.getClientImpl().reserve(0);
                    if (job != null) {
                        job.setServerIndex(server_idx);
                        job.setClient(client.getClientImpl());
                        return job;
                    }
                }
            } catch (BeanstalkException e) {
                BeanstalkExceptionType etype = e.getReason();
                if (etype == BeanstalkExceptionType.DEADLINESOON) {
                    throw e;
                } else {
                    client.resetClientImpl();
                }
            }
            sleep(sleepMillis);
        } while (strategy.tryNextServer());

        return job;
    }

    /**
     * The consumer delete cmd. Consumer use this cmd to detete the job from the beanstalk service. Only
     * delete the job in the dedicated instance according the server index and the jobid which are kept in Job
     * structure.
     *
     * @param job the job will be deleted.
     * @return the result of deleting.
     * @throws IllegalArgumentException
     */
    public boolean delete(Job job) throws IllegalArgumentException {
        return this.delete(job.getServerIndex(), job.getJobId());
    }

    /**
     * The consumer delete cmd. Consumer use this cmd to detete the job from the beanstalk service. Only
     * delete the job in the dedicated instance according the server index and the jobid which are kept in Job
     * structure.
     *
     * @param serverIdx the server index where the job come from
     * @param jobId the job id which will be deleted
     * @return the result of deleting.
     * @throws IllegalArgumentException
     */
    public boolean delete(int serverIdx, long jobId) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx >= hosts.size()) {
            throw new IllegalArgumentException("invalid serverIdx");
        }
        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().delete(jobId);
            } else {
                return false;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return false;
    }

    /**
     * The consumer release cmd. Consumer use this cmd to release the job from the beanstalk service. Only
     * release the job in the dedicated instance according the server index and the jobid which are kept in
     * Job structure. This cmd use the DEFAULT_PRIORITY and the DEFAULT_PRIORITY as the new parameter for the
     * job.
     *
     * @param job the job will be released.s
     * @return the result of releasing.
     * @throws IllegalArgumentException
     */
    public boolean release(Job job) throws IllegalArgumentException {
        return this.release(job, JobImpl.DEFAULT_PRIORITY, JobImpl.DEFAULT_TTR);
    }

    /**
     * The consumer release cmd. Consumer use this cmd to release the job from the beanstalk service. Only
     * release the job in the dedicated instance according the server index and the jobid which are kept in
     * Job structure.
     *
     * @param job the job will be released.
     * @param priority the new job priority will be release to the beanstalk service.
     * @param delaySeconds the new job delayed time will be release to the beanstalk service.
     * @return the result of releasing.
     * @throws IllegalArgumentException
     */
    public boolean release(Job job, long priority, int delaySeconds) throws IllegalArgumentException {
        return this.release(job.getServerIndex(), job.getJobId(), priority, delaySeconds);
    }

    /**
     * The consumer release cmd. Consumer use this cmd to release the job from the beanstalk service. Only
     * release the job in the dedicated instance according the server index and the jobid which are kept in
     * Job structure.
     *
     * @param serverIdx the server index where the job come from
     * @param jobId the job id which will be released
     * @param priority the new job priority will be release to the beanstalk service.
     * @param delaySeconds the new job delayed time will be release to the beanstalk service.
     * @return the result of releasing.
     * @throws IllegalArgumentException
     */
    public boolean release(int serverIdx, long jobId, long priority, int delaySeconds) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx >= hosts.size()) {
            throw new IllegalArgumentException("invalid serverIdx");
        }
        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().release(jobId, priority, delaySeconds);
            } else {
                return false;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return false;
    }

    /**
     * The consumer bury cmd. Consumer use this cmd to bury the job into the buried list, will not be reserved
     * any more, unless the job is kicked out from the buried list again.
     *
     * @param job the job will be buried.
     * @param priority the new job priority will be buried to the beanstalk service.
     * @return the result of burying.
     * @throws IllegalArgumentException
     */
    public boolean bury(Job job, long priority) throws IllegalArgumentException {
        return this.bury(job.getServerIndex(), job.getJobId(), priority);
    }

    /**
     * The consumer bury cmd. Consumer use this cmd to bury the job into the buried list, will not be reserved
     * any more, unless the job is kicked out from the buried list again.
     *
     * @param serverIdx the server index where the job come from
     * @param jobId the job id which will be buried
     * @param priority the new job priority will be buried to the beanstalk service.
     * @return the result of burying.
     * @throws IllegalArgumentException
     */
    public boolean bury(int serverIdx, long jobId, long priority) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx >= hosts.size()) {
            throw new IllegalArgumentException("invalid serverIdx");
        }
        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().bury(jobId, priority);
            } else {
                return false;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return false;
    }

    /**
     * The consumer touch cmd. Consumer use this cmd to earn more time for consuming..
     *
     * @param job the job will be touched.
     * @return the result of touching.
     * @throws IllegalArgumentException
     */
    public boolean touch(Job job) throws IllegalArgumentException {
        return this.touch(job.getServerIndex(), job.getJobId());
    }
    /**
     * The consumer touch cmd. Consumer use this cmd to earn more time for consuming..
     *
     * @param serverIdx the server index where the job come from
     * @param jobId the job id which will be buried
     * @return the result of touching.
     * @throws IllegalArgumentException
     */
    public boolean touch(int serverIdx, long jobId) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx >= hosts.size()) {
            throw new IllegalArgumentException("invalid serverIdx");
        }
        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().touch(jobId);
            } else {
                return false;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return false;
    }

    /**
     * The consumer peek cmd. Consumer use this cmd to peek a specific job from the dedicated beanstalck
     * instance.
     *
     * @param serverIdx the beanstalck instance offset in the Host Arraylist, from which the job will be
     * peeked.
     * @param jobId the job's id, which will be peeked out from the service.
     * @return the peeked job. this is just a job copy, the orignal job can still be reserved by other client.
     * @throws IllegalArgumentException
     */
    public Job peek(int serverIdx, long jobId) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx > hosts.size()) {
            throw new IllegalArgumentException("invalid server index");
        }
        if (jobId < 0) {
            throw new IllegalArgumentException("invalid jobid");
        }
        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().peek(jobId);
            } else {
                return null;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return null;
    }

    /**
     * The consumer peek-buried cmd. Consumer use this cmd to peek a specific job from the dedicated buried
     * list in the watched tubes .
     *
     * @return the peeked job. this is just a job copy, the orignal job is still in the buried list.
     */
    public Job peekBuried() {
        Job job;
        do {
            int server_idx = (int) Math.abs(this.strategyPeekBuried.shard());
            Client client = hosts.get(server_idx);
            try {
                if (client.getClientImpl() != null) {
                    job = client.getClientImpl().peekBuried();
                    if (job != null) {
                        job.setServerIndex(server_idx);
                        job.setClient(client.getClientImpl());
                        break;
                    }
                }
            } catch (BeanstalkException e) {
                client.resetClientImpl();
            }

        } while (this.strategyPeekBuried.tryNextServer());
        return null;
    }

    /**
     * The consumer peek-delayed cmd. Consumer use this cmd to peek a specific job from the dedicated delayed
     * queue in the watched tubes .
     *
     * @return the peeked job. this is just a job copy, the orignal job is still in the delayed queue.
     */
    public Job peekDelayed() {
        Job job;
        do {
            int server_idx = (int) Math.abs(this.strategyPeekDelayed.shard());
            Client client = hosts.get(server_idx);
            try {
                if (client.getClientImpl() != null) {
                    job = client.getClientImpl().peekDelayed();
                    if (job != null) {
                        job.setServerIndex(server_idx);
                        job.setClient(client.getClientImpl());
                        break;
                    }
                }
            } catch (BeanstalkException e) {
                client.resetClientImpl();
            }

        } while (this.strategyPeekDelayed.tryNextServer());
        return null;
    }

    /**
     * The consumer peek-ready cmd. Consumer use this cmd to peek a specific job from the dedicated ready
     * queue in the watched tubes .
     *
     * @return the peeked job. this is just a job copy, the orignal job is still in the ready queue.
     */
    public Job peekReady() {
        Job job;
        do {
            int server_idx = (int) Math.abs(this.strategyPeekReady.shard());
            Client client = hosts.get(server_idx);
            try {
                if (client.getClientImpl() != null) {
                    job = client.getClientImpl().peekReady();
                    if (job != null) {
                        job.setServerIndex(server_idx);
                        job.setClient(client.getClientImpl());
                        return job;
                    }
                }
            } catch (BeanstalkException e) {
                client.resetClientImpl();
            }

        } while (this.strategyPeekReady.tryNextServer());
        return null;
    }

    /**
     * The consumer kick cmd. Consumer use this cmd to kick some job from the delayed queue. the proxy iterate
     * all the instance and kick out the 'count' number jobs from each.
     *
     * @param count the count of the jobs will be kick out from the instance.
     */
    public void kick(int count) {
        do {
            int server_idx = (int) Math.abs(this.strategyKick.shard());
            Client client = hosts.get(server_idx);
            try {
                if (client.getClientImpl() != null) {
                    client.getClientImpl().kick(count);
                }
            } catch (BeanstalkException e) {
                client.resetClientImpl();
            }

        } while (this.strategyKick.tryNextServer());
    }

    /**
     * The consumer kick-job cmd. Consumer use this cmd to kick the specific job from the specific beanstalck
     * instance.
     *
     * @param serverIdx the specific server
     * @param jobId the specific job
     * @return true if successfully executed, else false
     * @throws IllegalArgumentException
     */
    public boolean kickJob(int serverIdx, long jobId) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx > hosts.size()) {
            throw new IllegalArgumentException("invalid server index");
        }
        if (jobId < 0) {
            throw new IllegalArgumentException("invalid job id");
        }
        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().kickJob(jobId);
            } else {
                return false;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return false;
    }

    /**
     * The stats-job cmd. program can use this get the specific job's statss.
     *
     * @param serverIdx the server where the job is in.
     * @param jobId the job id which will be stated.
     * @return the stats of the job.
     * @throws IllegalArgumentException
     */
    public Map<String, String> statsJob(int serverIdx, long jobId) throws IllegalArgumentException {
        if (serverIdx < 0 || serverIdx > hosts.size()) {
            throw new IllegalArgumentException("invalid server index");
        }
        if (jobId < 0) {
            throw new IllegalArgumentException("invalid job id");
        }

        Client client = hosts.get(serverIdx);
        try {
            if (client.getClientImpl() != null) {
                return client.getClientImpl().statsJob(jobId);
            } else {
                return null;
            }
        } catch (Exception e) {
            client.resetClientImpl();
        }
        return null;
    }

    /**
     * The stats-tube cmd. get the tube stats from all the beanstalk instances.
     *
     * @param tubeName the tube name which will be stats
     * @return the stats of the job.
     * @throws IllegalArgumentException
     */
    public Map<String, Map<String, String>> statsTube(String tubeName) throws IllegalArgumentException {
        if (tubeName == null) {
            throw new IllegalArgumentException("invalid tube name");
        }
        Map<String, Map<String, String>> statsMap = new HashMap<String, Map<String, String>>();
        for (Client client : hosts) {
            Map<String, String> stats = null;
            try {
                if (client.getClientImpl() != null) {
                    stats = client.getClientImpl().statsTube(tubeName);
                }
            } catch (Exception e) {
                client.resetClientImpl();
            }
            statsMap.put(client.getServerIp() + ":" + client.getServerPort(), stats);
        }
        return statsMap;
    }

    /**
     * The stats cmd. get the stats of the servers.
     *
     * @return the stats of all the servers.
     */
    public Map<String, Map<String, String>> stats() {
        Map<String, Map<String, String>> statsMap = new HashMap<String, Map<String, String>>();
        for (Client client : hosts) {
            Map<String, String> stats = null;
            try {
                if (client.getClientImpl() != null) {
                    stats = client.getClientImpl().stats();
                }
            } catch (Exception e) {
                client.resetClientImpl();
            }
            statsMap.put(client.getServerIp() + ":" + client.getServerPort(), stats);
        }
        return statsMap;
    }

    /**
     * The list-tubes cmd. get the all the tubes in the server.
     *
     * @return the tube of all the servers.
     */
    public Set<String> listTubes() {
        Set<String> tubeSet = new HashSet<String>();
        for (Client client : hosts) {
            List<String> tubes = null;
            try {
                if (client.getClientImpl() != null) {
                    tubes = client.getClientImpl().listTubes();
                }
            } catch (Exception e) {
                client.resetClientImpl();
            }
            if (tubes != null) {
                tubeSet.addAll(tubes);
            }
        }
        return tubeSet;
    }

    /**
     * The list-tube-used cmd. get the currently used tube.
     *
     * @return the currently used tube of the client.
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public String listTubeUsed() throws BeanstalkException {
        for (Client client : hosts) {
            try {
                String tube = client.getUsedTubes();
                if (tube != null) {
                    return tube;
                }
                if (client.getClientImpl() != null) {
                    tube = client.getClientImpl().listTubeUsed();
                    if (tube != null) {
                        return tube;
                    }
                }
            } catch (BeanstalkException e) {
                client.resetClientImpl();
            }
        }
        throw new BeanstalkException(BeanstalkExceptionType.ALLFAILD);
    }

    /**
     * The list-tube-watched cmd. get all the currently watched tube.
     *
     * @return the currently watched tube of the client.
     * @throws com.surftools.BeanstalkClient.BeanstalkException
     */
    public Set<String> listTubesWatched() throws BeanstalkException {
        int tryTimes = 2;
        Set<String> tubes = new HashSet<String>();
        for (Client client : hosts) {
            while (tryTimes-- > 0) {
                try {
                    if (client.getClientImpl() != null) {
                        tubes.addAll(client.getClientImpl().listTubesWatched());
                        tryTimes = 0;
                    }
                } catch (BeanstalkException e) {
                    // maybe there is io error
                    client.resetClientImpl();
                }
            }
        }
        if (tubes.isEmpty()) {
            throw new BeanstalkException(BeanstalkExceptionType.ALLFAILD);
        }
        return tubes;
    }

    /**
     * The pause-tube cmd. pause the tube, will not get any job from the tube.
     *
     * @param tubeName the tube will be paused
     * @param pauseSeconds the paused time of the paused tube
     */
    public void pauseTube(String tubeName, int pauseSeconds) {
        int tryTimes = 2;
        for (Client client : hosts) {
            while (tryTimes-- > 0) {
                try {
                    if (client.getClientImpl() != null) {
                        client.getClientImpl().pauseTube(tubeName, pauseSeconds);
                        tryTimes = 0;
                    }
                } catch (Exception e) {
                    client.resetClientImpl();
                }
            }
        }
    }

    /**
     * The upper level of the Beanstalk client. Mainly perform the connection operation, like connection or
     * reconnection.
     */
    private static class Client {

        private static final boolean uniqueConnectionPerThread = false;
        private final String serverIp;
        private final Integer serverPort;
        private final Integer connectTimeout;
        private com.surftools.BeanstalkClient.Client clientImpl = null;
        private String usedTubes = null;
        private List<String> watchedTubes = null;
        private Class<?> beanstalkImplClass = null;
        private Constructor<?> beanstalkClientConstructor = null;

        public Client(Class<?> beanstalkImplClass, String ip, int port, int connectTimeout) throws BeanstalkException {
            this.beanstalkImplClass = beanstalkImplClass;
            this.serverIp = ip;
            this.serverPort = port;
            this.connectTimeout = connectTimeout;

            try {
                beanstalkClientConstructor = this.beanstalkImplClass
                        .getConstructor(String.class, Integer.class, Integer.class);
            } catch (Exception ex) {
                throw new BeanstalkException(BeanstalkExceptionType.INVALIDIMPL, ex.getMessage());
            }
            this.clientImpl = initConnection();
        }

        public String getServerIp() {
            return serverIp;
        }

        public int getServerPort() {
            return serverPort;
        }

        public void release() {
            resetClientImpl();
            if (this.watchedTubes != null) {
                watchedTubes.clear();
            }
        }

        public void resetClientImpl() {
            if (this.clientImpl != null) {
                try {
                    this.clientImpl.close();
                } catch (Exception e) {
                    //encounter exception when close handler
                }
                this.clientImpl = null;
            }
        }

        public com.surftools.BeanstalkClient.Client getClientImpl() {
            if (this.clientImpl == null) {
                this.clientImpl = initConnection();
            }
            return clientImpl;
        }

        public void setUsedTube(String usedTubes) {
            if (usedTubes == null) {
                throw new BeanstalkException(BeanstalkExceptionType.NULL);
            }
            this.usedTubes = usedTubes;
        }

        public String getUsedTubes() {
            return usedTubes;
        }

        public List<String> getWatchedTubes() {
            return watchedTubes;
        }

        public void addWatchedTubes(String watchedTube) {
            if (watchedTube == null) {
                throw new BeanstalkException(BeanstalkExceptionType.NULL);
            }
            if (this.watchedTubes == null) {
                this.watchedTubes = new ArrayList<String>();
            }
            this.watchedTubes.add(watchedTube);
        }

        public void removeWatchedTubes(String watchedTube) {
            if (watchedTube == null) {
                throw new BeanstalkException(BeanstalkExceptionType.NULL);
            }
            if (this.watchedTubes == null) {
                this.watchedTubes = new ArrayList<String>();
            }
            this.watchedTubes.remove(watchedTube);
        }

        public void setWatchedTubes(List<String> watchedTubes) {
            if (watchedTubes == null) {
                throw new BeanstalkException(BeanstalkExceptionType.NULL);
            }
            this.watchedTubes = watchedTubes;
        }

        private com.surftools.BeanstalkClient.Client initConnection() {
            com.surftools.BeanstalkClient.Client conn;

            try {
                conn = (com.surftools.BeanstalkClient.Client) this.beanstalkClientConstructor
                        .newInstance(this.serverIp, this.serverPort, this.connectTimeout);
                conn.setUniqueConnectionPerThread(uniqueConnectionPerThread);
                // if usedTubes and watchedTubes is not null, then it's a reconnection
                // reset the usedtubes and the watchedtubes after the reconnection
                if (this.usedTubes != null) {
                    conn.useTube(this.usedTubes);
                }
                // this maybe a reconnect, rewatch the previous wathed tubes, ignore the default watched tubes which is 'default'
                if (this.watchedTubes != null) {
                    // ignore the 'default' tube
                    List<String> list = conn.listTubesWatched();
                    for (String tube : watchedTubes) {
                        conn.watch(tube);
                        list.remove(tube);
                    }
                    for (String existingTubeName : list) {
                        conn.ignore(existingTubeName);
                    }
                } else {
                    // get the already watched tubes
                    this.watchedTubes = conn.listTubesWatched();
                }

                logger.log(Level.INFO, "connect server successfully: {0}:{1}",
                        new Object[]{this.serverIp, Integer.toString(this.serverPort)});
            } catch (Exception e) {
                // connect  to the server or set tube failed, will retry later
                conn = null;
                logger.log(Level.WARNING, "connect server failed: {0}:{1}",
                        new Object[]{this.serverIp, Integer.toString(this.serverPort)});
            }
            return conn;
        }
    }

    public static class JobBuilder {

        private byte[] data = null;

        private long priority = JobImpl.DEFAULT_PRIORITY;
        private int delaySeconds = 0;
        private int timeToRun = JobImpl.DEFAULT_TTR;
        private DistributeStrategy strategy = null;

        public JobBuilder(byte[] data) {
            this.data = data;
        }

        public JobBuilder priority(long priority) {
            this.priority = priority;
            return this;
        }

        public JobBuilder delaySeconds(int delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        public JobBuilder timeToRun(int timeToRun) {
            this.timeToRun = timeToRun;
            return this;
        }

        public JobBuilder strategy(DistributeStrategy strategy) {
            this.strategy = strategy;
            return this;
        }

        public byte[] getData() {
            return data;
        }

        public long getPriority() {
            return priority;
        }

        public int getDelaySeconds() {
            return delaySeconds;
        }

        public int getTimeToRun() {
            return timeToRun;
        }

        public DistributeStrategy getStrategy() {
            return strategy;
        }
    }

    public static class RoundRobinStrategy implements DistributeStrategy {

        private int serverCount = 0;
        private int currentIndex = 0;
        private int maxRetryTimes = 0;
        private boolean loopForever = false;

        public RoundRobinStrategy(int serverCount) {
            this(serverCount, false);
        }

        public RoundRobinStrategy(int serverCount, boolean loopForever) {
            if (serverCount <= 0) {
                throw new BeanstalkException(BeanstalkExceptionType.ADDRESS);
            }
            this.serverCount = serverCount;
            this.loopForever = loopForever;
            this.maxRetryTimes = this.serverCount;
        }

        @Override
        public void reset() {
            this.maxRetryTimes = this.serverCount;
        }

        @Override
        public int shard() {
            int shard = this.currentIndex++ % this.serverCount;
            if (this.currentIndex >= this.serverCount) {
                this.currentIndex = this.currentIndex % this.serverCount;
            }
            return shard;
        }

        @Override
        public boolean tryNextServer() {
            if (this.loopForever == true) {
                return true;
            }
            return --this.maxRetryTimes > 0;
        }

    }

    public static class ConsistentHashShardingStrategy implements DistributeStrategy {

        private static final int MAX_TRY_TIMES = 3;
        private static final int DEFAULT_VNODE_NUMS = 128;
        private static final long CIRCLE_SPACE = 4294967296L;
        private int vnodeNums = DEFAULT_VNODE_NUMS;
        private int vnodeDistance = (int) CIRCLE_SPACE / vnodeNums;
        private int currentIndex = 0;
        private int maxTryTimes = MAX_TRY_TIMES;
        public SortedMap<Integer, Integer> consitentHashCircle = new TreeMap<Integer, Integer>();
        public Integer[] newConsitentHashArray = null;

        private MessageDigest md5Digest = null;

        public ConsistentHashShardingStrategy(ArrayList<String> servers) {
            this(servers, DEFAULT_VNODE_NUMS);
        }

        public ConsistentHashShardingStrategy(ArrayList<String> servers, int vnode_nums) {
            this.vnodeNums = vnode_nums;
            if (this.vnodeNums * servers.size() < MAX_TRY_TIMES) {
                throw new IllegalArgumentException("invalid server host and vnode nums");
            }
            this.vnodeDistance = (int) (CIRCLE_SPACE / this.vnodeNums);

            try {
                md5Digest = (MessageDigest) MessageDigest.getInstance("MD5").clone();
            } catch (Exception e) {
            }

            for (int index = 0; index < servers.size(); ++index) {
                this.add(servers.get(index), index);
            }
        }

        private int md5sum(String data) {
            md5Digest.reset();
            byte[] digest = md5Digest.digest(data.getBytes());
            return (((digest[15] << 24) & 0xFF000000)
                    | ((digest[14] << 16) & 0x00FF0000)
                    | ((digest[13] << 8) & 0x0000FF00)
                    | (digest[12] & 0x000000FF));
        }

        private void add(String endpoint, int index) {
            int endpointHash = md5sum(endpoint);
            int startPoint = endpointHash;
            for (int i = 0; i < this.vnodeNums; ++i) {
                startPoint += this.vnodeDistance;
                consitentHashCircle.put(startPoint, index);
            }
        }

        private void remove(String endpoint) {
            int endpointHash = md5sum(endpoint);
            int startPoint = endpointHash;
            for (int i = 0; i < this.vnodeNums; ++i) {
                startPoint += this.vnodeDistance;
                consitentHashCircle.remove(startPoint);
            }
        }

        public Integer[] hashKey(String key) throws Exception {
            if (key == null) {
                throw new IllegalArgumentException("invalid key");
            }
            this.newConsitentHashArray = null;
            int hash = md5sum(key);
            SortedMap<Integer, Integer> tailedMap = null;
            SortedMap<Integer, Integer> headedMap = null;

            try {
                tailedMap = consitentHashCircle.tailMap(hash);
                if (tailedMap.size() < MAX_TRY_TIMES) {
                    headedMap = consitentHashCircle.headMap(hash);
                }
            } catch (Exception e) {
                throw e;
            }

            this.newConsitentHashArray = new Integer[MAX_TRY_TIMES];
            int readyCount = 0;
            for (Integer index : tailedMap.values()) {
                this.newConsitentHashArray[readyCount++] = index;
                if (readyCount == MAX_TRY_TIMES) {
                    break;
                }
            }
            // in the end of the circle, append the start node of the circle
            if (readyCount < MAX_TRY_TIMES) {
                if (headedMap != null) {
                    for (Integer index : headedMap.values()) {
                        this.newConsitentHashArray[readyCount++] = index;
                        if (readyCount == MAX_TRY_TIMES) {
                            break;
                        }
                    }
                }
            }
            reset();
            return this.newConsitentHashArray;
        }

        @Override
        public int shard() {
            if (this.newConsitentHashArray == null) {
                throw new IllegalArgumentException("invalid hashes");
            }
            return this.newConsitentHashArray[this.currentIndex++];
        }

        @Override
        public boolean tryNextServer() {
            boolean res = this.currentIndex < this.maxTryTimes;
            if (res == false) {
                this.currentIndex %= this.maxTryTimes;
            }
            return res;
        }

        @Override
        public void reset() {
            this.currentIndex = 0;
            this.maxTryTimes = MAX_TRY_TIMES;
        }
    }
}
