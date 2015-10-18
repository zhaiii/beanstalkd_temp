//package com.ishansong.beanstalkd;
//
//import com.trendrr.beanstalk.BeanstalkClient;
//import com.trendrr.beanstalk.BeanstalkException;
//import com.trendrr.beanstalk.BeanstalkJob;
//
///**
// * Created by zhai on 15/10/14.
// */
//public class BeanstalkdClient {
//
//    private final BeanstalkdExecutor executor;
//    public BeanstalkdClient(String host,Integer port,String tube){
//        executor=new BeanstalkdExecutor(host,port,tube);
//    }
//    public BeanstalkdClient(String host,Integer port,Integer maxPoolSize,String tube){
//
//        executor=new BeanstalkdExecutor(host,port,maxPoolSize,tube);
//    }
//    public BeanstalkdClient(String host,Integer port){
//        executor=new BeanstalkdExecutor(host,port);
//    }
//
//    /**
//     * Puts a task into the currently used queue
//     * @param priority  优先级
//     * @param delay     延迟推送task到ready中的时间
//     * @param ttr       job运行的时间，超出这个时间job会被relese
//     * @param bytes     job中的数据
//     * @return          插入到job的id
//     */
//    public Long put(final Long priority, final Integer delay,final Integer ttr, final byte[] bytes) {
//        return executor.execute(new BeanstalkdExecutor.ActionResult<Long>(){
//            @Override
//            public Long action(BeanstalkClient client){
//                return client.put(priority,delay,ttr,bytes);
//            }
//        });
//    }
//    /**
//     * 指定使用那个tube
//     * @param tube  管道名
//     */
//    public void useTube(final String tube){
//        executor.execute(new BeanstalkdExecutor.ActionVoid(){
//            @Override
//            public void doAction(BeanstalkClient client) {
//                client.useTube(tube);
//            }
//        });
//    }
//    /**
//     * 拿到优先级最高的job
//     * @param timeoutSeconds 等待job的时间
//     * @return               优先级最高的job，或null
//     */
//    public BeanstalkJob reserve(final Integer timeoutSeconds){
//        return executor.execute(new BeanstalkdExecutor.ActionResult<BeanstalkJob>(){
//
//            @Override
//            public BeanstalkJob action(BeanstalkClient client) {
//
//                return client.reserve(timeoutSeconds);
//            }
//        });
//    }
//    /**
//     * 查看管道状态
//     * @return
//     */
//    public String tubeStats(){
//        return executor.execute(new BeanstalkdExecutor.ActionResult<String>(){
//            @Override
//            public String action(BeanstalkClient client){
//              return client.tubeStats();
//            }
//        });
//    }
//
//}
