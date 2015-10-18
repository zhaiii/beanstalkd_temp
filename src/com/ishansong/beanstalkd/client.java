package com.ishansong.beanstalkd;

import com.trendrr.beanstalk.BeanstalkClient;
import com.trendrr.beanstalk.BeanstalkJob;
import com.trendrr.beanstalk.BeanstalkPool;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * 基于trendrr的beanstalkd的客户端
 * Created by zhai on 15/10/13.
 */
public class client {

    @Test
    public void poolClient() throws Exception{
        BeanstalkPool pool = new BeanstalkPool("localhost", 8010,
                30, //poolsize
                "example" //tube to use
        );

        BeanstalkClient client = pool.getClient();

    }

    @Test
    public void test() throws Exception{

        BeanstalkClient client = new BeanstalkClient("localhost", 11300, "example");
        //producer,通过put命令将job放在一个tube中去
        client.put(1l, 0, 5000, "this is some data".getBytes());

        client.useTube("zhai");
        //设置delay时间按，job被put到ready queue，
        client.put(2l,12,5000,"tube zhai".getBytes());

        //consumer，获取job或是改变job的状态
        //调用reserve方法，总可以拿到优先级最高的命令，时间复杂度为
        BeanstalkJob job = client.reserve(60);//time-out 代表等待job的时间
        String s = new String(job.getData());
        System.out.println(s);

        BeanstalkClient client1 = job.getClient();
        System.out.println(client1.tubeStats());
        client1.release(job,23,23);//当job执行完成后，在释放到delay或ready

        client.bury(job,23);


        client.deleteJob(job);
        client.close(); //closes the connection

    }

    @Test
    public void test2(){
        BigDecimal divide = new BigDecimal(32010).divide(new BigDecimal(100)).setScale(2);
        System.out.println(divide.toString());

    }

}
