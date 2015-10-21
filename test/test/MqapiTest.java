package test;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.IMqClientFactory;
import com.ishansong.mqapi.MqMessage;
import org.junit.Test;
import trendrr.beanstalk.BeanstalkJob;

/**
 * Created by zhai on 15/10/21.
 */
public class MqapiTest {

    @Test
    public void test() throws Exception{

        IMqClient mqClient = IMqClientFactory.newInstance().getMqClient("127.0.0.1", 11300);
        long tube = mqClient.put(2, 0, 30, "haha".getBytes());
        System.out.println(tube);

//        MqMessage reserve = mqClient.reserve(80);
//        IMqClient cleint = reserve.getCleint();
//        String s = cleint.tubeStats();
//        System.out.println(s);
//        cleint.watchTube("default");
        MqMessage message = mqClient.reserve(23);
        System.out.println(new String(message.getData()));

        IMqClient cleint = message.getCleint();
        String s = cleint.tubeStats();
        System.out.println(s);
//        String s1 = mqClient.tubeStats();
//        System.out.println(s1);

    }
    @Test
    public void test1() throws Exception{
        IMqClient mqClient = IMqClientFactory.newInstance().getMqClient("127.0.0.1", 11300);
        mqClient.put(1l, 0, 5000, "this is some data".getBytes());
        MqMessage reserve = mqClient.reserve(60);
        System.out.println(new String(reserve.getData()));
        mqClient.deleteJob(reserve.getId());
        mqClient.close(); //closes the connection
    }

    @Test
    public void test2() throws Exception{
        IMqClient mqClient = IMqClientFactory.newInstance().getPooledMqClient("127.0.0.1", 11300, 10);
        long put = mqClient.put(23, 0, 90, "gagag".getBytes());
        System.out.println(put);
        MqMessage mess = mqClient.reserve(90);
        IMqClient cleint = mess.getCleint();
        String s = cleint.tubeStats();
        System.out.println(s);

    }
}
