package test;

import com.ishansong.mqapi.IMqClient;
import com.ishansong.mqapi.IMqClientFactory;
import com.ishansong.mqapi.MqMessage;
import org.junit.Test;

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
        MqMessage reserve = mqClient.reserve(23);
        System.out.println(new String(reserve.getData()));

        IMqClient cleint = reserve.getCleint();
        String s = cleint.tubeStats();
        System.out.println(s);
//        String s1 = mqClient.tubeStats();
//        System.out.println(s1);

    }
}
