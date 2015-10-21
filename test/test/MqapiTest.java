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

        IMqClient mqClient = IMqClientFactory.newInstance().getMqClient(
                "192.168.254.129", 11300);
        //String tube = "hello";
        //mqClient.useTube(tube);
        long msgid = mqClient.put(2, 0, 30, "haha".getBytes());
        System.out.println(msgid);

//        MqMessage reserve = mqClient.reserve(80);
//        IMqClient cleint = reserve.getCleint();
//        String s = cleint.tubeStats();
//        System.out.println(s);
//        cleint.watchTube("default");
        MqMessage msg = mqClient.reserve(23);

        System.out.println(new String(msg.getData()));

        IMqClient cleint = msg.getCleint();
        String s = cleint.tubeStats();
        System.out.println(s);
//        String s1 = mqClient.tubeStats();
//        System.out.println(s1);

    }
}
