package com.ishansong.beanstalkd;

import com.sun.media.jfxmedia.logging.Logger;
import com.trendrr.beanstalk.BeanstalkClient;
import com.trendrr.beanstalk.BeanstalkDisconnectedException;
import com.trendrr.beanstalk.BeanstalkException;
import com.trendrr.beanstalk.BeanstalkPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by zhai on 15/10/14.
 */
public class BeanstalkdExecutor {

    private Log log = LogFactory.getLog(BeanstalkdExecutor.class);

    private final BeanstalkPool pool;

    public BeanstalkdExecutor(String addr,Integer port){
        pool=new BeanstalkPool(addr,port,30);
    }

    public BeanstalkdExecutor(String addr,Integer port,String tube){
        pool=new BeanstalkPool(addr,port,30,tube);
    }
    public BeanstalkdExecutor(String addr,Integer port,Integer maxPoolSize,String tube){
        pool=new BeanstalkPool(addr,port,maxPoolSize,tube);
    }

    /**
     * Excete a action
     * @param action   a void action
     * @param <T>
     * @return
     * @throws BeanstalkException
     */
    public <T> T execute(ActionResult<T> action) throws BeanstalkException{
        BeanstalkClient client=null;
        try {
            client = pool.getClient();
            return action.action(client);
        }catch (BeanstalkDisconnectedException e){
            log.error("beanstalk connection error:{}",e);
            throw e;
        }finally {
            if(client != null){
                client.close();
            }
        }
    }
    

    public void execute(ActionVoid action) throws BeanstalkException{
        BeanstalkClient client=null;
        try{
            client=pool.getClient();
            action.doAction(client);
        }catch (BeanstalkDisconnectedException e){
            log.error("beanstalk connection lost",e);
            throw e;
        }finally {
            if(client != null){
                client.close();
            }
        }

    }

    /**
     * An interface do a redis operation with T result
     */
    public interface ActionResult<T> {
        T action(BeanstalkClient client);
    }

    /**
     * An interface do a redis operation without result
     */
    public interface ActionVoid {
        void doAction(BeanstalkClient client);
    }
}

