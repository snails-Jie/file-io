package zhangjie.io.queue;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * @Author zhangjie
 * @Date 2020/6/27 17:25
 **/
public class PriorityBlockQueueTest {

    @Test
    public void testDemo(){
        PriorityBlockingQueue<User> queue = new PriorityBlockingQueue<User>();
        for(int i=0; i<12; i++){
            User user = new User();
            int max=20;
            int min=10;
            Random random = new Random();

            int n = random.nextInt(max)%(max-min+1) + min;

            user.setPriority(n);
            user.setUsername("李艳第"+i+"天");

            queue.add(user);
        }

        for(int i=0; i<12; i++){
            User u = queue.poll();
            System.out.println("优先级是："+u.getPriority()+","+u.getUsername());
        }
    }

    @Test
    public void testIsBlock(){
        PriorityBlockingQueue<User> queue = new PriorityBlockingQueue<User>();
        queue.offer(new User());
        try {
            //如果队列为空，take操作会阻塞
            User user =  queue.take();
            System.out.println(user.getUsername());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
