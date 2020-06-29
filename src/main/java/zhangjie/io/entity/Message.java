package zhangjie.io.entity;

/**
 * @Author zhangjie
 * @Date 2020/6/27 10:53
 **/
public class Message {
    private String topic;
    private int queueId;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}
