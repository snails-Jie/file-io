package zhangjie.io.file;

import java.io.File;

/**
 * @Author zhangjie
 * @Date 2020/6/28 9:48
 **/
public class ConsumeQueue {
    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final String storePath;

    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize){
        this.storePath = storePath;
        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
    }
}
