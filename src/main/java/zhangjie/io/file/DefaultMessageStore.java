package zhangjie.io.file;

import zhangjie.io.common.ServiceThread;
import zhangjie.io.util.StorePathConfigHelper;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @Author zhangjie
 * @Date 2020/6/27 18:08
 **/
public class DefaultMessageStore {
    private final MessageStoreConfig messageStoreConfig;
    private final AllocateMappedFileService allocateMappedFileService;
    private final ReputMessageService reputMessageService;
    private final CommitLog commitLog;

    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    public DefaultMessageStore(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.allocateMappedFileService =  new AllocateMappedFileService();
        this.allocateMappedFileService.start();

        this.commitLog = new CommitLog(this);
        this.commitLog.start();

        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.reputMessageService = new ReputMessageService();

    }

    public boolean load() {
        boolean result = true;

        this.reputMessageService.start();
        // load Commit Log
        result = this.commitLog.load();
        // load Consume Queue
        result = result && this.loadConsumeQueue();
        return  result;
    }


    private boolean loadConsumeQueue() {
        //创建consummer文件夹
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

        }
        System.out.println("load logics queue all over, OK");
        return true;
    }



    public void start() throws Exception {
        long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
    }

    public boolean putMessage(String msg){
        return this.commitLog.putMessage(msg);
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    class ReputMessageService extends ServiceThread {
        //ReputMessageService从哪个物理偏移量开始转发消息给ConsumeQueue和Index
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

        @Override
        public void run() {
            System.out.println(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception. "+ e);
                }
            }
            System.out.println(this.getServiceName() + " service end");
        }

        private void doReput() {

        }
    }


}
