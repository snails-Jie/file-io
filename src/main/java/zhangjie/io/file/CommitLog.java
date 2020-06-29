package zhangjie.io.file;

import zhangjie.io.common.ServiceThread;
import zhangjie.io.constant.AppendMessageStatus;
import zhangjie.io.constant.FlushDiskType;
import zhangjie.io.constant.PutMessageStatus;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author zhangjie
 * @Date 2020/6/26 8:16
 **/
public class CommitLog {
    private ReentrantLock putMessageNormalLock = new ReentrantLock();
    protected final MappedFileQueue mappedFileQueue;
    private final AppendMessageCallback appendMessageCallback;
    protected final DefaultMessageStore defaultMessageStore;
    private final FlushCommitLogService flushCommitLogService;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;

        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                                            defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                                            defaultMessageStore.getAllocateMappedFileService() );
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

        this.flushCommitLogService = new GroupCommitService();
    }

    public boolean load(){
        boolean result = this.mappedFileQueue.load();
        System.out.println("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushCommitLogService.start();
    }


    public boolean putMessage(String message){
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        AppendMessageResult result = null;
        putMessageNormalLock.lock();
        try{
           if (null == mappedFile) {
               mappedFile = this.mappedFileQueue.getLastMappedFile(0,true);
           }
            result = mappedFile.appendMessageInner(message,appendMessageCallback);
        }finally {
            putMessageNormalLock.unlock();
        }

        //触发刷盘操作
        handleDiskFlush(result);
        return true;
    }

    public void handleDiskFlush(AppendMessageResult result){
        // Synchronization flush
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                                                                    this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
            service.putRequest(request);
            CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
            PutMessageStatus flushStatus = null;
            try {
                flushStatus = flushOkFuture.get(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(),
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                //flushOK=false;
            }
            if (flushStatus != PutMessageStatus.PUT_OK) {
                System.out.println("do groupcommit, wait for flush failed ");
            }
        }

    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset();
        }
        return -1;
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class GroupCommitService extends FlushCommitLogService {

        //提供读容器与写容器，避免同步刷盘消费任务与其他消息提交任务直接的锁竞争
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public void run() {
            System.out.println(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    this.swapRequests();
                    this.doCommit();
                } catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception. "+ e);
                }
            }
        }
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        if (!flushOK) {
                            CommitLog.this.mappedFileQueue.flush(0);
                        }

                        req.wakeupCustomer(flushOK);
                    }
                }
            }
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long startTimestamp = System.currentTimeMillis();
        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }
        public long getNextOffset() {
            return nextOffset;
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

        public void wakeupCustomer(final boolean flushOK) {
            long endTimestamp = System.currentTimeMillis();
            PutMessageStatus result = (flushOK && ((endTimestamp - this.startTimestamp) <= this.timeoutMillis)) ?
                    PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT;
            this.flushOKFuture.complete(result);
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // 文件末尾的最小固定长度为空
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;

        DefaultAppendMessageCallback(final int size) {
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, String msg) {

            long wroteOffset = fileFromOffset + byteBuffer.position();
            final int msgLen = msg.length();

            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            this.msgStoreItemMemory.put(msg.getBytes());

            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK,wroteOffset,msgLen);
            return result;
        }

        /**
         * 1.三大属性：capacity、 position、limit
         *  1.1 position和limit的含义取决于Buffer处在读模式还是写模式
         *  1.2 当你写数据到Buffer中时，position表示当前的位置
         *      --> position最大可为capacity – 1
         *  1.3 当读取数据时，也是从某个特定位置读,当将Buffer从写模式切换到读模式，position会被重置为0
         * @param byteBuffer
         * @param limit
         */
        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            //flip方法将Buffer从写模式切换到读模式。调用flip()方法会将position设回0，并将limit设置成之前position的值
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }
    }

}

