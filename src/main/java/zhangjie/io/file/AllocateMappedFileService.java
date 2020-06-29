package zhangjie.io.file;

import zhangjie.io.common.ServiceThread;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * @Author zhangjie
 * @Date 2020/6/27 16:59
 **/
public class AllocateMappedFileService extends ServiceThread {
    protected volatile boolean stopped = false;
    private static int waitTimeOut = 1000 * 5;
    private ConcurrentMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<String, AllocateRequest>();
    private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<AllocateRequest>();

    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;
        if (nextPutOK) {
            this.requestQueue.offer(nextReq);
        }
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            this.requestQueue.offer(nextNextReq);
        }
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try{
            if (result != null) {
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if(waitOK){
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            }
        }catch (InterruptedException e){
            System.out.println(this.getServiceName() + " service has exception. "+ e);
        }

        return null;
    }


    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void run() {
        System.out.println(this.getServiceName() + " service started");
        while (!this.isStopped() && this.mmapOperation()) {

        }
        System.out.println(this.getServiceName() + " service end");
    }

    private boolean mmapOperation() {
        AllocateRequest req = null;
        boolean isSuccess = false;
        try{
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (req.getMappedFile() == null) {
                MappedFile mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                req.setMappedFile(mappedFile);
            }
            isSuccess = true;

        } catch (InterruptedException e) {
            System.out.println(this.getServiceName() + " service has exception. "+e);
            return false;
        }catch (IOException e){
            System.out.println(this.getServiceName() + " service has exception. "+ e);
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        }finally {
            if (req != null && isSuccess){
                req.getCountDownLatch().countDown();
            }
        }
        return true;
    }

    static class AllocateRequest implements Comparable<AllocateRequest> {
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        @Override
        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize){
                return 1;
            }else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }
}
