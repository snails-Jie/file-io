package zhangjie.io.file;

import zhangjie.io.constant.FlushDiskType;

import java.io.File;

/**
 * @Author zhangjie
 * @Date 2020/6/26 9:08
 **/
public class MessageStoreConfig {
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "commitlog";
    /**
     * CommitLog file size,default is 1G ->1073741824
     *                                     10485760
     */
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    // The maximum size of message,default is 4M
    private int maxMessageSize = 1024 * 1024 * 4;

    private FlushDiskType flushDiskType = FlushDiskType.SYNC_FLUSH;
    private int syncFlushTimeout = 1000 * 5;


    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }
}
