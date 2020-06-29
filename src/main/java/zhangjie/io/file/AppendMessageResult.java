package zhangjie.io.file;

import zhangjie.io.constant.AppendMessageStatus;

/**
 * @Author zhangjie
 * @Date 2020/6/27 21:24
 **/
public class AppendMessageResult {
    // Return code
    private AppendMessageStatus status;
    // Where to start writing
    private long wroteOffset;
    // Write Bytes
    private int wroteBytes;

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }
}
