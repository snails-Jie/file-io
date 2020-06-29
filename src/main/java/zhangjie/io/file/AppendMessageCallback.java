package zhangjie.io.file;

import java.nio.ByteBuffer;

/**
 * @Author zhangjie
 * @Date 2020/6/26 9:37
 **/
public interface AppendMessageCallback {
    /**
     * 消息序列化后，写入MapedByteBuffer
     * @return How many bytes to write
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                 final int maxBlank, final String msg);
}
