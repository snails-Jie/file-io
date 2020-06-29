package zhangjie.io.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author zhangjie
 * @Date 2020/6/26 7:56
 **/
public class MappedFile {

    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    protected int fileSize;
    protected FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    private String fileName;
    private long fileFromOffset;
    private File file;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public AppendMessageResult appendMessageInner(final String messageExt, final AppendMessageCallback cb){
        int currentPos = this.wrotePosition.get();
        AppendMessageResult result = null;
        if (currentPos < this.fileSize) {
            //指定 position位置
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, messageExt);
            this.wrotePosition.addAndGet(result.getWroteBytes());
        }

        return result;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;

        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        //如果需要访问文件的部分内容，而不是把文件从头读到尾，使用RandomAccessFile将是更好的选择
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        /**
         *1. 把文件映射到用户空间里的虚拟内存，省去了从内核缓冲区复制到用户空间的过程
         *  -->文件中的位置在虚拟内存中有了对应的地址，可以像操作内存一样操作这个文件
         *2. 只有真正使用这些数据时,虚拟内存管理系统 VMS 才根据缺页加载的机制从磁盘加载对应的数据块到物理内存进行渲染
         */
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    public int flush(final int flushLeastPages) {
        int value = getReadPosition();
        try{
            if (this.fileChannel.position() != 0) {
                this.fileChannel.force(false);
            }else {
                this.mappedByteBuffer.force();
            }
        }catch (Throwable e) {
            System.out.println("Error occurred when force data to disk."+e);
        }

        this.flushedPosition.set(value);

        return this.getFlushedPosition();
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return  this.wrotePosition.get();
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }
}
