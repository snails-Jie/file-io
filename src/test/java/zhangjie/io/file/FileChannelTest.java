package zhangjie.io.file;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;

/**
 * @Author zhangjie
 * @Date 2020/6/25 8:26
 **/
public class FileChannelTest {

    private DefaultMessageStore defaultMessageStore;

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathCommitLog("F:\\study\\store\\commitlog");
        messageStoreConfig.setStorePathRootDir("F:\\study\\store");
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        this.defaultMessageStore = new DefaultMessageStore(messageStoreConfig);
        defaultMessageStore.load();
        defaultMessageStore.start();
    }

    @Test
    public void testGetFileChannel() throws InterruptedException {
        defaultMessageStore.putMessage("zhangjie study");
        Thread.sleep(1000);
    }

    @Test
    public void testMappedFile() throws IOException {
        String path = "F:\\study\\store\\commitlog\\00000000000000000000";
        int fileSize = 10485760;
        MappedFile mappedFile = new MappedFile(path, fileSize);
    }

    @Test
    public void testWriteMappedFile() throws IOException, InterruptedException {
        String path = "F:\\study\\store\\commitlog\\00000000000000000000";
        File file = new File(path);
        long len = file.length();
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, len);
        // 写
//        byte[] data = new byte[4];
        String str = "Hello";
        byte[] data = str.getBytes();
        int position = 8;
        // 从当前 mmap 指针的位置写入 4b 的数据
        mappedByteBuffer.put(data);
        // 指定 position 写入 4b 的数据
        MappedByteBuffer subBuffer = (MappedByteBuffer) mappedByteBuffer.slice();
        subBuffer.position(position);
        subBuffer.put(data);
        Thread.sleep(1000);
    }

    @Test
    public void testReadMappedFile() throws IOException {
//        String path = "F:\\study\\store\\commitlog\\11111.txt";
        String path = "F:\\study\\store\\commitlog\\00000000000000000000";
        File file = new File(path);
        long len = file.length();
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, len);

        byte[] ds = new byte[(int) len];
        for (int offset = 0; offset < len; offset++) {
            byte b = mappedByteBuffer.get();
            ds[offset] = b;
        }

        Scanner scan = new Scanner(new ByteArrayInputStream(ds)).useDelimiter(" ");
        while (scan.hasNext()) {
            System.out.print(scan.next() + " ");
        }

    }
}
