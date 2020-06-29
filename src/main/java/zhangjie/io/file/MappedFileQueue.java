package zhangjie.io.file;

import zhangjie.io.util.UtilAll;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @Author zhangjie
 * @Date 2020/6/26 8:02
 **/
public class MappedFileQueue {
    private final String storePath;
    private final int mappedFileSize;

    private final AllocateMappedFileService allocateMappedFileService;
    private long flushedWhere = 0;

    /**
     * 1.ArrayList 不是线程安全的
     *   1.1 在读线程在读取 ArrayList 的时候如果有写线程在写数据的时候，基于 fast-fail 机制，会抛出ConcurrentModificationException异常
     * 2. 读多写少
     *   2.1 ReenTrantReadWriteLock通过读写分离的思想，使得读读之间不会阻塞
     *     --> 写时读操作仍会阻塞
     *   2.2 CopyOnWriteArrayList 容器可以保证线程安全，保证读读之间在任何时候都不会被阻塞
     *     --> 牺牲数据实时性而保证数据最终一致性
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    public MappedFileQueue(final String storePath,int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService){
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.length() != this.mappedFileSize) {
                    System.out.println(file.length()
                            + " length not matched message store config value, please check it manually");
                    return false;
                }

                try{
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                }catch (IOException e) {
                    System.out.println(e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 没有则新建commitlog文件,并且连续新建两个
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(startOffset);
        String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(startOffset + this.mappedFileSize);
        MappedFile mappedFile =  this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                        nextNextFilePath, this.mappedFileSize);
        this.mappedFiles.add(mappedFile);
        return mappedFile;
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
            break;
        }
        return mappedFileLast;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        //根据偏移量查询MappedFile
        long flushOffset = this.flushedWhere;
        MappedFile firstMappedFile = this.getFirstMappedFile();
        int index = (int) ((flushOffset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
        MappedFile mappedFile = this.mappedFiles.get(index);

        if (mappedFile != null) {
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
        }
        return result;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                System.out.println("getFirstMappedFile has exception."+ e);
            }
        }

        return mappedFileFirst;
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }
}
