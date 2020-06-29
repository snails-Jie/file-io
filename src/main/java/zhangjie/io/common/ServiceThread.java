package zhangjie.io.common;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author zhangjie
 * @Date 2020/6/26 11:21
 **/
public abstract class ServiceThread implements Runnable {
    private final AtomicBoolean started = new AtomicBoolean(false);
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    protected volatile boolean stopped = false;
    private Thread thread;
    protected boolean isDaemon = false;

    public abstract String getServiceName();

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public boolean isStopped() {
        return stopped;
    }
}
