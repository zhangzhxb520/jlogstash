package com.dtstack.jlogstash.threadpool;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程池内部工作线程
 *
 * @author zxb
 * @date 2017/5/2
 * @since 1.0.0
 */

public class WorkThread extends Thread implements Thread.UncaughtExceptionHandler {

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkThread.class);

    private static final AtomicInteger created = new AtomicInteger();

    private static final AtomicInteger alive = new AtomicInteger();


    public WorkThread(Runnable target, String name) {
        super(target, name + "-" + created.incrementAndGet());
        setUncaughtExceptionHandler(this);
    }

    /**
     * 获取已创建的线程数
     *
     * @return 已创建线程数
     */
    public static int getThreadsCreated() {
        return created.get();
    }

    /**
     * 获取正在运行的线程数
     *
     * @return 正在运行的线程数
     */
    public static int getThreadsAlive() {
        return alive.get();
    }

    @Override
    public void run() {
        LOGGER.debug("Created {}", getName());
        try {
            alive.incrementAndGet();
            super.run();
        } finally {
            alive.decrementAndGet();
            LOGGER.debug("Exiting {}", getName());
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOGGER.error("uncaughtException: " + t.getName(), e);
    }
}
