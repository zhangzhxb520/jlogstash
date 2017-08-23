package com.dtstack.jlogstash.utils;


import com.dtstack.jlogstash.threadpool.CancellingExecutor;
import com.dtstack.jlogstash.threadpool.WorkThread;

import java.util.concurrent.*;

/**
 * 线程池工具类
 *
 * @author zxb
 * @date 2017/5/1
 * @since jdk1.6
 */

public class ThreadPoolUtil {

    private static final String WORK_THREAD_NAME = "WORK-THREAD-NAME";

    private ThreadPoolUtil() {
    }

    /**
     * 获取缓冲线程池，采用有界阻塞队列
     *
     * @return ThreadPoolExecutor
     */
    public static ThreadPoolExecutor newCachedThreadPool() {
        return newCachedThreadPool(WORK_THREAD_NAME);
    }

    /**
     * 获取缓冲线程池，采用有界阻塞队列
     *
     * @return ThreadPoolExecutor
     */
    public static ThreadPoolExecutor newCachedThreadPool(final String workThreadName) {
        ThreadPoolExecutor threadPoolExecutor = new CancellingExecutor(0, Integer.MAX_VALUE, 1L, TimeUnit.MINUTES, new SynchronousQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new WorkThread(r, workThreadName);
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
        return threadPoolExecutor;
    }

    /**
     * 获取大小线程池
     *
     * @param poolSize      池大小
     * @param queueCapacity 队列大小
     * @return 线程池
     */
    public static ThreadPoolExecutor newFixedThreadPool(int poolSize, int queueCapacity) {
        return newFixedThreadPool(poolSize, queueCapacity, WORK_THREAD_NAME);
    }

    /**
     * 获取大小线程池
     *
     * @param poolSize      池大小
     * @param queueCapacity 队列大小
     * @return 线程池
     */
    public static ThreadPoolExecutor newFixedThreadPool(int poolSize, int queueCapacity, final String workThreadName) {
        ThreadPoolExecutor threadPoolExecutor = new CancellingExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(queueCapacity), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new WorkThread(r, workThreadName);
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
        return threadPoolExecutor;
    }

    /**
     * 单线程线程池
     *
     * @param queueCapacity 队列大小
     * @return 线程池
     */
    public static ThreadPoolExecutor newSingleThreadExecutor(int queueCapacity) {
        return newSingleThreadExecutor(queueCapacity, WORK_THREAD_NAME);
    }

    /**
     * 单线程线程池
     *
     * @param queueCapacity 队列大小
     * @return 线程池
     */
    public static ThreadPoolExecutor newSingleThreadExecutor(int queueCapacity, final String workThreadName) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(queueCapacity), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new WorkThread(r, workThreadName);
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
        return threadPoolExecutor;
    }
}
