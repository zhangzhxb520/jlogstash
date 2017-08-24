package com.dtstack.jlogstash.monitor;

import com.dtstack.jlogstash.assembly.ShutDownHelper;
import com.dtstack.jlogstash.assembly.pthread.FilterThread;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.utils.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 关闭线程Monitor
 *
 * @author zxb
 * @version 1.0.0
 * 2017年08月23日 17:43
 * @since 1.0.0
 */
public class ShutDownMonitor {

    public static final String THREAD_NAME = "ShutDownMonitorThread";
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ShutDownMonitor.class);
    public static volatile boolean shouldCheckEventInFilter = false;
    private static ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.newSingleThreadExecutor(5000, THREAD_NAME);
    private static AtomicBoolean hasStart = new AtomicBoolean(false);
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void startMonitor() {
        LOGGER.info("准备开启监控退出线程...");
        if (!hasStart.getAndSet(true)) {
            LOGGER.info("正在开启监控退出线程...");
            shouldCheckEventInFilter = true;

            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    LOGGER.info("监控退出线程正在执行任务，循环条件：{}", !Thread.currentThread().isInterrupted());
                    try {
                        while (!Thread.currentThread().isInterrupted()) {

                            LOGGER.info("开始检查是否满足退出条件...");
                            boolean inputEmpty = BaseInput.getInputQueueList().allQueueEmpty();
                            boolean noEventInFilter = FilterThread.getInFilterEventCount().get() <= 0;
                            boolean outputEmpty = FilterThread.getOutPutQueueList().allQueueEmpty();

                            LOGGER.info("输入队列是否为空：{}，Filter中没有Event：{}，输出队列是否为空：{}", inputEmpty, noEventInFilter, outputEmpty);
                            // if (inputEmpty && filterThreadsEmpty && outputEmpty && outputThreadEmpty) {
                            if (inputEmpty && noEventInFilter && outputEmpty) {
                                LOGGER.info("满足退出条件，开始关闭JLogstash...");
                                ShutDownHelper.shutDown();
                                break;
                            }

                            Thread.sleep(5000);
                        }
                    } catch (Exception e) {
                        LOGGER.error("监控退出线程检查异常", e);
                    }
                }
            });
            LOGGER.info("成功开启监控退出线程...");
            threadPoolExecutor.shutdown();
        } else {
            LOGGER.warn("监控退出线程已经被开启，请勿重复开启");
        }
    }

    public static void finishInit() {
        countDownLatch.countDown();
    }

    public static void waitFinishInit() {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("等待初始化完成被中断...", e);
        }
    }
}
