package com.dtstack.jlogstash.assembly;

import com.dtstack.jlogstash.assembly.qlist.QueueList;
import com.dtstack.jlogstash.filters.BaseFilter;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.outputs.BaseOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 退出工具类
 *
 * @author zxb
 * @version 1.0.0
 * 2017年08月22日 16:13
 * @since 1.0.0
 */
public class ShutDownHelper {

    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ShutDownHelper.class);

    /**
     * 关闭钩子
     */
    private static ShutDownHook shutDownHook;

    private static AtomicInteger inputCount;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void initInputCount(int inputCount) {
        ShutDownHelper.inputCount = new AtomicInteger(inputCount);
    }

    public static void decrease() {
        if (ShutDownHelper.inputCount.decrementAndGet() <= 0) {
            normalExit(); // 输入源全部完成，正常退出程序
        }
    }

    /**
     * 当输入源都完成数据抽取时，正常退出程序
     */
    private static void normalExit() {
        try {
            LOGGER.debug("等待所有组件初始化完成...");
            countDownLatch.await();
            LOGGER.debug("所有组件已初始化完成...");
        } catch (InterruptedException e) {
            LOGGER.error("等待组件初始化完成被中断...", e);
        } finally {
            // 退出程序
            shutDown();
        }
    }

    /**
     * 手动退出程序
     */
    public static void shutDown() {
        if (shutDownHook != null) {
            LOGGER.info("正在退出JLogStash，开始释放资源...");
            shutDownHook.shutdown();
            LOGGER.info("成功退出JLogStash，释放资源完毕...");
        } else {
            LOGGER.error("未找到ShutDownHook，无法正常退出JLogStash");
        }
    }

    /**
     * 添加JVM退出时的回调钩子
     *
     * @param initInputQueueList
     * @param initOutputQueueList
     * @param baseInputs
     * @param baseOutputs
     * @param baseFilters
     */
    public static void addShutDownHook(QueueList initInputQueueList, QueueList initOutputQueueList, List<BaseInput> baseInputs, List<BaseOutput> baseOutputs, List<BaseFilter> baseFilters) {
        ShutDownHelper.shutDownHook = new ShutDownHook(initInputQueueList, initOutputQueueList, baseInputs, baseOutputs, baseFilters);
        addShutDownHook();
    }

    /**
     * 添加JVM退出时的回调钩子
     */
    private static void addShutDownHook() {
        Thread shut = new Thread(new Runnable() {
            @Override
            public void run() {
                ShutDownHelper.shutDown();
            }
        });
        shut.setDaemon(true);
        Runtime.getRuntime().addShutdownHook(shut);
        LOGGER.debug("addShutDownHook success ...");
    }

    /**
     * 表示已完成初始化工作
     */
    public static void finishInit() {
        countDownLatch.countDown();
    }
}
