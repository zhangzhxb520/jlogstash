package com.dtstack.jlogstash.assembly;

import com.dtstack.jlogstash.assembly.qlist.QueueList;
import com.dtstack.jlogstash.filters.BaseFilter;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.utils.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * 退出工具类
 *
 * @author zxb
 * @version 1.0.0
 * 2017年08月22日 16:13
 * @since 1.0.0
 */
public class ShutDownHelper {

    public static final String THREAD_NAME = "ShutDownThread";
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ShutDownHelper.class);
    private static final Object FLAG_OBJ = new Object();
    /**
     * 关闭钩子
     */
    private static ShutDownHook shutDownHook;

    /**
     * 输出线程闭锁，保证所有的输出线程任务全部完成
     */
    private static CountDownLatch outputCountDown;

    /**
     * 过滤线程闭锁，确保所有过滤线程任务完成
     */
    private static CountDownLatch filterCountDown;

    private static ExecutorService singleExecutor = ThreadPoolUtil.newSingleThreadExecutor(5000, THREAD_NAME);

    /**
     * 记录已经CountDown过的线程，防止同一线程重复调用CountDown
     */
    private static ConcurrentHashMap<Object, Object> countDownMap = new ConcurrentHashMap<Object, Object>();

    public static void initOutputThreadCount(int outputThreadsNum) {
        ShutDownHelper.outputCountDown = new CountDownLatch(outputThreadsNum);
    }

    public static void initFilterThreadCount(int filterThreadNum) {
        ShutDownHelper.filterCountDown = new CountDownLatch(filterThreadNum);
    }

    public static void decreaseOutput(Object obj) {
        if (countDownMap.get(obj) != FLAG_OBJ) {
            ShutDownHelper.outputCountDown.countDown();
            countDownMap.put(obj, FLAG_OBJ);
        }
    }

    public static void decreaseFilter(Object obj) {
        if (countDownMap.get(obj) != FLAG_OBJ) {
            ShutDownHelper.filterCountDown.countDown();
            countDownMap.put(obj, FLAG_OBJ);
        }
    }

    /**
     * 当输入源都完成数据抽取时，正常退出程序
     */
    private static void normalExit() {
        try {
            LOGGER.debug("等待所有组件初始化完成...");
            outputCountDown.await();
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
     * 注册正常退出关闭线程
     */
    public static void registerNormalExitThread() {
        singleExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 当所有输出线程完成任务时开始关闭JLogstash
                    ShutDownHelper.outputCountDown.await();
                    // ShutDownHelper.filterCountDown.await();
                    ShutDownHelper.shutDown();
                } catch (InterruptedException e) {
                    LOGGER.error("ShutDownThread等待输入线程完成时被中断", e);
                }
            }
        });
        LOGGER.debug("start ShutDownThread success...");
    }
}
