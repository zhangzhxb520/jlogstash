package com.dtstack.jlogstash.threadpool;


import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;

/**
 * 可取消任务，扩展自Callable。可实现自定义的取消逻辑。
 *
 * @author zxb
 * @date 2017/5/2
 * @since 1.0.0
 */

public interface CancelableTask<T> extends Callable<T> {


    /**
     * 取消任务
     */
    void cancel();

    /**
     * 构造新任务
     *
     * @return
     */
    RunnableFuture<T> newTask();
}
