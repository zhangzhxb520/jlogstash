package com.dtstack.jlogstash.threadpool;

import java.util.concurrent.*;

/**
 * 可取消的Executor，扩展自ThreadPoolExecutor。对于某些阻塞方法，如果它不能响应中断时应该采用CancellingExecutor及配合AbstractCancelableTask来完成自定义的取消操作。
 *
 * @author zxb
 * @date 2017/5/2
 * @since 1.0.0
 */

public class CancellingExecutor extends ThreadPoolExecutor {
    public CancellingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        if (callable instanceof CancelableTask) {
            return ((CancelableTask) callable).newTask();
        }
        return super.newTaskFor(callable);
    }
}
