package com.dtstack.jlogstash.threadpool;

import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * @author zxb
 * @date 2017/5/2
 * @since 1.0.0
 */

public abstract class AbstractCancelableTask<T> implements CancelableTask<T> {
    @Override
    public RunnableFuture<T> newTask() {
        return new FutureTask<T>(this) {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                try {
                    AbstractCancelableTask.this.cancel();
                } finally {
                    return super.cancel(mayInterruptIfRunning);
                }
            }
        };
    }
}
