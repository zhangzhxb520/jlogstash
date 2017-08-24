/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.assembly.pthread;

import com.dtstack.jlogstash.assembly.qlist.InputQueueList;
import com.dtstack.jlogstash.assembly.qlist.OutPutQueueList;
import com.dtstack.jlogstash.callback.FilterThreadSetter;
import com.dtstack.jlogstash.factory.FilterFactory;
import com.dtstack.jlogstash.filters.BaseFilter;
import com.dtstack.jlogstash.monitor.ShutDownMonitor;
import com.dtstack.jlogstash.utils.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午15:30:18
 * Company:www.dtstack.com
 *
 * @author sishu.yss
 */
public class FilterThread implements Runnable {

    public static final int QUEUE_CAPACITY = 5000;
    public static final String THREAD_NAME = "FilterThread";
    private static Logger logger = LoggerFactory.getLogger(FilterThread.class);
    private static OutPutQueueList outPutQueueList;
    private static ThreadPoolExecutor filterExecutor;
    private BlockingQueue<Map<String, Object>> inputQueue;
    private List<BaseFilter> filterProcessors;
    private static AtomicInteger inFilterEventCount = new AtomicInteger(0);


    public FilterThread(List<BaseFilter> filterProcessors, BlockingQueue<Map<String, Object>> inputQueue) {
        this.filterProcessors = filterProcessors;
        this.inputQueue = inputQueue;
    }

    public static ThreadPoolExecutor getFilterExecutor(){
        return filterExecutor;
    }

    public static OutPutQueueList getOutPutQueueList(){
        return outPutQueueList;
    }

    public static AtomicInteger getInFilterEventCount() {
        return inFilterEventCount;
    }

    @SuppressWarnings("rawtypes")
    public static void initFilterThread(List<Map> filters, InputQueueList inPutQueueList, OutPutQueueList outPutQueueList, List<BaseFilter> allBaseFilters) throws Exception {
        if (filterExecutor == null) {
            filterExecutor = ThreadPoolUtil.newFixedThreadPool(inPutQueueList.getQueueList().size(), QUEUE_CAPACITY, THREAD_NAME);
        }

        FilterThread.outPutQueueList = outPutQueueList;
        for (BlockingQueue<Map<String, Object>> queueList : inPutQueueList.getQueueList()) {
            List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);
            if (baseFilters != null) {
                allBaseFilters.addAll(baseFilters);
            }
            FilterThread filterThread = new FilterThread(baseFilters, queueList);

            // 设置回调
            if (baseFilters != null) {
                for (BaseFilter baseFilter : baseFilters) {
                    if (baseFilter instanceof FilterThreadSetter) {
                        ((FilterThreadSetter) baseFilter).setFilterThread(filterThread);
                    }
                }
            }
            filterExecutor.submit(filterThread);
        }
    }

    public static void put(Map<String, Object> event) {
        outPutQueueList.put(event);
    }

    public static void shutDownExecutor() {
        if (filterExecutor != null) {
            filterExecutor.shutdownNow();
        }
    }

    @Override
    public void run() {
        try {
            A:
            while (!Thread.currentThread().isInterrupted()) {
                Map<String, Object> event = this.inputQueue.take();
                if (ShutDownMonitor.shouldCheckEventInFilter) {
                    inFilterEventCount.incrementAndGet();
                }

                if (filterProcessors != null) {
                    for (BaseFilter bf : filterProcessors) {
                        if (event == null || event.size() == 0)
                            continue A;
                        event = bf.process(event);
                    }
                }
                if (event != null) {
                    outPutQueueList.put(event);
                }

                if (ShutDownMonitor.shouldCheckEventInFilter) {
                    inFilterEventCount.decrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            logger.error("FilterThread被中断", e);
        }
    }

    public List<BaseFilter> getFilterProcessors() {
        return filterProcessors;
    }
}
