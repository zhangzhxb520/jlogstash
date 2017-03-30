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
import com.dtstack.jlogstash.exception.ExceptionUtil;
import com.dtstack.jlogstash.factory.FilterFactory;
import com.dtstack.jlogstash.filters.BaseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午15:30:18
 * Company:www.dtstack.com
 *
 * @author sishu.yss
 */
public class FilterThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(FilterThread.class);
    private static OutPutQueueList outPutQueueList;
    private static ExecutorService filterExecutor;
    private BlockingQueue<Map<String, Object>> inputQueue;
    private List<BaseFilter> filterProcessors;

    public FilterThread(List<BaseFilter> filterProcessors, BlockingQueue<Map<String, Object>> inputQueue) {
        this.filterProcessors = filterProcessors;
        this.inputQueue = inputQueue;
    }

    @SuppressWarnings("rawtypes")
    public static void initFilterThread(List<Map> filters, InputQueueList inPutQueueList, OutPutQueueList outPutQueueList, List<BaseFilter> allBaseFilters) throws Exception {
        if (filterExecutor == null) filterExecutor = Executors.newFixedThreadPool(inPutQueueList.getQueueList().size());
        FilterThread.outPutQueueList = outPutQueueList;
        for (BlockingQueue<Map<String, Object>> queueList : inPutQueueList.getQueueList()) {
            List<BaseFilter> baseFilters = FilterFactory.getBatchInstance(filters);
            allBaseFilters.addAll(baseFilters);

            FilterThread filterThread = new FilterThread(baseFilters, queueList);

            // 设置回调
            for (BaseFilter baseFilter : baseFilters) {
                if (baseFilter instanceof FilterThreadSetter) {
                    ((FilterThreadSetter) baseFilter).setFilterThread(filterThread);
                }
            }
            filterExecutor.submit(filterThread);
        }
    }

    public static void put(Map<String, Object> event) {
        outPutQueueList.put(event);
    }

    @Override
    public void run() {
        A:
        while (true) {
            Map<String, Object> event = null;
            try {
                event = this.inputQueue.take();
                if (filterProcessors != null) {
                    for (BaseFilter bf : filterProcessors) {
                        if (event == null || event.size() == 0)
                            continue A;
                        event = bf.process(event);
                    }
                }
                if (event != null) outPutQueueList.put(event);
            } catch (Exception e) {
                logger.error("{}:filter event failed:{}", event, ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    public List<BaseFilter> getFilterProcessors() {
        return filterProcessors;
    }
}
