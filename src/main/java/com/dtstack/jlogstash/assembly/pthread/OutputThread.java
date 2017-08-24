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

import com.dtstack.jlogstash.assembly.qlist.OutPutQueueList;
import com.dtstack.jlogstash.factory.OutputFactory;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.dtstack.jlogstash.utils.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午15:30:18
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class OutputThread implements Runnable {

    public static final String THREAD_NAME = "OutputThread";
    public static final int QUEUE_CAPACITY = 5000;
    private static Logger logger = LoggerFactory.getLogger(OutputThread.class);
    private static ThreadPoolExecutor outputExecutor;
    private List<BaseOutput> outputProcessors;
    private BlockingQueue<Map<String, Object>> outputQueue;


    public OutputThread(List<BaseOutput> outputProcessors, BlockingQueue<Map<String, Object>> outputQueue) {
        this.outputProcessors = outputProcessors;
        this.outputQueue = outputQueue;
    }

    public static ThreadPoolExecutor getOutputExecutor(){
        return outputExecutor;
    }

    @SuppressWarnings("rawtypes")
    public static void initOutPutThread(List<Map> outputs, OutPutQueueList outPutQueueList, List<BaseOutput> allBaseOutputs) throws Exception {
        if (outputExecutor == null) {
            outputExecutor = ThreadPoolUtil.newFixedThreadPool(outPutQueueList.getQueueList().size(), QUEUE_CAPACITY, THREAD_NAME);
        }

        for (BlockingQueue<Map<String, Object>> queueList : outPutQueueList.getQueueList()) {
            List<BaseOutput> baseOutputs = OutputFactory.getBatchInstance(outputs);
            allBaseOutputs.addAll(baseOutputs);
            outputExecutor.submit(new OutputThread(baseOutputs, queueList));
        }
    }

    public static void shutDownExecutor() {
        if (outputExecutor != null) {
            outputExecutor.shutdownNow();
        }
    }

    @Override
    public void run() {
        Map<String, Object> event = null;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                if (!priorityFail()) {
                    event = this.outputQueue.take();
                    if (event != null) {
                        for (BaseOutput bo : outputProcessors) {
                            bo.process(event);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            logger.error("OutputThread被中断", e);
        }
    }

    private boolean priorityFail() {
        //优先处理失败信息
        boolean dealFailMsg = false;
        for (BaseOutput bo : outputProcessors) {
            if (bo.isConsistency()) {
                dealFailMsg = dealFailMsg || bo.dealFailedMsg();
            }
        }
        return dealFailMsg;
    }
}
