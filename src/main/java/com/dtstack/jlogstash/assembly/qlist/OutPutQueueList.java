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
package com.dtstack.jlogstash.assembly.qlist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月29日 下午1:25:23
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class OutPutQueueList extends QueueList {

    private static Logger logger = LoggerFactory.getLogger(OutPutQueueList.class);

    private static ExecutorService executor = Executors.newFixedThreadPool(1);
    private static int SLEEP = 1;//queue选取的间隔时间
    private static OutPutQueueList outPutQueueList;
    private final AtomicInteger pIndex = new AtomicInteger(0);

    public static OutPutQueueList getOutPutQueueListInstance(int queueNumber, int queueSize) {
        if (outPutQueueList != null) return outPutQueueList;
        outPutQueueList = new OutPutQueueList();
        for (int i = 0; i < queueNumber; i++) {
            outPutQueueList.queueList.add(new ArrayBlockingQueue<Map<String, Object>>(queueSize));
        }
        outPutQueueList.startElectionIdleQueue();
        return outPutQueueList;
    }

    @Override
    public void put(Map<String, Object> message) {
        try {
            queueList.get(pIndex.get()).put(message);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.error("put output queue message error:{}", e.getCause());
        }
    }

    @Override
    public void startElectionIdleQueue() {
        executor.submit(new ElectionIdleQueue());
    }

    @Override
    public void queueRelease() {
        try {
            boolean empty = allQueueEmpty();
            while (!empty) {
                empty = allQueueEmpty();
            }
            logger.warn("out queue size==" + allQueueSize());
            logger.warn("outputQueueRelease success ...");
        } catch (Exception e) {
            logger.error("outputQueueRelease error:{}", e.getCause());
        }

        // 停止选取空闲输入队列的线程池
        executor.shutdownNow();
    }

    class ElectionIdleQueue implements Runnable {
        @Override
        public void run() {
            int size = queueList.size();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    if (size > 0) {
                        int id = 0;
                        int sz = Integer.MAX_VALUE;
                        for (int i = 0; i < size; i++) {
                            int ssz = queueList.get(i).size();
                            if (ssz <= sz) {
                                sz = ssz;
                                id = i;
                            }
                        }
                        pIndex.getAndSet(id);
                    }
                    Thread.sleep(SLEEP);
                }
            } catch (InterruptedException e) {
                logger.error("选取输出队列线程被中断...", e);
            }
        }
    }
}
