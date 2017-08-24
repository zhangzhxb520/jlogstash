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

import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.monitor.ShutDownMonitor;
import com.dtstack.jlogstash.utils.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:29
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class InputThread implements Runnable {

    public static final String THREAD_NAME = "InputThread";
    public static final int CAPACITY = 5000;
    private static ExecutorService inputExecutor;
    private static CyclicBarrier cyclicBarrier;
    private Logger logger = LoggerFactory.getLogger(InputThread.class);
    private BaseInput baseInput;

    public InputThread(BaseInput baseInput) {
        this.baseInput = baseInput;
    }

    public static void initInputThread(List<BaseInput> baseInputs) {
        if (inputExecutor == null) {
            inputExecutor = ThreadPoolUtil.newFixedThreadPool(baseInputs.size(), CAPACITY, THREAD_NAME);
        }

        cyclicBarrier = new CyclicBarrier(baseInputs.size());

        for (BaseInput input : baseInputs) {
            inputExecutor.submit(new InputThread(input));
        }
    }

    public static void shutDownExecutor() {
        if (inputExecutor != null) {
            inputExecutor.shutdownNow();
        }
    }

    @Override
    public void run() {
        if (baseInput == null) {
            logger.error("input plugin is not null");
            System.exit(1);
        }

        baseInput.emit();

        try {
            cyclicBarrier.await();; // 先完成的输入源在此等待
        } catch (InterruptedException e) {
            logger.error(baseInput + "输入源等待其它输入源时被中断", e);
        } catch (BrokenBarrierException e) {
            logger.error(baseInput + "输入源等待其它输入源时被打破栅栏", e);
        }

        ShutDownMonitor.waitFinishInit();
        ShutDownMonitor.startMonitor();

        // 往输入队列投毒，用于提示并终止JLogstash
        /*
        while (!Thread.currentThread().isInterrupted()) {
            baseInput.process(Constants.POISON);
        }
        logger.info(baseInput + "输入源已正常停止...");
        */
    }
}
