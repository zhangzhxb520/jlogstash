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
package com.dtstack.jlogstash.assembly;

import com.dtstack.jlogstash.assembly.qlist.QueueList;
import com.dtstack.jlogstash.filters.BaseFilter;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.outputs.BaseOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:25:34
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class ShutDownHook {

    private Logger logger = LoggerFactory.getLogger(ShutDownHook.class);

    private QueueList initInputQueueList;

    private QueueList initOutputQueueList;

    private List<BaseInput> baseInputs;

    private List<BaseOutput> baseOutputs;

    private List<BaseFilter> baseFilters;

    public ShutDownHook(QueueList initInputQueueList, QueueList initOutputQueueList, List<BaseInput> baseInputs, List<BaseOutput> baseOutputs, List<BaseFilter> baseFilters) {
        this.initInputQueueList = initInputQueueList;
        this.initOutputQueueList = initOutputQueueList;
        this.baseInputs = baseInputs;
        this.baseOutputs = baseOutputs;
        this.baseFilters = baseFilters;
    }

    public void addShutDownHook() {
        Thread shut = new Thread(new ShutDownHookThread());
        shut.setDaemon(true);
        Runtime.getRuntime().addShutdownHook(shut);
        logger.debug("addShutDownHook success ...");
    }

    class ShutDownHookThread implements Runnable {
        private void inputRelease() {
            try {
                if (baseInputs != null) {
                    for (BaseInput input : baseInputs) {
                        input.release();
                    }
                }
                logger.warn("inputRelease success...");
            } catch (Exception e) {
                logger.error("inputRelease error:{}", e.getMessage());
            }
        }

        private void filterRelease() {
            try {
                if (baseFilters != null) {
                    for (BaseFilter baseFilter : baseFilters) {
                        baseFilter.release();
                    }
                }
                logger.info("filterRelease success...");
            } catch (Exception e) {
                logger.error("filterRelease error", e);
            }
        }

        private void outPutRelease() {
            try {
                if (baseOutputs != null) {
                    for (BaseOutput outPut : baseOutputs) {
                        outPut.release();
                    }
                }
                logger.warn("outPutRelease success...");
            } catch (Exception e) {
                logger.error("outPutRelease error:{}", e.getMessage());
            }
        }


        @Override
        public void run() {
            inputRelease();
            if (initInputQueueList != null) initInputQueueList.queueRelease();
            if (initOutputQueueList != null) initOutputQueueList.queueRelease();
            outPutRelease();
        }
    }
}
