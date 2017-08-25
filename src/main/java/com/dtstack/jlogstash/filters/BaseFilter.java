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
package com.dtstack.jlogstash.filters;

import com.dtstack.jlogstash.utils.EventDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;


/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:26:50
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public abstract class BaseFilter implements Cloneable, java.io.Serializable {

    private static final long serialVersionUID = -6525215605315577598L;
    private static final Logger logger = LoggerFactory.getLogger(BaseFilter.class);
    protected Map config;
    protected String tagOnFailure;
    protected String IF;
    protected boolean hasIF;
    protected ArrayList<String> removeFields;

    public BaseFilter(Map config) {
        this.config = config;
        if (config.containsKey("tagOnFailure")) {
            this.tagOnFailure = (String) config.get("tagOnFailure");
        } else {
            this.tagOnFailure = null;
        }

        String ifValue = (String) config.get("IF");
        if (ifValue != null){
            this.IF = ifValue;
            this.hasIF = true;
        }
    }

    public abstract void prepare();

    public Map process(Map event) {
        if (event != null && event.size() > 0) {
            try {
                boolean forward = true;
                if (hasIF) {
                    Object ifValue = event.get(IF);
                    if (ifValue == null || ifValue == Boolean.FALSE) {
                        forward = false;
                    }
                }

                if (forward) {
                    long startTime = System.currentTimeMillis();

                    event = this.filter(event);
                    this.postProcess(event, true);

                    long endTime = System.currentTimeMillis();
                    logger.debug("过滤器：{}处理一条数据耗时：{}毫秒", this.getClass().getSimpleName(), (endTime - startTime));
                }
            } catch (Exception e) {
                logger.error("process error", e);
                this.postProcess(event, false);
            }
        }
        return event;
    }

    protected abstract Map filter(Map event);

    @SuppressWarnings("unchecked")
    public void postProcess(Map event, boolean ifsuccess) {
        if (ifsuccess == false) {
            if (this.tagOnFailure == null) {
                return;
            }
            EventDecorator.addTag(event, this.tagOnFailure);
        }
    }

    /**
     * 在jvm退出时调用，子类可以选择重写该方法以释放资源
     */
    public void release() {
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
