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
package com.dtstack.jlogstash.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import com.dtstack.jlogstash.assembly.CmdLineParams;
import org.slf4j.LoggerFactory;

/**
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:27:16
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class LogbackComponent extends LogComponent {

    public static final String DEFAULT_PACKAGE = "com.dtstack";
    private static String formatePattern = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} [%file:%line] - %msg%n";

    private static int day = 7;

    @Override
    public void setupLogger() {
        String file = checkFile();
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);

        // 定义文件Appender
        RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<ILoggingEvent>();

        // policy
        TimeBasedRollingPolicy<ILoggingEvent> policy = new TimeBasedRollingPolicy<ILoggingEvent>();
        policy.setContext(loggerContext);
        policy.setMaxHistory(day);
        policy.setFileNamePattern(formateLogFile(file));
        policy.setParent(fileAppender);
        policy.start();

        // encoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern(formatePattern);
        encoder.start();

        // 配置FileAppender
        fileAppender.setRollingPolicy(policy);
        fileAppender.setContext(loggerContext);
        fileAppender.setEncoder(encoder);
        fileAppender.setFile(file);
        fileAppender.setPrudent(true); //support that multiple JVMs can safely write to the same file.
        fileAppender.start();

        // 配置console Appender
        PatternLayoutEncoder consoleEncoder = new PatternLayoutEncoder();
        consoleEncoder.setContext(loggerContext);
        consoleEncoder.setPattern(formatePattern);
        consoleEncoder.start();

        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<ILoggingEvent>();
        consoleAppender.setName("CONSOLE");
        consoleAppender.setContext(loggerContext);
        consoleAppender.setEncoder(consoleEncoder);
        consoleAppender.start();

        // 设置Logger
        root.detachAndStopAllAppenders();
        root.addAppender(consoleAppender);
        root.addAppender(fileAppender);
        root.setLevel(Level.WARN); // 默认全局为WARN级别
        root.setAdditive(false);

        // 默认Package，则采用参数控制级别
        Logger dtStackLogger = loggerContext.getLogger(DEFAULT_PACKAGE);
        setLevel(dtStackLogger);
    }

    private String formateLogFile(String file) {
        int index = file.indexOf(".");
        if (index >= 0) {
            file = file.substring(0, index);
        }
        file = file + "_%d{yyyy-MM-dd}.log";
        return file;
    }

    public void setLevel(Logger logger) {
        if (CmdLineParams.hasOptionTrace()) {
            logger.setLevel(Level.TRACE);
        } else if (CmdLineParams.hasOptionDebug()) {
            logger.setLevel(Level.DEBUG);
        } else if (CmdLineParams.hasOptionInfo()) {
            logger.setLevel(Level.INFO);
        } else if (CmdLineParams.hasOptionWarn()) {
            logger.setLevel(Level.WARN);
        } else if (CmdLineParams.hasOptionError()) {
            logger.setLevel(Level.ERROR);
        } else {
            logger.setLevel(Level.WARN);
        }
    }
}
