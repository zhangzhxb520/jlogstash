package com.dtstack.jlogstash.threadpool.annotation;

import java.lang.annotation.*;

/**
 * 指定某字段或方法的同步策略
 *
 * @author zxb
 * @date 2017/5/3
 * @since 1.0.0
 */
@Documented
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.CLASS)
public @interface GuardedBy {

    /**
     * 指定同步策略名称
     *
     * @return 同步策略名称
     */
    String value();
}
