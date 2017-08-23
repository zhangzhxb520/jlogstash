package com.dtstack.jlogstash.threadpool.annotation;

import java.lang.annotation.*;

/**
 * 线程安全，标注该注解的类为线程安全的类
 *
 * @author zxb
 * @date 2017/5/3
 * @since 1.0.0
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
public @interface ThreadSafe {
}
