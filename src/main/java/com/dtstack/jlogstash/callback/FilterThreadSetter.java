package com.dtstack.jlogstash.callback;

import com.dtstack.jlogstash.assembly.pthread.FilterThread;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月29日 11:39
 * @since Jdk1.6
 */
public interface FilterThreadSetter {

    /**
     * 设置FilterThread
     *
     * @param filterThread
     */
    void setFilterThread(FilterThread filterThread);
}
