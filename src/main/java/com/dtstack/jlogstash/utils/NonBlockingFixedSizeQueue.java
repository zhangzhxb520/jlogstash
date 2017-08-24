package com.dtstack.jlogstash.utils;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * 固定大小非阻塞的队列
 *
 * @author zxb
 * @version 1.0.0
 * 2017年08月24日 11:45
 * @since 1.0.0
 */
public class NonBlockingFixedSizeQueue<E> extends ArrayBlockingQueue<E> {

    private int size;

    public NonBlockingFixedSizeQueue(int capacity) {
        super(capacity);
        this.size = capacity;
    }

    synchronized public boolean add(E e) {
        // Check if queue full already?
        if (super.size() == this.size) {
            // remove element from queue if queue is full
            this.remove();
        }
        return super.add(e);
    }
}
