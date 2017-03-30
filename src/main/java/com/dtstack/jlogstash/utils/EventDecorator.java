package com.dtstack.jlogstash.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 事件装饰
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月29日 10:27
 * @since Jdk1.6
 */
public class EventDecorator {

    private static final String TAGS = "tags";

    public static void addTags(Map event, List<String> tagList) {
        if (tagList == null || tagList.size() <= 0) {
            return;
        }

        Object tagsObj = event.get(TAGS);
        if (tagsObj != null) {
            if (List.class.isAssignableFrom(tagsObj.getClass())) {
                List tags = (List) tagsObj;

                // 添加tag，保证不重复添加
                for (String tag : tagList) {
                    if (!tags.contains(tag)) {
                        tags.add(tag);
                    }
                }
            }
        } else {
            event.put(TAGS, tagList);
        }
    }

    public static void addTag(Map event, String tag) {
        if (StringUtils.isEmpty(tag)) {
            return;
        }

        Object tags = event.get(TAGS);
        if (tags != null) {
            if (List.class.isAssignableFrom(tags.getClass())) {
                List tagList = (List) tags;

                // 添加tag，保证不重复添加
                if (!tagList.contains(tag)) {
                    tagList.add(tag);
                }
            }
        } else {
            List<String> tagList = new ArrayList<String>();
            tagList.add(tag);

            event.put(TAGS, tagList);
        }
    }
}
