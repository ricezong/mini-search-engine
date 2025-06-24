package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.content.CrawlerContent;

/**
 * @author gzkon
 * @description: 执行器接口
 * @date 2025/6/22 12:11
 */
public interface BaseExecutor<T> {

    void execute(T data, CrawlerContent content);

    default String nodeName() {
        return "default"; // 默认值
    }
}
