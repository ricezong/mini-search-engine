package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.processor.collect.job.CrawlerContent;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:11
 */
public interface BaseExecutor<T> {

    void execute(T data, CrawlerContent content);

    default String nodeName() {
        return "default"; // 默认值
    }
}
