package cn.kong.engine.service;

import cn.kong.engine.content.CrawlerContent;
import cn.kong.engine.model.CrawlerTask;
import cn.kong.engine.processor.collect.Crawler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 21:11
 */
@Service
public class CollectService {
    private final Crawler crawler;

    private final Map<String, CrawlerContent> crawlerContentMap = new ConcurrentHashMap<>();

    @Autowired
    public CollectService(Crawler crawler) {
        this.crawler = crawler;
    }

    public void runCrawler(CrawlerTask task) {
        if (crawlerContentMap.size() > 2) {
            throw new IllegalStateException("当前爬虫任务数量已达上限，请稍后再试");
        }
        CrawlerContent content = new CrawlerContent();
        content.init(task);
        crawlerContentMap.put(task.getTaskId(), content);
        CompletableFuture.runAsync(() -> {
            crawler.run(content);
        });
    }

    public void stopCrawler(String taskId) {
        crawlerContentMap.get(taskId).setRunning(false);
        crawlerContentMap.remove(taskId);
    }
}
