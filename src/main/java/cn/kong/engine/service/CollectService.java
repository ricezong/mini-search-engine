package cn.kong.engine.service;

import cn.kong.engine.processor.collect.Crawler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 21:11
 */
@Service
public class CollectService {

    @Autowired
    private Crawler crawler;

    public void runCrawler(List<String> seeds) {
        CompletableFuture.runAsync(() -> {
            crawler.run(seeds);
        });
    }
}
