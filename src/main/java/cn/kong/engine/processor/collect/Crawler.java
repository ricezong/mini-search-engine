package cn.kong.engine.processor.collect;

import cn.kong.engine.processor.collect.entity.BaseEntry;
import cn.kong.engine.processor.collect.entity.DocInfo;
import cn.kong.engine.processor.collect.executor.HtmlWritingExe;
import cn.kong.engine.processor.collect.executor.LinkExtractExe;
import cn.kong.engine.processor.collect.executor.LinkRecordExe;
import cn.kong.engine.processor.collect.executor.RequestExe;
import cn.kong.engine.processor.collect.job.CrawlerConfig;
import cn.kong.engine.processor.collect.job.CrawlerContent;
import cn.kong.engine.service.SQLiteService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:35
 */
@Slf4j
@Component
public class Crawler {
    private static ExecutorService executorService;
    private final RequestExe requestExe;
    private final LinkExtractExe linkExtractExe;
    private final LinkRecordExe linkRecordExe;
    private final HtmlWritingExe htmlWritingExe;
    private final SQLiteService sqliteService;

    public Crawler() {
        executorService = CrawlerConfig.createThreadPool();
        this.requestExe = new RequestExe();
        this.linkExtractExe = new LinkExtractExe();
        this.linkRecordExe = new LinkRecordExe();
        this.htmlWritingExe = new HtmlWritingExe();
        this.sqliteService = new SQLiteService();
    }


    public void run() {
        List<String> seeds = new ArrayList<>();
        seeds.add("https://www.qq.com/");
        run(seeds);
    }

    public void run(List<String> links) {
        CrawlerContent content = new CrawlerContent();
        content.init(links);

        // 启动一个线程来执行数据存储的逻辑
        executorService.submit(() -> {
            try {
                dataStored(content);  // 在一个独立线程中执行数据存储逻辑
            } catch (Exception e) {
                log.error("数据存储执行失败: {}", e.getMessage(), e);
                // 终止线程池
                executorService.shutdownNow();
            }
        });

        // 继续执行 Crawler 的逻辑
        run(content);
    }

    public void run(CrawlerContent content) {
        while (true) {
            try {
                BaseEntry entry = content.getUrlQueue().poll(1, TimeUnit.SECONDS);
                if (entry != null) {
                    linkRecordExe.execute(entry, content);
                    executorService.submit(() -> run(entry, content));
                } else {
                    if (((ThreadPoolExecutor) executorService).getActiveCount() == 0) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        linkRecordExe.flushRemaining();
        executorService.shutdown();
    }

    private void run(BaseEntry entry, CrawlerContent content) {
        requestExe.execute(entry, content);
        linkExtractExe.execute(null, content);
    }

    // 数据存储
    private void dataStored(CrawlerContent content) {
        long totalCount = sqliteService.count();
        long processedCount = 0;

        // 检查 totalCount 是否为 0，若是，休眠 30 秒并重新获取
        if (totalCount == 0) {
            log.info("数据总量为 0，休眠 30 秒后继续检查...");
            try {
                Thread.sleep(30000); // 睡眠 30 秒
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 保证中断状态
                log.error("线程被中断", e);
            }

            // 第二次查询数据总量
            totalCount = sqliteService.count();

            // 如果第二次查询结果仍为 0，结束执行
            if (totalCount == 0) {
                log.info("第二次查询数据量仍为 0，结束执行");
                return; // 直接结束方法执行
            }
        }

        // 进入数据处理循环
        while (true) {
            log.info("处理进度: {}/{} ({}%)", processedCount, totalCount, String.format("%.2f", processedCount * 100.0 / totalCount));

            // 处理当前批次数据
            processedCount = processBatch(totalCount, processedCount, content);

            // 检查是否完成
            if (processedCount >= totalCount) {
                // 获取最新数据量
                long newCount = sqliteService.count();
                if (newCount <= totalCount) {
                    break; // 没有新数据，结束循环
                }
                log.info("发现新数据: {} -> {}", totalCount, newCount);
                totalCount = newCount;
            }
        }

        log.info("数据处理完成");
    }

    private long processBatch(long totalCount, long startIndex, CrawlerContent content) {
        int processed = 0;
        for (long step = startIndex + 1; step <= totalCount; step++) {
            Optional<DocInfo> raw = sqliteService.selectById(step);
            if (raw.isEmpty()) {
                continue;
            }

            DocInfo docInfo = raw.get();
            try {
                // 处理数据
                requestExe.execute(of(docInfo), content);
                htmlWritingExe.execute(null, content);
                processed++;

                // 每处理100条输出一次进度
                if (processed % 100 == 0) {
                    log.info("正在处理: {}/{},处理进度: ({}%)", step, totalCount, String.format("%.2f", step * 100.0 / totalCount));
                }
            } catch (Exception e) {
                log.error("处理记录ID={}失败: {}", docInfo.getId(), e.getMessage(), e);
            }
        }
        return startIndex + processed;
    }

    private BaseEntry of(DocInfo docInfo) {
        BaseEntry entry = new BaseEntry();
        entry.setId(docInfo.getId());
        entry.setUrl(docInfo.getUrl());
        return entry;
    }
}

