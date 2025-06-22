package cn.kong.engine.processor.collect.job;

import cn.kong.engine.common.Constants;
import cn.kong.engine.processor.collect.entity.BaseEntry;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.hash.BloomFilter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:11
 */
@Slf4j
public class CrawlerContent {

    // 使用ThreadLocal为每个线程维护独立数据
    private final ThreadLocal<Map<String, Object>> nodeData = ThreadLocal.withInitial(ConcurrentHashMap::new);

    @SuppressWarnings("all")
    private BloomFilter<CharSequence> bloomFilter;

    private OkHttpClient httpClient;

    private BlockingQueue<BaseEntry> urlQueue;

    private final AtomicLong docId = new AtomicLong(0); // 全局link编号

    private String workspace;


    public void init(List<String> links) {
        this.httpClient = CrawlerConfig.createHttpClient();
        this.bloomFilter = CrawlerConfig.createBloomFilter();
        this.urlQueue = new LinkedBlockingQueue<>();

        this.workspace = Constants.OUT_DIC;
        File folder = new File(workspace);
        if (!folder.exists()) {
            boolean created = folder.mkdirs();
            if (created) {
                log.info("文件夹创建成功: " + workspace);
            } else {
                log.error("文件夹创建失败: " + workspace);
            }
        }

        if (Iterables.isEmpty(links)) {
            return;
        }
        links.stream()
                .filter(link -> !Strings.isNullOrEmpty(link))  // 过滤掉 null 或 空字符串
                .forEach(link -> {
                    // 创建 BaseEntry，并将其添加到 urlQueue
                    BaseEntry entry = new BaseEntry(this.docId.incrementAndGet(), link);
                    this.urlQueue.add(entry);
                });
    }

    public void putNodeData(String key, Object value) {
        nodeData.get().put(key, value);
    }

    public Object getNodeData(String key) {
        return nodeData.get().get(key);
    }

    public void clearNodeData() {
        nodeData.remove();
    }

    public ThreadLocal<Map<String, Object>> getNodeData() {
        return nodeData;
    }

    public BloomFilter<CharSequence> getBloomFilter() {
        return bloomFilter;
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public BlockingQueue<BaseEntry> getUrlQueue() {
        return urlQueue;
    }

    public AtomicLong getDocId() {
        return docId;
    }

    public String getWorkspace() {
        return workspace;
    }
}
