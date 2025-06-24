package cn.kong.engine.content;

import cn.kong.engine.common.Constants;
import cn.kong.engine.model.CrawlerTask;
import cn.kong.engine.processor.collect.entity.BaseEntry;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gzkon
 * @description: 爬虫上下文
 * @date 2025/6/22 12:11
 */
@Slf4j
public class CrawlerContent {
    private CrawlerTask task;

    // 使用ThreadLocal为每个线程维护独立数据
    private final ThreadLocal<Map<String, Object>> nodeData = ThreadLocal.withInitial(ConcurrentHashMap::new);

    private BlockingQueue<BaseEntry> urlQueue;  // 用来存储url的队列，每个实例私有

    @SuppressWarnings("all")
    private BloomFilter<CharSequence> bloomFilter;  // 用来记录重复url的布隆过滤器，每个实例私有

    private final AtomicLong docId = new AtomicLong(0); // 全局link唯一编号

    private BufferedOutputStream outputStream;  // 文件写入流，每个实例私有

    private long maxFileSize = 1024 * 1024 * 1024; // 单个文件的最大大小，默认1GB

    private long currentFileSize = 0;   // 当前文件大小

    private long fileIndex = 0;     // 当前文件下标

    private final AtomicBoolean running = new AtomicBoolean(true);

    @SuppressWarnings("all")
    public void init(CrawlerTask task) {
        this.task = task;
        this.urlQueue = new LinkedBlockingQueue<>();
        this.bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                100_000_000,
                0.01);


        List<String> links = task.getLinks();
        if (Iterables.isEmpty(links)) {
            throw new RuntimeException("种子Link为空");
        }
        links.stream()
                .filter(link -> !Strings.isNullOrEmpty(link))  // 过滤掉 null 或 空字符串
                .forEach(link -> {
                    // 创建 BaseEntry，并将其添加到 urlQueue
                    BaseEntry entry = new BaseEntry(this.docId.incrementAndGet(), link);
                    this.urlQueue.add(entry);
                });
        this.maxFileSize = task.getMaxFileSize() != null ? task.getMaxFileSize() : this.maxFileSize;
        // 初始化文件写入流
        openNewFile();
    }

    public void openNewFile() {
        // 先关掉旧文件
        closeCurrentWriter();

        String filePrefix = task.getFilePrefix();
        String fileSuffix = task.getFileSuffix();
        if (Strings.isNullOrEmpty(filePrefix)) {
            filePrefix = Constants.FILE_PREFIX;
        }
        if (Strings.isNullOrEmpty(fileSuffix)) {
            fileSuffix = Constants.FILE_SUFFIX;
        }

        String fileName = filePrefix + this.fileIndex++ + fileSuffix;
        Path filePath = Paths.get(Constants.OUT_DIC, fileName);
        try {
            Files.createDirectories(filePath.getParent());
            this.outputStream = new BufferedOutputStream(new FileOutputStream(filePath.toFile(), false));
            this.currentFileSize = 0;

        } catch (IOException e) {
            throw new RuntimeException("创建文件失败: " + filePath, e);
        }
    }


    public void closeCurrentWriter() {
        if (this.outputStream != null) {
            try {
                this.outputStream.write(Constants.DOC_TERMINATOR); // 写入文件结束标记
                this.outputStream.close();
            } catch (IOException e) {
                log.error("关闭旧文件失败：{}", e.getMessage());
            }
        }
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

    @SuppressWarnings("all")
    public BloomFilter<CharSequence> getBloomFilter() {
        return bloomFilter;
    }

    public BlockingQueue<BaseEntry> getUrlQueue() {
        return urlQueue;
    }

    public AtomicLong getDocId() {
        return docId;
    }

    public BufferedOutputStream getOutputStream() {
        return outputStream;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public long getCurrentFileSize() {
        return currentFileSize;
    }

    public void setCurrentFileSize(long currentFileSize) {
        this.currentFileSize = currentFileSize;
    }

    public boolean getRunning() {
        return running.get();
    }

    public void setRunning(boolean running) {
        this.running.set(running);
    }
}
