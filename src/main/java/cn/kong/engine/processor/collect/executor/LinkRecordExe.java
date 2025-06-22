package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.processor.collect.job.CrawlerContent;
import cn.kong.engine.processor.collect.entity.BaseEntry;
import cn.kong.engine.processor.collect.entity.DocInfo;
import cn.kong.engine.service.SQLiteService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:31
 */
@Service
public class LinkRecordExe implements BaseExecutor<BaseEntry> {

    private static final String NODE_NAME = "LinkRecord";
    private static final int BATCH_SIZE = 100; // 可根据需要调整

    private final List<DocInfo> buffer = new ArrayList<>();
    private final Object lock = new Object(); // 保证线程安全
    private final SQLiteService sqliteService;

    public LinkRecordExe() {
        this.sqliteService = new SQLiteService();
    }

    @Override
    public void execute(BaseEntry baseEntry, CrawlerContent content) {
        if (Objects.isNull(baseEntry)) {
            return;
        }
        synchronized (lock) {
            buffer.add(of(baseEntry));
            if (buffer.size() >= BATCH_SIZE) {
                flushBuffer();
            }
        }
    }

    @Override
    public String nodeName() {
        return NODE_NAME;
    }

    private void flushBuffer() {
        sqliteService.insertBatch(buffer);
        buffer.clear();
    }

    /**
     * 程序结束前调用，确保剩余未插入的记录也写入数据库
     */
    public void flushRemaining() {
        synchronized (lock) {
            if (!buffer.isEmpty()) {
                flushBuffer();
            }
        }
    }

    public DocInfo of(BaseEntry BaseEntry) {
        if (Objects.isNull(BaseEntry)) {
            return null;
        }
        DocInfo docInfo = new DocInfo();
        docInfo.setId(BaseEntry.getId());
        docInfo.setUrl(BaseEntry.getUrl());
        return docInfo;
    }
}
