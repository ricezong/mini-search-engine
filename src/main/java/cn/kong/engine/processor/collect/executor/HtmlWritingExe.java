package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.common.Constants;
import cn.kong.engine.processor.collect.entity.DocInfo;
import cn.kong.engine.processor.collect.entity.HtmlEntry;
import cn.kong.engine.processor.collect.job.CrawlerContent;
import cn.kong.engine.service.SQLiteService;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:19
 */
@Slf4j
@Service
public class HtmlWritingExe implements BaseExecutor<HtmlEntry> {

    private static final String NODE_NAME = "HtmlWriting";

    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024; // 1GB
    private static final String FILE_PREFIX = "doc_raw_";
    private static final String FILE_SUFFIX = ".bin";
    // 使用不可见控制字符减少冲突可能
    private static final byte[] FIELD_SEPARATOR = new byte[]{0x1F};  // ASCII单元分隔符
    private static final byte[] RECORD_SEPARATOR = new byte[]{0x1E}; // ASCII记录分隔符
    private static final byte[] DOC_TERMINATOR = new byte[]{0x17};   // ASCII结束传输块

    private BufferedOutputStream outputStream;
    private SQLiteService sqLiteService;

    private long currentFileSize = 0;
    private long fileIndex = 0;

    @PostConstruct
    void init() {
        openNewFile();
        sqLiteService = new SQLiteService();
    }

    public HtmlWritingExe() {
        init();
    }

    @Override
    public void execute(HtmlEntry entry, CrawlerContent content) {
        if (Objects.isNull(entry)) {
            entry = (HtmlEntry) content.getNodeData("Request");
        }
        String html = entry.getHtml();
        if (Strings.isNullOrEmpty(html)) {
            return;
        }

        try {
            byte[] recordBytes = buildRecordBytes(entry);
            // 检查并滚动文件
            synchronized (this) {
                if (currentFileSize + recordBytes.length > MAX_FILE_SIZE) {
                    openNewFile();
                }

                outputStream.write(recordBytes);
                outputStream.flush();
                currentFileSize += recordBytes.length;
                sqLiteService.update(of(entry));
            }
        } catch (IOException e) {
            throw new RuntimeException("文件写入失败: " + entry.getId(), e);
        }
    }


    private byte[] buildRecordBytes(HtmlEntry entry) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // 写入文档ID
        buffer.write(("doc_" + entry.getId()).getBytes(StandardCharsets.UTF_8));
        buffer.write(FIELD_SEPARATOR);

        // 写入文件大小
        int length = entry.getHtml().getBytes(StandardCharsets.UTF_8).length;
        buffer.write(length);
        buffer.write(FIELD_SEPARATOR);

        // 写入HTML内容
        byte[] htmlBytes = entry.getHtml().getBytes(StandardCharsets.UTF_8);
        buffer.write(htmlBytes);

        // 写入记录结束符
        buffer.write(RECORD_SEPARATOR);

        return buffer.toByteArray();
    }


    public void openNewFile() {
        // 先关掉旧文件
        closeCurrentWriter();
        String fileName = FILE_PREFIX + fileIndex++ + FILE_SUFFIX;
        Path filePath = Paths.get(Constants.OUT_DIC, fileName);
        try {
            Files.createDirectories(filePath.getParent());
            outputStream = new BufferedOutputStream(new FileOutputStream(filePath.toFile(), false));
            currentFileSize = 0;

        } catch (IOException e) {
            throw new RuntimeException("创建文件失败: " + filePath, e);
        }
    }

    public void closeCurrentWriter() {
        if (outputStream != null) {
            try {
                outputStream.write(DOC_TERMINATOR); // 写入文件结束标记
                outputStream.close();
            } catch (IOException e) {
                log.error("关闭旧文件失败：{}", e.getMessage());
            }
        }
    }


    @Override
    public String nodeName() {
        return NODE_NAME;
    }

    private DocInfo of(HtmlEntry entry) {
        DocInfo docInfo = new DocInfo();
        docInfo.setId(entry.getId());
        docInfo.setStored(true);
        return docInfo;
    }
}
