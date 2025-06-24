package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.common.Constants;
import cn.kong.engine.content.CrawlerContent;
import cn.kong.engine.model.DocInfo;
import cn.kong.engine.processor.collect.entity.HtmlEntry;
import cn.kong.engine.service.SQLiteService;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;


/**
 * @author gzkon
 * @description: HTML内容写入执行器
 * @date 2025/6/22 12:19
 */
@Slf4j
@Service
public class HtmlWritingExe implements BaseExecutor<HtmlEntry> {

    private static final String NODE_NAME = "HtmlWriting";
    private final SQLiteService sqLiteService;

    @Autowired
    public HtmlWritingExe(SQLiteService sqLiteService) {
        this.sqLiteService = sqLiteService;
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

        int length = entry.getHtml().getBytes(StandardCharsets.UTF_8).length;
        try {
            Document doc = Jsoup.parse(html);
            entry.setTitle(doc.title());

            byte[] recordBytes = buildRecordBytes(entry, length);

            BufferedOutputStream outputStream = content.getOutputStream();
            long maxFileSize = content.getMaxFileSize();
            long currentFileSize = content.getCurrentFileSize();

            // 检查文件大小
            if (currentFileSize + recordBytes.length > maxFileSize) {
                content.openNewFile();
            }

            outputStream.write(recordBytes);
            outputStream.flush();
            currentFileSize += recordBytes.length;
            content.setCurrentFileSize(currentFileSize);

            sqLiteService.update(buildDocInfo(entry, length, true));

        } catch (IOException e) {
            sqLiteService.update(buildDocInfo(entry, length, false));
            throw new RuntimeException("文件写入失败: " + entry.getId(), e);
        } finally {
            // 确保在执行完毕后清理数据
            content.clearNodeData();
        }
    }


    private byte[] buildRecordBytes(HtmlEntry entry, int length) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // 写入文档ID
        buffer.write(("doc_" + entry.getId()).getBytes(StandardCharsets.UTF_8));
        buffer.write(Constants.FIELD_SEPARATOR);

        // 写入文件大小
        buffer.write(length);
        buffer.write(Constants.FIELD_SEPARATOR);

        // 写入HTML内容
        byte[] htmlBytes = entry.getHtml().getBytes(StandardCharsets.UTF_8);
        buffer.write(htmlBytes);

        // 写入记录结束符
        buffer.write(Constants.RECORD_SEPARATOR);

        return buffer.toByteArray();
    }

    @Override
    public String nodeName() {
        return NODE_NAME;
    }

    private DocInfo buildDocInfo(HtmlEntry entry, int length, boolean stored) {
        DocInfo docInfo = new DocInfo();
        docInfo.setId(entry.getId());
        docInfo.setTitle(entry.getTitle());
        docInfo.setContentLength(length);
        docInfo.setStored(stored);
        docInfo.setUpdateTime(LocalDateTime.now());
        return docInfo;
    }
}
