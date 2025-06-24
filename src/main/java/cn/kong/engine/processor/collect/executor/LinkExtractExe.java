package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.content.CrawlerContent;
import cn.kong.engine.processor.collect.entity.BaseEntry;
import cn.kong.engine.processor.collect.entity.HtmlEntry;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.hash.BloomFilter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gzkon
 * @description: 链接提取执行器
 * @date 2025/6/22 12:24
 */
@Service
public class LinkExtractExe implements BaseExecutor<HtmlEntry> {

    private static final String NODE_NAME = "LinkExtract";

    private static final List<String> SKIP_EXTENSIONS = List.of(".pdf", ".doc", ".jpg", ".png", ".zip", ".exe", ".rar", ".apk");

    //@SuppressWarnings("all")
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
            Document doc = Jsoup.parse(html);
            Elements links = doc.select("a[href]");
            if (Iterables.isEmpty(links)) {
                return;
            }

            BlockingQueue<BaseEntry> urlQueue = content.getUrlQueue();
            BloomFilter<CharSequence> bloomFilter = content.getBloomFilter();
            AtomicLong docId = content.getDocId();

            for (Element link : links) {
                String url = link.absUrl("href");
                if (isHtmlUrl(url) && url.startsWith("http")) {
                    // 过滤掉重复的URL
                    synchronized (bloomFilter) {
                        if (!bloomFilter.mightContain(url)) {
                            long id = docId.incrementAndGet();
                            urlQueue.add(new BaseEntry(id, url));
                            bloomFilter.put(url);
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        } finally {
            // 手动清理ThreadLocal
            content.clearNodeData();
        }
    }

    private boolean isHtmlUrl(String url) {
        String lowerUrl = url.toLowerCase();
        for (String ext : SKIP_EXTENSIONS) {
            if (lowerUrl.endsWith(ext)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String nodeName() {
        return NODE_NAME;
    }
}
