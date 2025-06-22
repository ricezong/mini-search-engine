package cn.kong.engine.processor.collect.executor;

import cn.kong.engine.processor.collect.job.CrawlerContent;
import cn.kong.engine.processor.collect.entity.BaseEntry;
import cn.kong.engine.processor.collect.entity.HtmlEntry;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:28
 */
@Slf4j
@Service
public class RequestExe implements BaseExecutor<BaseEntry> {

    private static final String NODE_NAME = "Request";

    private static final Random RANDOM = new Random();

    // 随机User-Agent
    public static final List<String> USER_AGENTS = List.of(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/112.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1"
    );


    @Override
    public void execute(BaseEntry entry, CrawlerContent content) {
        OkHttpClient httpClient = content.getHttpClient();
        String url = entry.getUrl();
        Request request = buildRequest(url, content);
        //log.info("Fetching {}", url);

        try (Response response = httpClient.newCall(request).execute()) {
            handleResponse(response, content, entry);
        } catch (IOException e) {
            log.error("Error fetching {}: {}", url, e.getMessage());
        }
    }

    private Request buildRequest(String url, CrawlerContent content) {
        Request.Builder builder = new Request.Builder().url(url);
        builder.header("User-Agent", USER_AGENTS.get(RANDOM.nextInt(USER_AGENTS.size())));

        // 添加自定义headers
        /*Map<String, String> customHeaders = content.getCustomHeaders();
        if (customHeaders != null) {
            customHeaders.forEach(builder::header);
        }*/

        return builder.build();
    }

    private void handleResponse(Response response, CrawlerContent content, BaseEntry entry) {
        if (!response.isSuccessful()) {
            log.error("HTTP error: {} - {}", response.code(), response.message());
            return;
        }

        ResponseBody body = response.body();
        if (body == null) {
            log.error("Empty response body");
            return;
        }

        try {
            String html = body.string();
            content.putNodeData(NODE_NAME, of(html, entry));
        } catch (IOException e) {
            log.error("Error reading response body", e);
        }
    }


    @Override
    public String nodeName() {
        return NODE_NAME;
    }

    private HtmlEntry of(String html, BaseEntry entry) {
        HtmlEntry htmlEntry = new HtmlEntry();
        htmlEntry.setId(entry.getId());
        htmlEntry.setUrl(entry.getUrl());
        htmlEntry.setHtml(html);
        return htmlEntry;
    }
}
