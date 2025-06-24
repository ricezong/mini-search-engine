package cn.kong.engine.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.PostConstruct;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
@EnableConfigurationProperties({CrawlerConfig.HttpPoolProperties.class, CrawlerConfig.ThreadPoolProperties.class})
public class CrawlerConfig {

    @ConfigurationProperties(prefix = "crawler.http")
    @Getter
    @Setter
    public static class HttpPoolProperties {
        private int maxIdleConnections;
        private int keepAliveDuration; // 分钟
    }

    @ConfigurationProperties(prefix = "crawler.thread-pool")
    @Getter
    @Setter
    public static class ThreadPoolProperties {
        private int coreSize;
        private int maxSize;
        private int keepAliveSeconds;
        private String threadNamePrefix;
        private String rejectedPolicy; // ABORT, CALLER_RUNS, DISCARD, DISCARD_OLDEST

        @PostConstruct
        public void validate() {
            if (coreSize <= 0 || maxSize < coreSize || keepAliveSeconds <= 0) {
                throw new IllegalArgumentException("Invalid thread pool configuration");
            }
            log.info("Thread pool config loaded: coreSize={}, maxSize={}", coreSize, maxSize);
        }
    }

    @Bean
    public OkHttpClient okHttpClient(HttpPoolProperties properties) {
        return new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(
                        properties.getMaxIdleConnections(),
                        properties.getKeepAliveDuration(),
                        TimeUnit.MINUTES))
                .followRedirects(true)
                .retryOnConnectionFailure(true)
                .build();
    }

    @Bean(name = "crawlerThreadPool")
    public ThreadPoolTaskExecutor crawlerThreadPool(ThreadPoolProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // 核心配置
        executor.setCorePoolSize(properties.getCoreSize());
        executor.setMaxPoolSize(properties.getMaxSize());
        executor.setKeepAliveSeconds(properties.getKeepAliveSeconds());
        executor.setThreadNamePrefix(properties.getThreadNamePrefix());
        executor.setQueueCapacity(Integer.MAX_VALUE);

        // 拒绝策略
        RejectedPolicy policy = RejectedPolicy.choose(properties.getRejectedPolicy());
        executor.setRejectedExecutionHandler(policy.getHandler());

        executor.initialize();
        log.info("Created crawler thread pool with unbounded queue");
        return executor;
    }

    // 拒绝策略枚举
    private enum RejectedPolicy {
        ABORT(new ThreadPoolExecutor.AbortPolicy()),
        CALLER_RUNS(new ThreadPoolExecutor.CallerRunsPolicy()),
        DISCARD(new ThreadPoolExecutor.DiscardPolicy()),
        DISCARD_OLDEST(new ThreadPoolExecutor.DiscardOldestPolicy());

        private final RejectedExecutionHandler handler;

        RejectedPolicy(RejectedExecutionHandler handler) {
            this.handler = handler;
        }

        public RejectedExecutionHandler getHandler() {
            return handler;
        }

        // 根据策略名称获取对应的拒绝策略
        public static RejectedPolicy choose(String value) {
            try {
                return valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                log.warn("Invalid rejected policy '{}', using default ABORT", value);
                return ABORT;  // 如果策略无效，则默认使用 ABORT
            }
        }
    }
}