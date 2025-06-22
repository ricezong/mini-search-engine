package cn.kong.engine.processor.collect.job;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlerConfig {

    public static OkHttpClient createHttpClient() {
        return new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(10, 5, TimeUnit.MINUTES))
                .followRedirects(true)
                .retryOnConnectionFailure(true)
                .build();
    }

    public static ExecutorService createThreadPool() {
        int coreThreads = 10;
        int maxThreads = 50;
        int queueSize = Integer.MAX_VALUE;

        return new ThreadPoolExecutor(
                coreThreads,
                maxThreads,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize),
                new ThreadFactory() {
                    private final AtomicInteger count = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName("crawler-thread-" + count.getAndIncrement());
                        return t;
                    }
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @SuppressWarnings("all")
    public static BloomFilter<CharSequence> createBloomFilter() {
        return BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                100_000_000,
                0.01);
    }

}
