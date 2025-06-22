package cn.kong.engine.utils;

import java.util.BitSet;

/**
 * @description: 定制布隆过滤器
 * @author: kong
 * @date: 2025-06-06 16:07
 */
public class CustomBloomFilter {

    private static final int SIZE = 100_000_000;    // 表示位图 BitSet 的大小，即最多可以映射 1 亿个 bit
    private static final BitSet bitSet = new BitSet(SIZE); // 大概12MB
    private static final int[] seeds = {5, 7, 11, 13, 31, 37, 61}; // 哈希种子

    public static boolean isDuplicate(String url) {
        boolean exists = true;
        for (int seed : seeds) {
            int hash = hash(url, seed);
            if (!bitSet.get(hash)) {
                exists = false;
                System.out.println("布隆url:" + url);
                bitSet.set(hash);
            }
        }
        return exists;
    }

    private static int hash(String str, int seed) {
        int result = 0;
        for (char c : str.toCharArray()) {
            result = seed * result + c;
        }
        return (SIZE - 1) & result;
    }
}
