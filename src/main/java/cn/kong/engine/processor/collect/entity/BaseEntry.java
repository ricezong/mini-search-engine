package cn.kong.engine.processor.collect.entity;

/**
 * @author gzkon
 * @description: 爬虫基类
 * @date 2025/6/22 11:59
 */
public class BaseEntry {
    private Long id;             // 网页编号
    private String url;          // 网页URL

    public BaseEntry() {
    }

    public BaseEntry(Long id, String url) {
        this.id = id;
        this.url = url;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
