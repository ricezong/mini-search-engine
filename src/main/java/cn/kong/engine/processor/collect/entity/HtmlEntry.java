package cn.kong.engine.processor.collect.entity;

/**
 * @author gzkon
 * @description: HTML网页实体类
 * @date 2025/6/22 12:02
 */
public class HtmlEntry extends BaseEntry {

    private String title;
    private String html;

    public HtmlEntry() {
    }

    public HtmlEntry(String title, String html) {
        this.title = title;
        this.html = html;
    }

    public HtmlEntry(Long id, String url, String title, String html) {
        super(id, url);
        this.title = title;
        this.html = html;
    }

    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }
}
