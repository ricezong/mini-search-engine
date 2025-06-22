package cn.kong.engine.processor.collect.entity;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:02
 */
public class HtmlEntry extends BaseEntry {

    private String html;

    public HtmlEntry() {
    }

    public HtmlEntry(String html) {
        this.html = html;
    }

    public HtmlEntry(Long id, String url, String html) {
        super(id, url);
        this.html = html;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }
}
