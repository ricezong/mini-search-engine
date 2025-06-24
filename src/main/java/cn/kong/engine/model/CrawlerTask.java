package cn.kong.engine.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/24 13:43
 */
@Getter
@Setter
public class CrawlerTask {
    /**
     * 任务ID，唯一标识一个爬虫任务
     */
    public String taskId;
    /**
     * 任务名称
     */
    public String taskName;
    /**
     * 请求的链接列表
     */
    public List<String> links;
    /**
     * 爬取的数量限制
     */
    public Long crawlQuantity;
    /**
     * 保存目录
     */
    public String saveDirectory;
    /**
     * 单个文件的最大大小，单位为字节
     */
    public Long maxFileSize = (long) (1024 * 1024 * 1024); // 1GB
    /**
     * 文件前缀
     */
    public String filePrefix;
    /**
     * 文件后缀
     */
    public String fileSuffix;

}
