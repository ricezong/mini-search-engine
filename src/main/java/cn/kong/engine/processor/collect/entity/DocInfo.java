package cn.kong.engine.processor.collect.entity;

import cn.kong.engine.annotation.DbField;
import cn.kong.engine.annotation.DbTable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:05
 */
@Getter
@Setter
@NoArgsConstructor
@DbTable(name = "doc_info")
public class DocInfo {

    @DbField(primaryKey = true)
    private Long id;                    // 主键ID

    @DbField(name = "url")
    private String url;                 // 网页URL

    @DbField(name = "title")
    private String title;               // 网页标题

    @DbField(name = "domain")
    private String domain;              // 域名

    @DbField(name = "status_code")
    private Integer statusCode;         // HTTP状态码 (200, 404等)

    @DbField(name = "stored", defaultValue = "0")
    private boolean stored;             // 内容是否已存储

    @DbField(name = "content_type")
    private String contentType;         // 内容类型 (text/html, application/json等)

    @DbField(name = "content_length")
    private Long contentLength;         // 内容长度(字节)

    @DbField(name = "create_time")
    private LocalDateTime createTime;   // 记录创建时间

    @DbField(name = "update_time")
    private LocalDateTime updateTime;   // 最后更新时间

}