package cn.kong.engine.common;

import java.io.File;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 11:57
 */
public class Constants {

    public static final String OUT_DIC = System.getProperty("user.dir") + File.separator + "out";    // 输出目录
    public static final byte[] FIELD_SEPARATOR = new byte[]{0x1F};  // 字段分隔符
    public static final byte[] RECORD_SEPARATOR = new byte[]{0x1E}; // 单条记录分隔符
    public static final byte[] DOC_TERMINATOR = new byte[]{0x17};   // 文件结束标记
    public static final String FILE_PREFIX = "doc_raw_";    // 文件前缀
    public static final String FILE_SUFFIX = ".bin";    // 文件后缀

}
