package cn.kong.engine.utils;

import cn.kong.engine.annotation.DbField;
import cn.kong.engine.annotation.DbTable;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 11:53
 */
public class SqlGenerator {
    /**
     * 生成建表SQL
     */
    public static String generateCreateTable(Class<?> clazz) {
        DbTable tableAnnotation = clazz.getAnnotation(DbTable.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Class must have @DbTable annotation");
        }

        String tableName = tableAnnotation.name();
        List<String> columns = new ArrayList<>();

        for (Field field : clazz.getDeclaredFields()) {
            DbField dbField = field.getAnnotation(DbField.class);
            if (dbField != null) {
                String columnDef = buildColumnDefinition(field, dbField);
                columns.add(columnDef);
            }
        }

        return  "CREATE TABLE IF NOT EXISTS " +
                tableName + " (\n    " +
                String.join(",\n    ", columns) +
                "\n)";

    }

    /**
     * 生成插入SQL
     */
    public static String generateInsertSql(Class<?> clazz) {
        DbTable tableAnnotation = clazz.getAnnotation(DbTable.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Class must have @DbTable annotation");
        }

        List<String> columns = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();

        for (Field field : clazz.getDeclaredFields()) {
            DbField dbField = field.getAnnotation(DbField.class);
            if (dbField != null && !dbField.autoIncrement()) {
                columns.add(getColumnName(field, dbField));
                placeholders.add("?");
            }
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableAnnotation.name(),
                String.join(", ", columns),
                String.join(", ", placeholders));
    }

    private static String buildColumnDefinition(Field field, DbField dbField) {
        StringBuilder columnDef = new StringBuilder(getColumnName(field, dbField))
                .append(" ").append(getSqlType(field, dbField));

        if (dbField.primaryKey()) {
            columnDef.append(" PRIMARY KEY");
        }
        if (dbField.autoIncrement()) {
            columnDef.append(" AUTOINCREMENT");
        }
        if (!dbField.nullable()) {
            columnDef.append(" NOT NULL");
        }
        if (!dbField.defaultValue().isEmpty()) {
            columnDef.append(" DEFAULT ").append(dbField.defaultValue());
        }

        return columnDef.toString();
    }

    private static String getColumnName(Field field, DbField dbField) {
        return dbField.name().isEmpty() ? field.getName() : dbField.name();
    }

    private static String getSqlType(Field field, DbField dbField) {
        // 优先使用注解中指定的类型
        if (!dbField.type().isEmpty()) {
            return dbField.type();
        }

        // 默认类型推断
        Class<?> type = field.getType();

        if (type == String.class) {
            return "TEXT";
        } else if (type == Integer.class || type == int.class) {
            return "INTEGER";
        } else if (type == Long.class || type == long.class) {
            return "INTEGER";
        } else if (type == Boolean.class || type == boolean.class) {
            return "INTEGER";
        } else if (type == LocalDateTime.class) {
            return "TIMESTAMP";
        }
        return "TEXT";
    }
}
