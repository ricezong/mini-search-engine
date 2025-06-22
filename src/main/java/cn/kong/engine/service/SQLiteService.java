package cn.kong.engine.service;

import cn.kong.engine.common.Constants;
import cn.kong.engine.processor.collect.entity.DocInfo;
import cn.kong.engine.utils.SqlGenerator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * @author gzkon
 * @description: TODO
 * @date 2025/6/22 12:22
 */
@Slf4j
@Service
public class SQLiteService implements AutoCloseable {
    // Database configuration
    private static final String DB_NAME = "doc_info.db";
    private static final String  DB_URL = "jdbc:sqlite:" + Constants.OUT_DIC + File.separator + DB_NAME;

    // Prepared SQL statements
    private static String INSERT_SQL;
    private static String CREATE_TABLE_SQL;
    private static final String SELECT_BY_ID_SQL = "SELECT * FROM doc_info WHERE id = ?";
    private static final String SELECT_ALL_SQL = "SELECT * FROM doc_info ORDER BY id";
    private static final String SELECT_PAGE_SQL = "SELECT * FROM doc_info ORDER BY id LIMIT ? OFFSET ?";
    private static final String COUNT_SQL = "SELECT COUNT(*) FROM doc_info";
    private static final String DELETE_SQL = "DELETE FROM doc_info WHERE id = ?";

    // Connection pool
    private static  HikariDataSource dataSource;
    private static final AtomicInteger batchCounter = new AtomicInteger(0);

    @PostConstruct
    void init() {
        // Initialize SQL
        INSERT_SQL = SqlGenerator.generateInsertSql(DocInfo.class);
        CREATE_TABLE_SQL = SqlGenerator.generateCreateTable(DocInfo.class);

        // Configure connection pool
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setPoolName("SQLite-HikariPool");

        // Optimization settings
        config.setMaximumPoolSize(Math.max(4, Runtime.getRuntime().availableProcessors() * 2));
        config.setMinimumIdle(2);
        config.setConnectionTimeout(3000);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(180000);
        config.setAutoCommit(false);

        // SQLite optimizations
        config.addDataSourceProperty("journal_mode", "WAL");
        config.addDataSourceProperty("synchronous", "NORMAL");
        config.addDataSourceProperty("busy_timeout", 5000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
        initializeDatabase();
        log.info("SQLite database initialized successfully");
    }

    // ================ Initialization Methods ================
    private static void initializeDatabase() {
        executeUpdate(CREATE_TABLE_SQL);
        executeUpdate("CREATE INDEX IF NOT EXISTS idx_doc_info_url ON doc_info(url)");
        executeUpdate("CREATE INDEX IF NOT EXISTS idx_doc_info_status ON doc_info(status_code)");
        executeUpdate("CREATE INDEX IF NOT EXISTS idx_doc_info_domain ON doc_info(domain)");
    }

    // ================ CRUD Operations ================

    public boolean insert(DocInfo doc) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {

            stmt.setLong(1, doc.getId());
            stmt.setString(2, doc.getUrl());
            stmt.setString(3, doc.getTitle());
            stmt.setString(4, doc.getDomain());
            stmt.setInt(5, doc.getStatusCode());
            stmt.setInt(6, doc.isStored() ? 1 : 0);
            stmt.setString(7, doc.getContentType());
            stmt.setLong(8, doc.getContentLength());
            stmt.setTimestamp(9, Timestamp.valueOf(doc.getCreateTime()));
            stmt.setTimestamp(10, doc.getUpdateTime() != null ?
                    Timestamp.valueOf(doc.getUpdateTime()) : null);

            int affectedRows = stmt.executeUpdate();
            conn.commit();
            return affectedRows > 0;
        }catch (SQLException e) {
            log.error("Insert failed for document ID: {}", doc.getId(), e);
            return false;
        }
    }

    public int insertBatch(List<DocInfo> docs) {
        if (docs == null || docs.isEmpty()) {
            return 0;
        }

        final int batchSize = 500;
        int totalInserted = 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                for (DocInfo doc : docs) {
                    stmt.setLong(1, doc.getId());
                    stmt.setString(2, doc.getUrl());
                    stmt.setString(3, doc.getTitle());
                    stmt.setString(4, doc.getDomain());
                    stmt.setObject(5, doc.getStatusCode());
                    stmt.setObject(6, doc.isStored() ? 1 : 0);
                    stmt.setString(7, doc.getContentType());
                    stmt.setObject(8, doc.getContentLength());
                    stmt.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
                    stmt.setTimestamp(10, doc.getUpdateTime() != null ?
                            Timestamp.valueOf(doc.getUpdateTime()) : null);

                    stmt.addBatch();

                    if (batchCounter.incrementAndGet() % batchSize == 0) {
                        totalInserted += executeBatch(stmt, conn);
                    }
                }

                totalInserted += executeBatch(stmt, conn);
                conn.commit();
                return totalInserted;
            }
        } catch (SQLException e) {
            log.error("Batch insert failed, inserted {} records", totalInserted, e);
            return totalInserted;
        }
    }

    public boolean update(DocInfo doc) {
        if (doc == null || doc.getId() == null) {
            return false;
        }

        // Dynamically generate update SQL based on non-null fields
        StringBuilder sqlBuilder = new StringBuilder("UPDATE doc_info SET ");
        List<Object> params = new ArrayList<>();
        List<String> setClauses = new ArrayList<>();

        if (doc.getUrl() != null) {
            setClauses.add("url = ?");
            params.add(doc.getUrl());
        }
        if (doc.getTitle() != null) {
            setClauses.add("title = ?");
            params.add(doc.getTitle());
        }
        if (doc.getDomain() != null) {
            setClauses.add("domain = ?");
            params.add(doc.getDomain());
        }
        if (doc.getStatusCode() != null) {
            setClauses.add("status_code = ?");
            params.add(doc.getStatusCode());
        }
        if (doc.isStored()) {
            setClauses.add("stored = ?");
            params.add(1);
        }
        if (doc.getContentType() != null) {
            setClauses.add("content_type = ?");
            params.add(doc.getContentType());
        }
        if (doc.getContentLength() != null) {
            setClauses.add("content_length = ?");
            params.add(doc.getContentLength());
        }
        if (doc.getCreateTime() != null) {
            setClauses.add("create_time = ?");
            params.add(Timestamp.valueOf(doc.getCreateTime()));
        }
        if (doc.getUpdateTime() != null) {
            setClauses.add("update_time = ?");
            params.add(Timestamp.valueOf(doc.getUpdateTime()));
        }

        if (setClauses.isEmpty()) {
            return false;
        }

        sqlBuilder.append(String.join(", ", setClauses));
        sqlBuilder.append(" WHERE id = ?");
        params.add(doc.getId());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlBuilder.toString())) {

            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof String) {
                    stmt.setString(i + 1, (String) param);
                } else if (param instanceof Integer) {
                    stmt.setInt(i + 1, (Integer) param);
                } else if (param instanceof Long) {
                    stmt.setLong(i + 1, (Long) param);
                } else if (param instanceof Timestamp) {
                    stmt.setTimestamp(i + 1, (Timestamp) param);
                } else if (param == null) {
                    stmt.setNull(i + 1, Types.NULL);
                }
            }

            int affectedRows = stmt.executeUpdate();
            conn.commit();
            return affectedRows > 0;
        } catch (SQLException e) {
            log.error("Update failed for document ID: {}", doc.getId(), e);
            return false;
        }
    }

    public boolean deleteById(Long id) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(DELETE_SQL)) {

            stmt.setLong(1, id);
            int affectedRows = stmt.executeUpdate();
            conn.commit();
            return affectedRows > 0;
        } catch (SQLException e) {
            log.error("Delete failed for ID: {}", id, e);
            return false;
        }
    }

    // ================ Query Operations ================

    public Optional<DocInfo> selectById(Long id) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_ID_SQL)) {

            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapToDocInfo(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Query failed for ID: {}", id, e);
        }
        return Optional.empty();
    }

    public List<DocInfo> selectAll() {
        List<DocInfo> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_ALL_SQL);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                result.add(mapToDocInfo(rs));
            }
        } catch (SQLException e) {
            log.error("Failed to select all documents", e);
        }
        return result;
    }

    public List<DocInfo> selectByPage(int page, int size) {
        List<DocInfo> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_PAGE_SQL)) {

            stmt.setInt(1, size);
            stmt.setInt(2, (page - 1) * size);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapToDocInfo(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to select page {} with size {}", page, size, e);
        }
        return result;
    }

    public long count() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(COUNT_SQL);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Failed to count documents", e);
        }
        return 0L;
    }

    // ================ Helper Methods ================

    private int executeBatch(PreparedStatement stmt, Connection conn) throws SQLException {
        int[] results = stmt.executeBatch();
        stmt.clearBatch();

        int successCount = 0;
        for (int result : results) {
            if (result >= 0 || result == Statement.SUCCESS_NO_INFO) {
                successCount++;
            }
        }

        if (batchCounter.get() % 10 == 0) {
            conn.commit();
        }

        return successCount;
    }

    private DocInfo mapToDocInfo(ResultSet rs) {
        DocInfo doc = new DocInfo();
        try {
            doc.setId(rs.getLong("id"));
            doc.setUrl(rs.getString("url"));
            doc.setTitle(rs.getString("title"));
            doc.setDomain(rs.getString("domain"));
            doc.setStatusCode(rs.getInt("status_code"));
            doc.setStored(rs.getInt("stored") == 1);
            doc.setContentType(rs.getString("content_type"));
            doc.setContentLength(rs.getLong("content_length"));

            Timestamp createTime = rs.getTimestamp("create_time");
            if (createTime != null) {
                doc.setCreateTime(createTime.toLocalDateTime());
            }

            Timestamp updateTime = rs.getTimestamp("update_time");
            if (updateTime != null) {
                doc.setUpdateTime(updateTime.toLocalDateTime());
            }
        } catch (SQLException e) {
            log.error("Data mapping failed", e);
        }
        return doc;
    }

    // ================ Resource Management ================

    public static void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("Database connection pool closed");
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    private static void executeUpdate(String sql) {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            conn.commit();
        } catch (SQLException e) {
            log.error("SQL execution failed: {}", sql, e);
        }
    }
}
