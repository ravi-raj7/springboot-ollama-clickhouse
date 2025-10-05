package org.rasatech.springllmclickhouse.service;


import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Service
public class ClickHouseService {
    private final DataSource ds;
    private final int queryTimeoutSeconds;

    public ClickHouseService(DataSource ds, org.springframework.core.env.Environment env) {
        this.ds = ds;
        this.queryTimeoutSeconds = Integer.parseInt(env.getProperty("clickhouse.query-timeout-seconds", "30"));
    }

    public List<Map<String, Object>> executeReadOnly(String sql) {
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement()) {
            // Optional: set ClickHouse session settings for resource limits
            try {
                st.execute("SET max_result_rows = 100000");
            } catch (Exception ignored) {
            }
            try {
                st.execute("SET max_execution_time = 30");
            } catch (Exception ignored) {
            }
            st.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = st.executeQuery(sql)) {
                return toList(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException("ClickHouse exec error: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> toList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }
}
