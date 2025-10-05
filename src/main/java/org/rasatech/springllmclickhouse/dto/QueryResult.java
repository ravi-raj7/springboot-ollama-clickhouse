package org.rasatech.springllmclickhouse.dto;

import java.util.List;
import java.util.Map;

public record QueryResult(boolean success, String message, List<Map<String, Object>> rows) {
    public static QueryResult ok(List<Map<String, Object>> rows) {
        return new QueryResult(true, null, rows);
    }

    public static QueryResult error(String msg) {
        return new QueryResult(false, msg, null);
    }
}
