package org.rasatech.springllmclickhouse.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.rasatech.springllmclickhouse.model.TableMeta;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class SchemaService {
    private final JdbcTemplate jdbc;
    private final Cache<String, List<TableMeta>> cache;
    private final int candidateTables;
    private final int candidateColumns;

    public SchemaService(JdbcTemplate jdbc, org.springframework.core.env.Environment env) {
        this.jdbc = jdbc;
        int ttl = Integer.parseInt(env.getProperty("schema.cache-ttl-minutes", "5"));
        this.cache = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(ttl)).build();
        this.candidateTables = Integer.parseInt(env.getProperty("schema.candidate-tables", "6"));
        this.candidateColumns = Integer.parseInt(env.getProperty("schema.candidate-columns-per-table", "12"));
    }

    // Full refresh (called lazily or scheduled externally if desired)
    public List<TableMeta> loadFullSchema() {
        // cache key constant
        String key = "full-schema";
        return cache.get(key, k -> fetchSchema());
    }

    private List<TableMeta> fetchSchema() {
        String sql = "SELECT database, table, name FROM system.columns";
        List<Map<String, Object>> rows = jdbc.queryForList(sql);
        Map<String, Set<String>> map = new HashMap<>();
        Map<String, String> dbForTable = new HashMap<>();
        for (var r : rows) {
            String db = r.get("database").toString();
            String table = r.get("table").toString();
            String col = r.get("name").toString();
            String key = db + "." + table;
            map.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(col);
            dbForTable.putIfAbsent(key, db);
        }
        List<TableMeta> metas = map.entrySet().stream().map(e -> {
            String[] parts = e.getKey().split("\\.", 2);
            String db = parts[0], table = parts[1];
            List<String> cols = e.getValue().stream().limit(1000).collect(Collectors.toList());
            return new TableMeta(db, table, cols);
        }).collect(Collectors.toList());
        return metas;
    }

    // keyword-based candidate selection + column pruning
    public List<TableMeta> findCandidateTables(String userQuery) {
        var full = loadFullSchema();

        // score tables based on user query
        List<ScoredTable> scored = new ArrayList<>();
        String q = userQuery == null ? "" : userQuery.toLowerCase();

        for (var t : full) {
            int score = 0;
            String tname = t.table().toLowerCase();
            if (q.contains(tname) || tname.contains(q)) score += 5;
            for (var c : t.columns()) {
                String col = c.toLowerCase();
                if (q.contains(col)) score += 2;
                if (col.contains(q)) score += 1;
            }
            scored.add(new ScoredTable(score, t));
        }

        scored.sort(Comparator.comparingInt(ScoredTable::score).reversed());

        // take top N tables (score>0) OR fallback to first N tables
        List<TableMeta> candidates = scored.stream().filter(s -> s.score() > 0).limit(candidateTables).map(ScoredTable::table).collect(Collectors.toList());

        if (candidates.isEmpty()) {
            candidates = full.stream().limit(candidateTables).collect(Collectors.toList());
        }

        // ⚡ IMPORTANT: include **all columns** for each table to prevent validator rejection
        List<TableMeta> safeCandidates = new ArrayList<>();
        for (var t : candidates) {
            safeCandidates.add(new TableMeta(t.database(), t.table(), t.columns())); // no pruning
        }

        return safeCandidates;
    }


    private record ScoredTable(int score, TableMeta table) {
    }

}
