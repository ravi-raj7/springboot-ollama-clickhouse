package org.rasatech.springllmclickhouse.model;

import java.util.List;

public record TableMeta(String database, String table, List<String> columns) {
    public String fqName() {
        return database + "." + table;
    }
}
