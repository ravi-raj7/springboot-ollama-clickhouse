package org.rasatech.springllmclickhouse.util;


import org.rasatech.springllmclickhouse.model.TableMeta;

import java.util.List;

public class PromptBuilder {

    public static String build(String userPrompt, List<TableMeta> candidates) {
        StringBuilder sb = new StringBuilder();
        sb.append("System:\nYou are an expert ClickHouse SQL generator.\n");
        sb.append("Use ONLY the tables and columns listed below. Do NOT invent tables or columns.\n");
        sb.append("Return EXACTLY one SQL SELECT statement enclosed in triple backticks and nothing else.\n");
        sb.append("Use ClickHouse SQL dialect. Always include an ORDER BY when appropriate and a LIMIT (max 1000).\n\n");
        sb.append("Available tables:\n");
        for (var t : candidates) {
            sb.append("- ").append(t.fqName()).append(": [")
                    .append(String.join(", ", t.columns())).append("]\n");
        }
        sb.append("\nUser request:\n\"").append(userPrompt).append("\"\n\n");
        sb.append("Important: the SQL must be read-only, and must reference only the provided tables/columns.\n");
        sb.append("Output:\n```sql\n<your single SELECT statement here>\n```\n");
        return sb.toString();
    }

    public static String buildRepairPrompt(String brokenSql, String errorMessage, List<TableMeta> candidates) {
        String base = build("Fix the SQL to comply with allowed tables/columns.", candidates);
        StringBuilder sb = new StringBuilder(base);
        sb.append("\nPrevious SQL had error: ").append(errorMessage).append("\n");
        sb.append("Previous SQL:\n```\n").append(brokenSql).append("\n```\n");
        sb.append("Respond with corrected SQL ONLY inside triple backticks.\n");
        return sb.toString();
    }
}
