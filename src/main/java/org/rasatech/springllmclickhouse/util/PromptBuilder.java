package org.rasatech.springllmclickhouse.util;

import org.rasatech.springllmclickhouse.model.TableMeta;

import java.util.List;
import java.util.stream.Collectors;

public class PromptBuilder {

    /**
     * Builds a prompt for the LLM ensuring:
     * - Only allowed tables/columns are used
     * - ClickHouse syntax
     * - One SELECT statement
     * - ORDER BY and LIMIT applied
     * - Fully-qualified or table-only names allowed
     */
    public static String build(String userPrompt, List<TableMeta> tables) {
        StringBuilder sb = new StringBuilder();

        sb.append("System:\n");
        sb.append("You are an expert ClickHouse SQL generator.\n");
        sb.append("Return EXACTLY one SELECT statement enclosed in triple backticks.\n");
        sb.append("Use ONLY the tables and columns listed below. Do NOT invent any tables or columns.\n");
        sb.append("Use ClickHouse SQL syntax. Include ORDER BY and LIMIT (max 1000) where appropriate.\n\n");

        sb.append("Available tables and columns:\n");
        for (TableMeta t : tables) {
            sb.append("- ").append(t.fqName()).append(": [").append(String.join(", ", t.columns())).append("]\n");
        }

        sb.append("\nUser request:\n\"").append(userPrompt).append("\"\n\n");
        sb.append("Important: Only reference the tables and columns listed above. You may use fully-qualified names (db.table) or table-only names.\n");
        sb.append("Output your single SELECT statement enclosed in triple backticks, nothing else:\n```sql\n<your SELECT here>\n```\n");

        return sb.toString();
    }

    /**
     * Builds a repair prompt for the LLM when previous SQL had errors.
     * Provides broken SQL, the error message, and allowed tables/columns.
     */
    public static String buildRepairPrompt(String brokenSql, String errorMessage, List<TableMeta> tables) {
        StringBuilder sb = new StringBuilder();

        sb.append("System:\n");
        sb.append("You are an expert ClickHouse SQL generator.\n");
        sb.append("Fix the following SQL query so it is valid and only uses allowed tables/columns.\n\n");

        sb.append("Error encountered:\n").append(errorMessage).append("\n\n");

        sb.append("Previous SQL:\n```sql\n").append(brokenSql).append("\n```\n\n");

        sb.append("Available tables and columns:\n");
        for (TableMeta t : tables) {
            sb.append("- ").append(t.fqName()).append(": [").append(String.join(", ", t.columns())).append("]\n");
        }

        sb.append("\nOutput corrected SQL ONLY, enclosed in triple backticks:\n```sql\n<corrected SQL here>\n```\n");

        return sb.toString();
    }
}
