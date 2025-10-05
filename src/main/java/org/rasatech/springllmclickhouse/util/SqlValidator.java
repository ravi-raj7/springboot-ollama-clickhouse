package org.rasatech.springllmclickhouse.util;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.rasatech.springllmclickhouse.model.TableMeta;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class SqlValidator {

    // max rows enforced
    private final int maxRows = 1000;

    public record ValidationResult(boolean valid, String sql, String message) {
        public static ValidationResult ok(String sql) {
            return new ValidationResult(true, sql, null);
        }

        public static ValidationResult invalid(String msg) {
            return new ValidationResult(false, null, msg);
        }
    }

    // simple table whitelist check based on provided candidates
    public ValidationResult validate(String rawSql, List<TableMeta> allowedTables) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(rawSql);
            if (!(stmt instanceof Select)) {
                return ValidationResult.invalid("Only SELECT queries allowed.");
            }
            Select select = (Select) stmt;

            // extract tables referenced
            TablesNamesFinder finder = new TablesNamesFinder();
            List<String> tables = finder.getTableList((Statement) select);

            Set<String> allowedFqns = allowedTables.stream()
                    .map(TableMeta::fqName).collect(Collectors.toSet());
            // also allow unqualified table names (table only)
            Set<String> allowedShort = allowedTables.stream()
                    .map(TableMeta::table).collect(Collectors.toSet());

            for (String t : tables) {
                String clean = t.replace("\"", "");
                if (!(allowedFqns.contains(clean) || allowedShort.contains(clean))) {
                    return ValidationResult.invalid("Referenced disallowed table: " + clean);
                }
            }

            // ensure limit
            if (!hasLimit(select)) {
                addLimit(select, maxRows);
            } else {
                int lim = getLimit(select);
                if (lim > maxRows) setLimit(select, maxRows);
            }

            // disallow certain constructs via simple string checks
            String s = select.toString().toLowerCase();
            if (s.contains("insert ") || s.contains("update ") || s.contains("delete ") ||
                    s.contains("create ") || s.contains("drop ") || s.contains("into outfile") ||
                    s.contains("system.") || s.contains("file(")) {
                return ValidationResult.invalid("Disallowed SQL constructs detected.");
            }

            return ValidationResult.ok(select.toString());
        } catch (JSQLParserException e) {
            return ValidationResult.invalid("SQL parse error: " + e.getMessage());
        } catch (Exception e) {
            return ValidationResult.invalid("Validation error: " + e.getMessage());
        }
    }

    private boolean hasLimit(Select sel) {
        Select body = sel.getSelectBody();
        if (body instanceof PlainSelect plain) {
            return plain.getLimit() != null;
        }
        return false;
    }

    private int getLimit(Select sel) {
        Select body = sel.getSelectBody();
        if (body instanceof PlainSelect plain) {
            Limit l = plain.getLimit();
            if (l != null && l.getRowCount() != null) {
                return Integer.parseInt(l.getRowCount().toString());
            }
        }
        return Integer.MAX_VALUE;
    }

    private void setLimit(Select sel, int limit) {
        Select body = sel.getSelectBody();
        if (body instanceof PlainSelect plain) {
            Limit l = new Limit();
            l.setRowCount(new net.sf.jsqlparser.expression.LongValue(limit));
            plain.setLimit(l);
        }
    }

    private void addLimit(Select sel, int limit) {
        setLimit(sel, limit);
    }
}
