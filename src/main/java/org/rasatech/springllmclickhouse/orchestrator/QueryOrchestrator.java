package org.rasatech.springllmclickhouse.orchestrator;

import org.rasatech.springllmclickhouse.dto.QueryResult;
import org.rasatech.springllmclickhouse.model.TableMeta;
import org.rasatech.springllmclickhouse.service.ClickHouseService;
import org.rasatech.springllmclickhouse.service.OllamaClient;
import org.rasatech.springllmclickhouse.service.SchemaService;
import org.rasatech.springllmclickhouse.util.PromptBuilder;
import org.rasatech.springllmclickhouse.util.SqlValidator;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class QueryOrchestrator {
    private final SchemaService schemaService;
    private final OllamaClient ollama;
    private final SqlValidator validator;
    private final ClickHouseService clickHouse;

    public QueryOrchestrator(SchemaService schemaService, OllamaClient ollama,
                             SqlValidator validator, ClickHouseService clickHouse) {
        this.schemaService = schemaService;
        this.ollama = ollama;
        this.validator = validator;
        this.clickHouse = clickHouse;
    }

    public QueryResult handleNaturalLanguage(String userPrompt, String userId) {
        try {
            // 1. choose candidate tables
            List<TableMeta> candidates = schemaService.findCandidateTables(userPrompt);

            // 2. build prompt
            String prompt = PromptBuilder.build(userPrompt, candidates);

            // 3. call LLM
            String raw = String.valueOf(ollama.generateSql(prompt));
            if (raw == null || raw.isBlank()) return QueryResult.error("LLM returned empty response");

            // extract SQL block if returned in triple backticks
            String extracted = extractSql(raw);

            // 4. validate
            var vr = validator.validate(extracted, candidates);
            if (!vr.valid()) {
                // attempt repair once
                String repairPrompt = PromptBuilder.buildRepairPrompt(extracted, vr.message(), candidates);
                String repairedRaw = String.valueOf(ollama.generateSql(repairPrompt));
                String repaired = extractSql(repairedRaw);
                var vr2 = validator.validate(repaired, candidates);
                if (!vr2.valid()) return QueryResult.error("Could not produce valid query: " + vr2.message());
                var rows = clickHouse.executeReadOnly(vr2.sql());
                return QueryResult.ok(rows);
            }

            var rows = clickHouse.executeReadOnly(vr.sql());
            return QueryResult.ok(rows);
        } catch (Exception e) {
            return QueryResult.error("Internal error: " + e.getMessage());
        }
    }

    private String extractSql(String raw) {
        // crude extraction: take content between ``` and ```
        int a = raw.indexOf("```");
        if (a >= 0) {
            int b = raw.indexOf("```", a + 3);
            if (b > a) {
                return raw.substring(a + 3, b).trim();
            }
        }
        // fallback: return entire text
        return raw.trim();
    }
}
