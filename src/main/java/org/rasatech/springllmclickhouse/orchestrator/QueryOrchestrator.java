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
            // 1️⃣ Select candidate tables
            List<TableMeta> candidates = schemaService.findCandidateTables(userPrompt);

            // 2️⃣ Build prompt
            String prompt = PromptBuilder.build(userPrompt, candidates);

            // 3️⃣ Call LLM
            String rawSql = ollama.generateSql(prompt);
            if (rawSql == null || rawSql.isBlank()) return QueryResult.error("LLM returned empty SQL");

            // 4️⃣ Validate SQL
            var vr = validator.validate(rawSql, candidates);
            if (!vr.valid()) {
                // 5️⃣ Repair once if invalid
                String repairPrompt = PromptBuilder.buildRepairPrompt(rawSql, vr.message(), candidates);
                String repairedRaw = ollama.generateSql(repairPrompt);
                String repaired = repairedRaw.trim();
                var vr2 = validator.validate(repaired, candidates);
                if (!vr2.valid()) return QueryResult.error("Could not produce valid query: " + vr2.message());
                return QueryResult.ok(clickHouse.executeReadOnly(vr2.sql()));
            }

            return QueryResult.ok(clickHouse.executeReadOnly(vr.sql()));
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
