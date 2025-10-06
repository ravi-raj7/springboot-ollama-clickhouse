package org.rasatech.springllmclickhouse.orchestrator;

import org.rasatech.springllmclickhouse.dto.QueryResult;
import org.rasatech.springllmclickhouse.model.TableMeta;
import org.rasatech.springllmclickhouse.service.ClickHouseService;
import org.rasatech.springllmclickhouse.service.OllamaClient;
import org.rasatech.springllmclickhouse.service.SchemaService;
import org.rasatech.springllmclickhouse.util.PromptBuilder;
import org.rasatech.springllmclickhouse.util.SqlValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
public class QueryOrchestrator {
    private static final Logger LOG = LoggerFactory.getLogger(QueryOrchestrator.class);

    private final SchemaService schemaService;
    private final OllamaClient ollama;
    private final SqlValidator validator;
    private final ClickHouseService clickHouse;
    private final int maxRetries = 3;

    public QueryOrchestrator(SchemaService schemaService, OllamaClient ollama,
                             SqlValidator validator, ClickHouseService clickHouse) {
        this.schemaService = schemaService;
        this.ollama = ollama;
        this.validator = validator;
        this.clickHouse = clickHouse;
    }

    public QueryResult handleNaturalLanguage(String userPrompt, String userId) {
        try {
            List<TableMeta> candidates = schemaService.findCandidateTables(userPrompt);

            String prompt = PromptBuilder.build(userPrompt, candidates);

            return attemptGenerateAndValidate(prompt, candidates, 0);

        } catch (Exception e) {
            LOG.error("Error handling natural language query", e);
            return QueryResult.error("Internal error: " + e.getMessage());
        }
    }

    private QueryResult attemptGenerateAndValidate(String prompt, List<TableMeta> candidates, int attempt) {
        if (attempt >= maxRetries) {
            return QueryResult.error("Failed to generate valid SQL after " + maxRetries + " attempts.");
        }

        Mono<String> sqlMono = ollama.generateSqlAsync(prompt);

        String rawSql = sqlMono.block(); // Block for simplicity; can fully async if using WebFlux controller
        if (rawSql == null || rawSql.isBlank()) {
            return QueryResult.error("LLM returned empty SQL");
        }

        var vr = validator.validate(rawSql, candidates);
        if (!vr.valid()) {
            LOG.warn("Validation failed on attempt {}: {}", attempt + 1, vr.message());
            String repairPrompt = PromptBuilder.buildRepairPrompt(rawSql, vr.message(), candidates);
            return attemptGenerateAndValidate(repairPrompt, candidates, attempt + 1);
        }

        // Optional: Check ClickHouse EXPLAIN SYNTAX
        if (!clickHouse.validateSyntax(vr.sql())) {
            LOG.warn("ClickHouse EXPLAIN failed on attempt {} for SQL: {}", attempt + 1, vr.sql());
            String repairPrompt = PromptBuilder.buildRepairPrompt(rawSql, "ClickHouse syntax invalid", candidates);
            return attemptGenerateAndValidate(repairPrompt, candidates, attempt + 1);
        }

        // SQL is valid, execute
        List<java.util.Map<String, Object>> rows = clickHouse.executeReadOnly(vr.sql());
        return QueryResult.ok(rows);
    }
}