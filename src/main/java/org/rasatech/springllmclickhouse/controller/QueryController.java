package org.rasatech.springllmclickhouse.controller;

import org.rasatech.springllmclickhouse.dto.QueryRequest;
import org.rasatech.springllmclickhouse.dto.QueryResult;
import org.rasatech.springllmclickhouse.orchestrator.QueryOrchestrator;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class QueryController {
    private final QueryOrchestrator orchestrator;

    public QueryController(QueryOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping(value = "/nl-query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<QueryResult> nlQuery(@RequestBody QueryRequest req) {
        var result = orchestrator.handleNaturalLanguage(req.nlQuery(), req.userId());
        return ResponseEntity.ok(result);
    }
}
