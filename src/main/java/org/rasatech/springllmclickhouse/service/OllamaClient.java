package org.rasatech.springllmclickhouse.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
public class OllamaClient {

    private final WebClient webClient;

    public OllamaClient(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.baseUrl("http://localhost:11434").build();
    }

    public Mono<String> generateSqlAsync(String prompt) {
        var body = Map.of(
                "model", "sqlcoder:7b",
                "prompt", prompt,
                "stream", false
        );

        return webClient.post()
                .uri("/api/generate")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofMinutes(2))
                .onErrorResume(e -> {
                    log.error("LLM async call failed", e);
                    return Mono.empty();
                })
                .map(this::extractSql);
    }

    private String extractSql(String response) {
        Pattern p = Pattern.compile("```sql\\s*(.*?)\\s*```", Pattern.DOTALL);
        Matcher m = p.matcher(response);
        if (m.find()) return m.group(1).trim();
        // fallback: first SELECT
        p = Pattern.compile("(SELECT\\s+.*;)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        m = p.matcher(response);
        if (m.find()) return m.group(1).trim();
        return response.trim();
    }
}