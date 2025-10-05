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
        this.webClient = webClientBuilder.baseUrl("http://localhost:11434").build(); // Ollama server URL
    }

    /**
     * Generates a ClickHouse SQL query from a natural language prompt.
     * Extracts the SQL inside triple backticks.
     */
    public String generateSql(String prompt) {
        try {
            var body = Map.of(
                    "model", "sqlcoder:7b",
                    "prompt", prompt,
                    "stream", false
            );

            String response = webClient.post()
                    .uri("/api/generate") // ✅ Correct Ollama endpoint
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(body)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(60))
                    .block();

            // ✅ Extract the SQL portion from the response
            return extractSql(response);

        } catch (Exception e) {
            log.error("LLM request failed for prompt: {}", prompt, e);
            throw new RuntimeException("LLM request failed: " + e.getMessage(), e);
        }
    }


    /**
     * Extracts the SQL query inside triple backticks.
     * Returns null if no SQL found.
     */
    private String extractSql(String response) {
        if (response == null || response.isBlank()) {
            throw new RuntimeException("Empty response from LLM");
        }

        // Match content between ```sql ... ```
        Pattern p = Pattern.compile("```sql\\s*(.*?)\\s*```", Pattern.DOTALL);
        Matcher m = p.matcher(response);
        if (m.find()) {
            return m.group(1).trim();
        }

        // Fallback: just pick first SELECT query
        p = Pattern.compile("(SELECT\\s+.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        m = p.matcher(response);
        if (m.find()) {
            return m.group(1).trim();
        }

        log.warn("No SQL found in LLM response: {}", response);
        throw new RuntimeException("Could not extract SQL from response");
    }


    /**
     * Escapes JSON special characters in prompt
     */
    private String escapeJson(String str) {
        return str.replace("\"", "\\\"").replace("\n", "\\n");
    }
}
