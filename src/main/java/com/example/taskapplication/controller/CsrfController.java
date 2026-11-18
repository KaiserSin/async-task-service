package com.example.taskapplication.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@SecurityRequirement(name = "basicAuth")
public class CsrfController {

    @GetMapping("/api/csrf")
    @Operation(summary = "Get a CSRF token for authenticated state-changing requests")
    public ResponseEntity<Map<String, String>> csrf(CsrfToken token) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
            .body(Map.of("token", token.getToken(), "headerName", token.getHeaderName()));
    }
}
