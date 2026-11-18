package com.example.taskapplication.config;

import com.example.taskapplication.dto.ErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;

import java.io.IOException;

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(ApiSecurityProperties.class)
@SecurityScheme(name = "basicAuth", type = SecuritySchemeType.HTTP, scheme = "basic")
public class SecurityConfig {

    @Bean
    PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    UserDetailsService users(ApiSecurityProperties properties, PasswordEncoder encoder) {
        var users = new InMemoryUserDetailsManager();
        for (var account : properties.users()) {
            users.createUser(User.withUsername(account.username())
                .password(encoder.encode(account.password()))
                .roles(account.roles().toArray(String[]::new))
                .build());
        }
        return users;
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http, ObjectMapper mapper) throws Exception {
        AuthenticationEntryPoint entryPoint = (request, response, failure) -> {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"task-api\"");
            writeError(response, mapper, 401, "UNAUTHORIZED", "Authentication is required");
        };
        AccessDeniedHandler denied = (request, response, failure) ->
            writeError(response, mapper, 403, "FORBIDDEN", "Access is denied");

        return http
            .authorizeHttpRequests(access -> access
                .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                .requestMatchers(HttpMethod.GET, "/actuator/health",
                    "/actuator/health/readiness", "/actuator/health/liveness").permitAll()
                .requestMatchers("/actuator/**", "/v3/api-docs/**", "/v3/api-docs.yaml",
                    "/swagger-ui/**", "/swagger-ui.html")
                    .hasRole("ADMIN")
                .requestMatchers("/api/**").hasAnyRole("USER", "ADMIN")
                .anyRequest().denyAll())
            .httpBasic(basic -> basic.authenticationEntryPoint(entryPoint))
            .exceptionHandling(errors -> errors
                .authenticationEntryPoint(entryPoint)
                .accessDeniedHandler(denied))
            .csrf(Customizer.withDefaults())
            .requestCache(AbstractHttpConfigurer::disable)
            .build();
    }

    private static void writeError(HttpServletResponse response, ObjectMapper mapper,
                                   int status, String code, String message) throws IOException {
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-store");
        mapper.writeValue(response.getOutputStream(), new ErrorResponse(code, message));
    }
}
