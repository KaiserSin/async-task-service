package com.example.taskapplication.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

@Validated
@ConfigurationProperties("api.security")
public record ApiSecurityProperties(@NotEmpty List<@Valid Account> users) {

    public record Account(
        @NotBlank @Size(max = 255) @Pattern(regexp = "[^\\p{Cc}]*") String username,
        @NotBlank String password,
        @NotEmpty List<@Pattern(regexp = "USER|ADMIN") String> roles
    ) {
    }
}
