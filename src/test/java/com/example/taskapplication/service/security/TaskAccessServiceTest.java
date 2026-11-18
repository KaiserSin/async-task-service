package com.example.taskapplication.service.security;

import com.example.taskapplication.model.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;

import static org.assertj.core.api.Assertions.assertThat;

class TaskAccessServiceTest {

    private final TaskAccessService access = new TaskAccessService();

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void ownerCanAccessOwnTask() {
        authenticate("user1", "ROLE_USER");

        assertThat(access.canAccess(task("user1"))).isTrue();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"user2", "User1", ""})
    void userCannotAccessForeignOrUnownedTask(String owner) {
        authenticate("user1", "ROLE_USER");

        assertThat(access.canAccess(task(owner))).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"user1", "user2"})
    void adminCanAccessAnyTaskIncludingLegacy(String owner) {
        authenticate("admin", "ROLE_ADMIN");

        assertThat(access.canAccess(task(owner))).isTrue();
    }

    @Test
    void missingAuthenticationCannotAccessTask() {
        SecurityContextHolder.clearContext();

        assertThat(access.canAccess(task("user1"))).isFalse();
    }

    @Test
    void unauthenticatedPrincipalCannotAccessItsTask() {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.unauthenticated("user1", "unused")
        );

        assertThat(access.canAccess(task("user1"))).isFalse();
    }

    @Test
    void anonymousPrincipalCannotAccessEvenWithAdminAuthority() {
        SecurityContextHolder.getContext().setAuthentication(
            new AnonymousAuthenticationToken("anonymous", "user1",
                AuthorityUtils.createAuthorityList("ROLE_ADMIN"))
        );

        assertThat(access.canAccess(task("user1"))).isFalse();
    }

    private void authenticate(String username, String authority) {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated(username, "unused",
                AuthorityUtils.createAuthorityList(authority))
        );
    }

    private Task task(String owner) {
        Task task = new Task();
        task.setOwnerUsername(owner);
        return task;
    }
}
