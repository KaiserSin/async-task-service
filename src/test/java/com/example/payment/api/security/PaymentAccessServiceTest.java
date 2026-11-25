package com.example.payment.api.security;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentFixtures;
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

class PaymentAccessServiceTest {

    private final PaymentAccessService access = new PaymentAccessService();

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void ownerCanAccessOwnPayment() {
        authenticate("user1", "ROLE_USER");

        assertThat(access.canAccess(payment("user1"))).isTrue();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"user2", "User1", ""})
    void userCannotAccessForeignOrUnownedPayment(String owner) {
        authenticate("user1", "ROLE_USER");

        assertThat(access.canAccess(payment(owner))).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"user1", "user2"})
    void adminCanAccessAnyPaymentIncludingLegacy(String owner) {
        authenticate("admin", "ROLE_ADMIN");

        assertThat(access.canAccess(payment(owner))).isTrue();
    }

    @Test
    void missingAuthenticationCannotAccessPayment() {
        SecurityContextHolder.clearContext();

        assertThat(access.canAccess(payment("user1"))).isFalse();
    }

    @Test
    void unauthenticatedPrincipalCannotAccessItsPayment() {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.unauthenticated("user1", "unused")
        );

        assertThat(access.canAccess(payment("user1"))).isFalse();
    }

    @Test
    void anonymousPrincipalCannotAccessEvenWithAdminAuthority() {
        SecurityContextHolder.getContext().setAuthentication(
            new AnonymousAuthenticationToken("anonymous", "user1",
                AuthorityUtils.createAuthorityList("ROLE_ADMIN"))
        );

        assertThat(access.canAccess(payment("user1"))).isFalse();
    }

    private void authenticate(String username, String authority) {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated(username, "unused",
                AuthorityUtils.createAuthorityList(authority))
        );
    }

    private Payment payment(String owner) {
        return PaymentFixtures.payment(null, owner, "Payment", 1_000L);
    }
}
