package com.example.payment.api.security;

import com.example.payment.domain.Payment;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Service
public class PaymentAccessService {

    public boolean canAccess(Payment payment) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (payment == null || authentication == null || !authentication.isAuthenticated()
            || authentication instanceof AnonymousAuthenticationToken) {
            return false;
        }
        boolean admin = authentication.getAuthorities().stream()
            .anyMatch(authority -> "ROLE_ADMIN".equals(authority.getAuthority()));
        return admin || payment.getOwnerUsername() != null
            && payment.getOwnerUsername().equals(authentication.getName());
    }
}
