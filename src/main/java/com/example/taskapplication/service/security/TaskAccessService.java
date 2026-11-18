package com.example.taskapplication.service.security;

import com.example.taskapplication.model.Task;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Service
public class TaskAccessService {

    public boolean canAccess(Task task) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (task == null || authentication == null || !authentication.isAuthenticated()
            || authentication instanceof AnonymousAuthenticationToken) {
            return false;
        }
        boolean admin = authentication.getAuthorities().stream()
            .anyMatch(authority -> "ROLE_ADMIN".equals(authority.getAuthority()));
        return admin || task.getOwnerUsername() != null
            && task.getOwnerUsername().equals(authentication.getName());
    }
}
