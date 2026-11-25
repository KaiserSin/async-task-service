package com.example.payment;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

/**
 * Checks the boundaries between the packages directly under {@code com.example.payment}.
 * Each module can use public types in another module's base package. Its subpackages are private
 * to that module. The test also checks for circular dependencies.
 */
class ModuleStructureTest {

    @Test
    void modulesUseOnlyEachOthersBasePackagesWithoutCycles() {
        ApplicationModules.of(PaymentApplication.class).verify();
    }
}
