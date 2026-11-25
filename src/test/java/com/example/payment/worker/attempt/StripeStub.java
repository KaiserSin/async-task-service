package com.example.payment.worker.attempt;

import com.example.payment.stripe.StripeGateway;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import static org.mockito.Answers.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;

@TestConfiguration(proxyBeanMethods = false)
public class StripeStub {

    public static boolean replaces(String className) {
        return StripePaymentHandler.class.getName().equals(className) || StripeGateway.class.getName().equals(className);
    }

    @Bean
    StripePaymentHandler stripePaymentHandler() {
        return mock(StripePaymentHandler.class, StripeStub::rejectStripeCall);
    }

    @Bean
    StripeGateway stripeGateway() {
        return mock(StripeGateway.class, StripeStub::rejectStripeCall);
    }

    private static Object rejectStripeCall(InvocationOnMock invocation) throws Throwable {
        if (invocation.getMethod().getDeclaringClass() == Object.class) {
            return RETURNS_DEFAULTS.answer(invocation);
        }
        throw new AssertionError("Unexpected Stripe call: " + invocation.getMethod().getName());
    }
}
