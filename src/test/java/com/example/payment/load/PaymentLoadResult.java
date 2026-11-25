package com.example.payment.load;

import java.util.Collection;

record PaymentLoadResult(
    int sent,
    int confirmed,
    int registered,
    double throughput,
    double registrationP95Ms,
    double maxSendDelayMs
) {
    static double p95(Collection<Double> values) {
        double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        return sorted.length == 0 ? 0 : sorted[(int) Math.ceil(sorted.length * 0.95) - 1];
    }

}
