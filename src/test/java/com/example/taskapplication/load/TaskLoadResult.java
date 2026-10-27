package com.example.taskapplication.load;

import java.util.Collection;
import java.util.Locale;

record TaskLoadResult(
    int sent,
    int confirmed,
    int registered,
    int waiting,
    int running,
    int completed,
    int failed,
    double throughput,
    double registrationP95Ms,
    double waitP95Ms,
    double completionP95Ms,
    double maxSendDelayMs
) {
    static double p95(Collection<Double> values) {
        double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        return sorted.length == 0 ? 0 : sorted[(int) Math.ceil(sorted.length * 0.95) - 1];
    }

    @Override
    public String toString() {

        return String.format(Locale.ROOT,
            "sent=%d, confirmed=%d, registered=%d, NEW=%d, IN_PROGRESS=%d, COMPLETED=%d, "
                + "FAILED=%d, total throughput=%.2f tasks/s, registration p95=%.0f ms, "
                + "queue p95=%.0f ms, completion p95=%.0f ms, max dispatch delay=%.0f ms",
            sent, confirmed, registered, waiting, running, completed, failed, throughput,
            registrationP95Ms, waitP95Ms, completionP95Ms, maxSendDelayMs);
    }
}
