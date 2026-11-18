package com.example.taskapplication.service.database;

import jakarta.persistence.PersistenceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataIntegrityViolationException;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import static com.example.taskapplication.service.database.DatabaseFailureClassifier.Action.NOT_DATABASE;
import static com.example.taskapplication.service.database.DatabaseFailureClassifier.Action.RECOVER;
import static com.example.taskapplication.service.database.DatabaseFailureClassifier.Action.RETRY;
import static com.example.taskapplication.service.database.DatabaseFailureClassifier.Action.STOP;
import static org.assertj.core.api.Assertions.assertThat;

class DatabaseFailureClassifierTest {

    private final DatabaseFailureClassifier classifier = new DatabaseFailureClassifier();

    @ParameterizedTest
    @ValueSource(strings = {"22000", "22001", "22021", "22P02", "23000", "23502", "23503", "23505", "23514", "23P01"})
    void recoversDataErrors(String sqlState) {
        var failure = new DataIntegrityViolationException("Invalid task",
            new PersistenceException(new SQLException("Invalid data", sqlState)));

        assertThat(classifier.classify(failure)).isEqualTo(RECOVER);
    }

    @Test
    void findsDataErrorInBatchExceptionChain() {
        var failure = new BatchUpdateException();
        failure.setNextException(new SQLException("NUL in name", "22021"));

        assertThat(classifier.classify(failure)).isEqualTo(RECOVER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"08003", "08006", "08007", "40001", "40P01", "53300", "55P03", "57014", "57P01", "57P02", "57P03"})
    void retriesTemporaryDatabaseFailures(String sqlState) {
        assertThat(classifier.classify(new SQLException("Temporary failure", sqlState)))
            .isEqualTo(RETRY);
    }

    @Test
    void retriesConnectionRefusal() {
        assertThat(classifier.classify(new SQLException("Connection refused", "08001", new ConnectException())))
            .isEqualTo(RETRY);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "08001", "28P01", "42501", "42601", "42P01", "XX000"})
    void stopsForUnknownOrPermanentDatabaseFailures(String sqlState) {
        assertThat(classifier.classify(new SQLException("Database failure", sqlState)))
            .isEqualTo(STOP);
    }

    @Test
    void retriesTransientConnectionFailureWithoutSqlState() {
        assertThat(classifier.classify(new SQLTransientConnectionException("Connection unavailable")))
            .isEqualTo(RETRY);
    }

    @Test
    void transientConnectionFailureWithoutSqlStateTakesPrecedenceOverDataError() {
        var failure = new SQLTransientConnectionException("Connection unavailable");
        failure.setNextException(new SQLException("Invalid data", "22021"));

        assertThat(classifier.classify(failure)).isEqualTo(RETRY);
    }

    @Test
    void networkFailureTakesPrecedenceOverDataError() {
        var failure = new SQLException("Invalid data", "22021", new ConnectException());

        assertThat(classifier.classify(failure)).isEqualTo(RETRY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"08006", "42P01"})
    void infrastructureFailureTakesPrecedenceOverDataError(String sqlState) {
        var dataFirst = new SQLException("Invalid data", "22021");
        dataFirst.setNextException(new SQLException("Infrastructure failure", sqlState));
        var infrastructureFirst = new SQLException("Infrastructure failure", sqlState);
        infrastructureFirst.setNextException(new SQLException("Invalid data", "23514"));
        var expected = sqlState.equals("08006") ? RETRY : STOP;

        assertThat(classifier.classify(dataFirst)).isEqualTo(expected);
        assertThat(classifier.classify(infrastructureFirst)).isEqualTo(expected);
    }

    @Test
    void invalidConnectionSettingsStillStopListener() {
        assertThat(classifier.classify(new SQLException("Invalid data", "22021", new UnknownHostException())))
            .isEqualTo(STOP);
    }

    @Test
    void recoversMultipleDataErrorsEvenWithCyclicCauses() {
        var first = new SQLException("Invalid name", "22021");
        var second = new SQLException("Constraint violation", "23514");
        first.initCause(second);
        second.initCause(first);

        assertThat(classifier.classify(first)).isEqualTo(RECOVER);
    }

    @Test
    void leavesNonDatabaseFailuresToDefaultHandling() {
        assertThat(classifier.classify(new IllegalArgumentException("Invalid message")))
            .isEqualTo(NOT_DATABASE);
    }
}
