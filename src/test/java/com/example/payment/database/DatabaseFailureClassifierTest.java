package com.example.payment.database;

import jakarta.persistence.PersistenceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.orm.jpa.JpaSystemException;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.util.stream.Stream;

import static com.example.payment.database.DatabaseFailureClassifier.Action.NOT_DATABASE;
import static com.example.payment.database.DatabaseFailureClassifier.Action.RECOVER;
import static com.example.payment.database.DatabaseFailureClassifier.Action.RETRY;
import static com.example.payment.database.DatabaseFailureClassifier.Action.STOP;
import static org.assertj.core.api.Assertions.assertThat;

class DatabaseFailureClassifierTest {

    private final DatabaseFailureClassifier classifier = new DatabaseFailureClassifier();

    @ParameterizedTest
    @ValueSource(strings = {"22000", "22001", "22021", "22P02", "23000", "23502", "23503", "23505", "23514", "23P01"})
    void recoversDataErrors(String sqlState) {
        var failure = new DataIntegrityViolationException("Invalid payment",
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
    @ValueSource(strings = {
        "08000", "08001", "08003", "08004", "08006", "08007", "08P01",
        "25006", "40001", "40P01", "53000", "53100", "53200", "53300", "53400",
        "55P03", "57014", "57P01", "57P02", "57P03", "57P04", "57P05", "58000", "58030", "58P01"
    })
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
    @ValueSource(strings = {" ", "2", "28000", "28P01", "3D000", "3F000", "42501", "42601", "42P01", "0A000", "F0000", "XX000"})
    void stopsForUnknownOrPermanentDatabaseFailures(String sqlState) {
        assertThat(classifier.classify(new SQLException("Database failure", sqlState)))
            .isEqualTo(STOP);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Connection is closed", "JDBC connection unavailable", "Unrelated driver wording"})
    void retriesJdbcTransactionFailuresWithoutDependingOnMessage(String message) {
        var failure = new JpaSystemException(new org.hibernate.TransactionException(
            "Rollback failed", new SQLException(message)));

        assertThat(classifier.classify(failure)).isEqualTo(RETRY);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void retriesJdbcTransactionFailuresWithNoUsableSqlState(String sqlState) {
        var failure = new JpaSystemException(new org.hibernate.TransactionException(
            "Transaction failed", new SQLException("Driver detail", sqlState)));

        assertThat(classifier.classify(failure)).isEqualTo(RETRY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"22021", "23505"})
    void jdbcTransactionWrapperDoesNotOverrideDataError(String sqlState) {
        var failure = new JpaSystemException(new org.hibernate.TransactionException(
            "Transaction failed", new SQLException("Invalid data", sqlState)));

        assertThat(classifier.classify(failure)).isEqualTo(RECOVER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"28P01", "42P01", "2", "XX000"})
    void jdbcTransactionWrapperDoesNotOverridePermanentOrUnknownSqlState(String sqlState) {
        var failure = new JpaSystemException(new org.hibernate.TransactionException(
            "Transaction failed", new SQLException("Permanent SQL failure", sqlState)));

        assertThat(classifier.classify(failure)).isEqualTo(STOP);
    }

    @Test
    void connectionIsClosedMessageAloneDoesNotMakeSqlFailureRetryable() {
        assertThat(classifier.classify(new SQLException("Connection is closed"))).isEqualTo(STOP);
    }

    @Test
    void hibernateTransactionFailureWithoutJdbcCauseIsNotRetried() {
        var failure = new JpaSystemException(new org.hibernate.TransactionException(
            "Transaction failed", new IllegalStateException("Connection is closed")));

        assertThat(classifier.classify(failure)).isEqualTo(STOP);
    }

    @Test
    void hibernateTransactionFailureWithIndirectJdbcCauseDoesNotUseJdbcFallback() {
        var failure = new JpaSystemException(new org.hibernate.TransactionException(
            "Transaction failed", new IllegalStateException("Driver wrapper", new SQLException("Driver detail"))));

        assertThat(classifier.classify(failure)).isEqualTo(STOP);
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

    @ParameterizedTest
    @MethodSource("connectionFailures")
    void connectionFailureTakesPrecedenceOverDataError(Throwable connectionFailure) {
        assertThat(classifier.classify(new SQLException("Invalid data", "22021", connectionFailure)))
            .isEqualTo(RETRY);
    }

    private static Stream<Throwable> connectionFailures() {
        return Stream.of(new ConnectException(), new SocketTimeoutException(),
            new UnknownHostException(), new SSLException("TLS handshake failed"));
    }

    @ParameterizedTest
    @MethodSource("infrastructureFailuresWithoutSqlState")
    void retriesInfrastructureFailuresWithoutSqlState(Throwable failure) {
        assertThat(classifier.classify(failure)).isEqualTo(RETRY);
    }

    private static Stream<Throwable> infrastructureFailuresWithoutSqlState() {
        return Stream.of(
            new DataAccessResourceFailureException("Database unavailable"),
            new CannotCreateTransactionException("Database unavailable"),
            new RecoverableDataAccessException("Connection unavailable"),
            new QueryTimeoutException("Database timeout"),
            new SQLRecoverableException("Connection unavailable"),
            new SQLTimeoutException("Database timeout")
        );
    }

    @Test
    void permanentSqlStateTakesPrecedenceOverTransientWrapper() {
        var failure = new DataAccessResourceFailureException("Database unavailable",
            new SQLException("Invalid password", "28P01", new ConnectException()));

        assertThat(classifier.classify(failure)).isEqualTo(STOP);
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
