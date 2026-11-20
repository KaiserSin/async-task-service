package com.example.taskapplication.service.database;

import jakarta.persistence.PersistenceException;
import org.hibernate.TransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

@Component
public class DatabaseFailureClassifier {

    public enum Action {
        RETRY,
        RECOVER,
        STOP,
        NOT_DATABASE
    }

    private static final Set<String> RETRYABLE_SQL_CLASSES = Set.of("08", "53", "57", "58");
    private static final Set<String> RETRYABLE_SQL_STATES = Set.of("25006", "40001", "40P01", "55P03");

    public Action classify(Throwable failure) {
        return decide(collectEvidence(failure));
    }

    private FailureEvidence collectEvidence(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        Set<String> sqlStates = new HashSet<>();
        pending.add(failure);

        boolean databaseFailure = false;
        boolean retryableCause = false;
        boolean jdbcTransactionFailure = false;

        while (!pending.isEmpty()) {
            Throwable cause = pending.removeFirst();
            if (!visited.add(cause)) {
                continue;
            }

            if (cause instanceof SQLException sql) {
                collectSqlDetails(sql, sqlStates, pending);
            }
            databaseFailure |= cause instanceof SQLException
                || cause instanceof DataAccessException
                || cause instanceof PersistenceException
                || cause instanceof CannotCreateTransactionException;
            retryableCause |= isRetryableCause(cause);

            Throwable nestedCause = cause.getCause();
            jdbcTransactionFailure |= cause instanceof TransactionException
                && nestedCause instanceof SQLException;
            if (nestedCause != null) {
                pending.addLast(nestedCause);
            }
        }

        return new FailureEvidence(sqlStates, databaseFailure, retryableCause, jdbcTransactionFailure);
    }

    private boolean isRetryableCause(Throwable cause) {
        return cause instanceof SQLTransientException
            || cause instanceof SQLRecoverableException
            || cause instanceof TransientDataAccessException
            || cause instanceof RecoverableDataAccessException
            || cause instanceof DataAccessResourceFailureException
            || cause instanceof CannotCreateTransactionException
            || cause instanceof ConnectException
            || cause instanceof SocketTimeoutException
            || cause instanceof UnknownHostException
            || cause instanceof SSLException;
    }

    private Action decide(FailureEvidence evidence) {
        if (!evidence.databaseFailure()) {
            return Action.NOT_DATABASE;
        }
        boolean dataFailure = false;
        boolean retryable = evidence.retryableCause();
        for (String sqlState : evidence.sqlStates()) {
            if (sqlState.startsWith("22") || sqlState.startsWith("23")) {
                dataFailure = true;
                continue;
            }
            boolean retryableClass = sqlState.length() >= 2
                && RETRYABLE_SQL_CLASSES.contains(sqlState.substring(0, 2));
            if (!RETRYABLE_SQL_STATES.contains(sqlState) && !retryableClass) {
                return Action.STOP;
            }
            retryable = true;
        }

        // Infrastructure failures take precedence over a bad record in a SQL exception chain.
        if (retryable) {
            return Action.RETRY;
        }
        if (dataFailure) {
            return Action.RECOVER;
        }
        // A failed JDBC rollback can mask the original connection error and its SQLState.
        if (evidence.jdbcTransactionFailure() && evidence.sqlStates().isEmpty()) {
            return Action.RETRY;
        }
        return Action.STOP;
    }

    private record FailureEvidence(
        Set<String> sqlStates,
        boolean databaseFailure,
        boolean retryableCause,
        boolean jdbcTransactionFailure
    ) {
    }

    private void collectSqlDetails(
        SQLException sql,
        Set<String> sqlStates,
        Deque<Throwable> pending
    ) {
        if (sql.getSQLState() != null && !sql.getSQLState().isBlank()) {
            sqlStates.add(sql.getSQLState());
        }
        if (sql.getNextException() != null) {
            pending.addLast(sql.getNextException());
        }
    }
}
