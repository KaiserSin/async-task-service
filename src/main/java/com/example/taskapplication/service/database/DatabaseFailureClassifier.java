package com.example.taskapplication.service.database;

import jakarta.persistence.PersistenceException;
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
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        Set<String> sqlStates = new HashSet<>();
        pending.add(failure);

        boolean databaseFailure = false;
        boolean retryableWithoutCode = false;
        boolean networkFailure = false;
        boolean jdbcTransactionFailure = false;

        while (!pending.isEmpty()) {
            Throwable cause = pending.removeFirst();
            if (!visited.add(cause)) {
                continue;
            }

            switch (cause) {
                case SQLTransientException sql -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                    collectSqlDetails(sql, sqlStates, pending);
                }
                case SQLRecoverableException sql -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                    collectSqlDetails(sql, sqlStates, pending);
                }
                case SQLException sql -> {
                    databaseFailure = true;
                    collectSqlDetails(sql, sqlStates, pending);
                }
                case TransientDataAccessException ignored -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                }
                case RecoverableDataAccessException ignored -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                }
                case DataAccessResourceFailureException ignored -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                }
                case DataAccessException ignored -> databaseFailure = true;
                case org.hibernate.TransactionException transaction -> {
                    databaseFailure = true;
                    jdbcTransactionFailure |= transaction.getCause() instanceof SQLException;
                }
                case PersistenceException ignored -> databaseFailure = true;
                case CannotCreateTransactionException ignored -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                }
                case ConnectException ignored -> networkFailure = true;
                case SocketTimeoutException ignored -> networkFailure = true;
                case UnknownHostException ignored -> networkFailure = true;
                case SSLException ignored -> networkFailure = true;
                default -> {
                }
            }
            if (cause.getCause() != null) {
                pending.addLast(cause.getCause());
            }
        }

        if (!databaseFailure) {
            return Action.NOT_DATABASE;
        }
        boolean dataFailure = false;
        boolean retryableSqlState = false;
        for (String sqlState : sqlStates) {
            if (sqlState.startsWith("22") || sqlState.startsWith("23")) {
                dataFailure = true;
                continue;
            }
            boolean retryableClass = sqlState.length() >= 2
                && RETRYABLE_SQL_CLASSES.contains(sqlState.substring(0, 2));
            if (!RETRYABLE_SQL_STATES.contains(sqlState) && !retryableClass) {
                return Action.STOP;
            }
            retryableSqlState = true;
        }

        // Infrastructure failures take precedence over a bad record in a SQL exception chain.
        if (retryableSqlState || retryableWithoutCode || networkFailure) {
            return Action.RETRY;
        }
        if (dataFailure) {
            return Action.RECOVER;
        }
        // A failed JDBC rollback can mask the original connection error and its SQLState.
        if (jdbcTransactionFailure && sqlStates.isEmpty()) {
            return Action.RETRY;
        }
        return Action.STOP;
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
