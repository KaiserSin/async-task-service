package com.example.taskapplication.service.database;

import jakarta.persistence.PersistenceException;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
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
        STOP,
        NOT_DATABASE
    }

    private static final Set<String> RETRYABLE_SQL_STATES = Set.of(
        "08003", "08006", "08007",
        "40001", "40P01", "53300", "55P03", "57014",
        "57P01", "57P02", "57P03"
    );

    public Action classify(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        Set<String> sqlStates = new HashSet<>();
        pending.add(failure);

        boolean databaseFailure = false;
        boolean retryableWithoutCode = false;
        boolean networkFailure = false;
        boolean invalidConnectionSettings = false;

        while (!pending.isEmpty()) {
            Throwable cause = pending.removeFirst();
            if (!visited.add(cause)) {
                continue;
            }

            switch (cause) {
                case SQLTransientConnectionException sql -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                    collectSqlDetails(sql, sqlStates, pending);
                }
                case SQLException sql -> {
                    databaseFailure = true;
                    collectSqlDetails(sql, sqlStates, pending);
                }
                case CannotAcquireLockException ignored -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                }
                case CannotSerializeTransactionException ignored -> {
                    databaseFailure = true;
                    retryableWithoutCode = true;
                }
                case DataAccessException ignored -> databaseFailure = true;
                case PersistenceException ignored -> databaseFailure = true;
                case CannotCreateTransactionException ignored -> databaseFailure = true;
                case ConnectException ignored -> networkFailure = true;
                case SocketTimeoutException ignored -> networkFailure = true;
                case UnknownHostException ignored -> invalidConnectionSettings = true;
                case SSLException ignored -> invalidConnectionSettings = true;
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
        if (invalidConnectionSettings) {
            return Action.STOP;
        }

        for (String sqlState : sqlStates) {
            boolean retryableConnection = "08001".equals(sqlState) && networkFailure;
            if (!RETRYABLE_SQL_STATES.contains(sqlState) && !retryableConnection) {
                return Action.STOP;
            }
        }

        if (!sqlStates.isEmpty() || retryableWithoutCode || networkFailure) {
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
