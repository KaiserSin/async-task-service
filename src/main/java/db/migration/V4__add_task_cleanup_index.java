package db.migration;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.Connection;
import java.sql.SQLException;

public class V4__add_task_cleanup_index extends BaseJavaMigration {

    @Override
    public boolean canExecuteInTransaction() {
        return false;
    }

    @Override
    public void migrate(Context context) throws SQLException {
        Connection connection = context.getConnection();
        String timeout;
        try (var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT current_setting('statement_timeout')")) {
            result.next();
            timeout = result.getString(1);
        }

        try {
            setStatementTimeout(connection, "0");
            try (var statement = connection.createStatement()) {
                statement.execute("""
                    CREATE INDEX CONCURRENTLY idx_tasks_status_finished_at_id
                    ON tasks (status, finished_at, id)
                    """);
            }
        } finally {
            setStatementTimeout(connection, timeout);
        }
    }

    private void setStatementTimeout(Connection connection, String timeout) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT set_config('statement_timeout', ?, false)")) {
            statement.setString(1, timeout);
            statement.execute();
        }
    }
}
