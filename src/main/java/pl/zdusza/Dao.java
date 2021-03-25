package pl.zdusza;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

public class Dao {

    private static final Logger LOGGER = LoggerFactory.getLogger(Dao.class);

    private static final String SET_TIMEZONE;

    static {
        SET_TIMEZONE = new ClassPathFileResolver().textFile("/db/queries/SetTimezone.sql");
    }

    private SQLClient sqlClient;

    public Dao(final SQLClient sqlClient) {
        this.sqlClient = sqlClient;
    }

    private Future<SQLConnection> getConnectionFromPoolWithoutACAndInTZ() {
        Future<SQLConnection> futureConnection = Future.future();
        try {
            sqlClient.getConnection(getConnectionAsyncCall -> {
                if (getConnectionAsyncCall.succeeded()) {
                    this.setAutoCommitFalse(getConnectionAsyncCall.result())
                            .setHandler(futureConnection.completer());
                } else {
                    futureConnection.fail(getConnectionAsyncCall.cause());
                }
            });
        } catch (Throwable t) {
            futureConnection.fail(t);
        }
        return futureConnection;
    }

    private Future<SQLConnection> setAutoCommitFalse(final SQLConnection connection) {
        Future<SQLConnection> future = Future.future();
        try {
            connection.setAutoCommit(false, setAutoCommitAsyncCall -> {
                if (setAutoCommitAsyncCall.succeeded()) {
                    setTimezone(connection)
                            .setHandler(future.completer());
                } else {
                    this.<SQLConnection>close(connection, setAutoCommitAsyncCall.cause())
                            .setHandler(future.completer());
                }
            });
        } catch (Throwable t) {
            this.<SQLConnection>close(connection, t)
                    .setHandler(future.completer());
        }
        return future;
    }

    private Future<SQLConnection> setTimezone(final SQLConnection connection) {
        final Future<SQLConnection> futureConnection = Future.future();
        try {
            connection.update(SET_TIMEZONE, setTimezoneAsyncCall -> {
                if (setTimezoneAsyncCall.succeeded()) {
                    futureConnection.complete(connection);
                } else {
                    this.<SQLConnection>rollback(connection, setTimezoneAsyncCall.cause())
                            .setHandler(futureConnection.completer());
                }
            });
        } catch (Throwable t) {
            this.<SQLConnection>rollback(connection, t)
                    .setHandler(futureConnection.completer());
        }
        return futureConnection;
    }

    private <T> Future<T> commit(final SQLConnection connection, final T result) {
        Future<T> futureResult = Future.future();
        try {
            connection.commit(commitAsyncCall -> {
                if (commitAsyncCall.succeeded()) {
                    try {
                        connection.close(closeAsyncCall -> {
                            if (closeAsyncCall.succeeded()) {
                                futureResult.complete(result);
                            } else {
                                LOGGER.error("Closing connection failed", closeAsyncCall.cause());
                                futureResult.fail(closeAsyncCall.cause());
                            }
                        });
                    } catch (Throwable t) {
                        LOGGER.error("Closing connection failed", t);
                        futureResult.fail(t);
                    }
                } else {
                    this.<T>rollback(connection, commitAsyncCall.cause())
                            .setHandler(futureResult.completer());
                }
            });
        } catch (Throwable t) {
            this.<T>rollback(connection, t)
                    .setHandler(futureResult.completer());
        }
        return futureResult;
    }

    private <T> Future<T> rollback(final SQLConnection connection,
                                   final Throwable t) {
        Future<T> future = Future.future();
        try {
            connection.rollback(rollbackAsyncCall -> {
                if (rollbackAsyncCall.failed()) {
                    LOGGER.error("Rollback failed", rollbackAsyncCall.cause());
                }
                this.<T>close(connection, t)
                        .setHandler(future.completer());
            });
        } catch (Exception rollbackException) {
            LOGGER.error("Rollback failed", rollbackException);
            this.<T>close(connection, t)
                    .setHandler(future.completer());
        }
        return future;
    }

    private <T> Future<T> close(
            final SQLConnection connection,
            final Throwable t) {
        final Future<T> future = Future.future();
        try {
            connection.close(closeAsyncCall -> {
                if (closeAsyncCall.failed()) {
                    LOGGER.error("Closing connection failed", closeAsyncCall.cause());
                }
                future.fail(t);
            });
        } catch (Exception closeConnectionException) {
            LOGGER.error("Closing connection failed", closeConnectionException);
            future.fail(t);
        }
        return future;
    }

    public final <T> void doInTransactionPLTZ(final Function<SQLConnection, Future<T>> function,
                                              final Future<T> future) {
        this.getConnectionFromPoolWithoutACAndInTZ().setHandler(
                getConnectionAC -> {
                    if (getConnectionAC.succeeded()) {
                        try {
                            function.apply(getConnectionAC.result())
                                    .setHandler(ac -> {
                                        if (ac.succeeded()) {
                                            this.commit(getConnectionAC.result(), ac.result())
                                                    .setHandler(future.completer());
                                        } else {
                                            this.<T>rollback(getConnectionAC.result(), ac.cause())
                                                    .setHandler(future.completer());
                                        }
                                    });
                        } catch (Throwable t) {
                            this.<T>rollback(getConnectionAC.result(), t)
                                    .setHandler(future.completer());
                        }
                    } else {
                        future.fail(getConnectionAC.cause());
                    }
                }
        );
    }


    public final <T> Future<T> doInTryCatch(final Handler<Future<T>> handler) {
        Future<T> future = Future.future();
        try {
            handler.handle(future);
        } catch (Throwable t) {
            future.fail(t);
        }
        return future;
    }

    private <T1, T2> Handler<AsyncResult<T1>> doInTryCatch(final Handler<T1> handler,
                                                           final Future<T2> future,
                                                           final Optional<String> query,
                                                           final Optional<JsonArray> queryParams) {
        return asyncCall -> {
            if (asyncCall.succeeded()) {
                try {
                    handler.handle(asyncCall.result());
                } catch (Throwable t) {
                    query.ifPresent(q ->
                            LOGGER.error("Query: {} failed for params: {}", q, queryParams.get().toString()));
                    future.fail(t);
                }
            } else {
                query.ifPresent(q -> LOGGER.error("Query: {} failed for params: {}", q, queryParams.get().toString()));
                future.fail(asyncCall.cause());
            }
        };
    }

    public final <T1, T2> Handler<AsyncResult<T1>> doInTryCatch(final Handler<T1> handler, final Future<T2> future) {
        return doInTryCatch(handler, future, Optional.empty(), Optional.empty());
    }

    public final <T1, T2> Handler<AsyncResult<T1>> doInTryCatch(final Handler<T1> handler,
                                                                final Future<T2> future,
                                                                final String query,
                                                                final JsonArray queryParams) {
        return doInTryCatch(handler, future, Optional.of(query), Optional.of(queryParams));
    }
}
