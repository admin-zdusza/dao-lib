package pl.zdusza;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;


@RunWith(VertxUnitRunner.class)
public class DaoTest {
    private static final String TEST_SUCCESSFUL_RESULT = "GreateSuccess";
    private static final RuntimeException TEST_EXCEPTION_1 = new RuntimeException("Test exception 1");
    private static final RuntimeException TEST_EXCEPTION_2 = new RuntimeException("Test exception 2");

    @Rule
    public RunTestOnContext vertxContext = new RunTestOnContext();

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private SQLConnection sqlConnection;

    @Mock
    private SQLClient sqlClient;

    private Dao dao;

    @Before
    public final void setUp() {
        this.dao = new Dao(sqlClient);
    }

    @Test
    public final void testShouldDoInTransactionSucceed(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).commit(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(TEST_SUCCESSFUL_RESULT), result);
        result.setHandler(v -> {
            if (v.succeeded()) {
                tc.verify(v1 -> {
                    Mockito.verify(sqlClient, Mockito.times(1)).getConnection(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1))
                            .setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).update(Mockito.any(), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).commit(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).close(Mockito.any());
                    tc.assertEquals(v.result(), TEST_SUCCESSFUL_RESULT);
                    async.complete();
                });
            } else {
                tc.fail(v.cause());
            }
        });
    }

    @Test
    public final void testShouldDoInTransactionFailWhenGetConnectionFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());

        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingGetConnectionFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenGetConnectionThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doThrow(TEST_EXCEPTION_1).when(sqlClient).getConnection(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingGetConnectionFailure(tc, async));
    }

    private Handler<AsyncResult<String>> verifyHandlingGetConnectionFailure(final TestContext tc, final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.fail();
            } else {
                tc.verify(v1 -> {
                    Mockito.verify(sqlClient, Mockito.times(1)).getConnection(Mockito.any());
                    tc.assertEquals(v.cause(), TEST_EXCEPTION_1);
                    async.complete();
                });
            }
        };
    }

    @Test
    public final void testShouldDoInTransactionFailWhenSetAutocommitFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingSetAutoCommitFailure(tc, async));
    }


    @Test
    public final void testShouldDoInTransactionFailWhenSetAutocommitThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doThrow(TEST_EXCEPTION_1)
                .when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingSetAutoCommitFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenSetAutocommitFailsAndCloseFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.failedFuture(TEST_EXCEPTION_2));
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingSetAutoCommitFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenSetAutocommitFailsAndCloseThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doThrow(TEST_EXCEPTION_2).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingSetAutoCommitFailure(tc, async));
    }

    private Handler<AsyncResult<String>> verifyHandlingSetAutoCommitFailure(final TestContext tc, final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.fail();
            } else {
                tc.verify(v1 -> {
                    Mockito.verify(sqlClient, Mockito.times(1)).getConnection(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1))
                            .setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).close(Mockito.any());
                    tc.assertEquals(v.cause(), TEST_EXCEPTION_1);
                    async.complete();

                });
            }
        };
    }

    @Test
    public final void testShouldDoInTransactionFailWhenUpdateFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingUpdateOrFunctionCallFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenUpdateThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doThrow(TEST_EXCEPTION_1).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingUpdateOrFunctionCallFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenUpdateFailsAndRollbackFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.failedFuture(TEST_EXCEPTION_2));
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingUpdateOrFunctionCallFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenUpdateFailsAndRollbackThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doThrow(TEST_EXCEPTION_2).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingUpdateOrFunctionCallFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenFunctionInTransactionThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> {
            throw TEST_EXCEPTION_1;
        }, result);
        result.setHandler(verifyHandlingUpdateOrFunctionCallFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenFunctionInTransactionFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.failedFuture(TEST_EXCEPTION_1), result);
        result.setHandler(verifyHandlingUpdateOrFunctionCallFailure(tc, async));
    }

    private Handler<AsyncResult<String>> verifyHandlingUpdateOrFunctionCallFailure(final TestContext tc,
                                                                                   final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.fail();
            } else {
                tc.verify(v1 -> {
                    Mockito.verify(sqlClient, Mockito.times(1)).getConnection(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1))
                            .setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).update(Mockito.any(), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).rollback(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).close(Mockito.any());
                    tc.assertEquals(v.cause(), TEST_EXCEPTION_1);
                    async.complete();
                });
            }
        };
    }

    @Test
    public final void testShouldDoInTransactionFailWhenCommitFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).commit(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingCommitFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenCommitThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doThrow(TEST_EXCEPTION_1).when(sqlConnection).commit(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).rollback(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingCommitFailure(tc, async));
    }

    private Handler<AsyncResult<String>> verifyHandlingCommitFailure(final TestContext tc, final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.fail();
            } else {
                tc.verify(v1 -> {
                    Mockito.verify(sqlClient, Mockito.times(1)).getConnection(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1))
                            .setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).update(Mockito.any(), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).commit(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).rollback(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).close(Mockito.any());
                    tc.assertEquals(v.cause(), TEST_EXCEPTION_1);
                    async.complete();
                });
            }
        };
    }

    @Test
    public final void testShouldDoInTransactionFailWhenCloseFails(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).commit(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.failedFuture(TEST_EXCEPTION_1));
            return null;
        }).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingCloseFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTransactionFailWhenCloseThrows(final TestContext tc) {
        Async async = tc.async();
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture(sqlConnection));
            return null;
        }).when(sqlClient).getConnection(Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(1)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).update(Mockito.any(), Mockito.any());
        Mockito.doAnswer(invocation -> {
            invocation.<Handler<AsyncResult<SQLConnection>>>getArgument(0)
                    .handle(Future.succeededFuture());
            return null;
        }).when(sqlConnection).commit(Mockito.any());
        Mockito.doThrow(TEST_EXCEPTION_1).when(sqlConnection).close(Mockito.any());
        Future<String> result = Future.future();
        this.dao.doInTransactionPLTZ(connection -> Future.succeededFuture(), result);
        result.setHandler(verifyHandlingCloseFailure(tc, async));
    }

    private Handler<AsyncResult<String>> verifyHandlingCloseFailure(final TestContext tc, final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.fail();
            } else {
                tc.verify(v1 -> {
                    Mockito.verify(sqlClient, Mockito.times(1)).getConnection(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1))
                            .setAutoCommit(ArgumentMatchers.eq(false), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).update(Mockito.any(), Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).commit(Mockito.any());
                    Mockito.verify(sqlConnection, Mockito.times(1)).close(Mockito.any());
                    tc.assertEquals(v.cause(), TEST_EXCEPTION_1);
                    async.complete();
                });
            }
        };
    }

    //TODO:
    @Test
    public final void testShouldDoInTryCatchHandlerSucceed(final TestContext tc) {
        Async async = tc.async();
        dao.doInTryCatch((Handler<Future<String>>) event -> event.complete(TEST_SUCCESSFUL_RESULT))
                .setHandler(verifyHandlingDoInTryCatchSuccess(tc, async));
    }

    @Test
    public final void testShouldDoInTryCatchHandlerFailWhenHandlerThrows(final TestContext tc) {
        Async async = tc.async();
        dao.doInTryCatch((Handler<Future<String>>) event -> {
            throw TEST_EXCEPTION_1;
        }).setHandler(verifyHandleDoInTryCatchFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTryCatchHandlerFutureSucceed(final TestContext tc) {
        Async async = tc.async();
        Future<String> future = Future.future();
        dao.<String, String>doInTryCatch(future::complete, future)
                .handle(Future.succeededFuture(TEST_SUCCESSFUL_RESULT));
        future.setHandler(verifyHandlingDoInTryCatchSuccess(tc, async)
        );
    }

    @Test
    public final void testShouldDoInTryCatchHandlerFutureFailWhenHandlerFails(final TestContext tc) {
        Async async = tc.async();
        Future<String> future = Future.future();
        dao.<String, String>doInTryCatch(future::complete, future)
                .handle(Future.failedFuture(TEST_EXCEPTION_1));
        future.setHandler(verifyHandleDoInTryCatchFailure(tc, async));
    }

    @Test
    public final void testShouldDoInTryCatchHandlerFutureFailWhenHandlerThrows(final TestContext tc) {
        Async async = tc.async();
        Future<String> future = Future.future();
        dao.<String, String>doInTryCatch(v -> {
            throw TEST_EXCEPTION_1;
        }, future)
                .handle(Future.succeededFuture(TEST_SUCCESSFUL_RESULT));
        future.setHandler(verifyHandleDoInTryCatchFailure(tc, async));
    }

    private Handler<AsyncResult<String>> verifyHandlingDoInTryCatchSuccess(final TestContext tc, final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.verify(v1 -> {
                    tc.assertEquals(v.result(), TEST_SUCCESSFUL_RESULT);
                    async.complete();
                });
            } else {
                tc.fail(v.cause());
            }
        };
    }

    private Handler<AsyncResult<String>> verifyHandleDoInTryCatchFailure(final TestContext tc, final Async async) {
        return v -> {
            if (v.succeeded()) {
                tc.fail();
            } else {
                tc.verify(v1 -> {
                    tc.assertEquals(v.cause(), TEST_EXCEPTION_1);
                    async.complete();
                });
            }
        };
    }
}
