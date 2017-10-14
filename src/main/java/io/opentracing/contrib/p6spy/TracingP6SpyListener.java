package io.opentracing.contrib.p6spy;

import com.p6spy.engine.common.StatementInformation;
import com.p6spy.engine.event.SimpleJdbcEventListener;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopScopeManager;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TracingP6SpyListener extends SimpleJdbcEventListener {
  private final static Pattern URL_PEER_SERVICE_FINDER =
      Pattern.compile("tracingPeerService=(\\w*)");

  private final static String TRACE_WITH_ACTIVE_SPAN_ONLY_FINDER = "traceWithActiveSpanOnly=true";

  private final String defaultPeerService;
  private final ThreadLocal<Scope> currentScope = new ThreadLocal<>();

  TracingP6SpyListener(String defaultPeerService) {
    this.defaultPeerService = defaultPeerService;
  }

  @Override public void onBeforeAnyExecute(StatementInformation statementInformation) {
    onBefore("Execute", statementInformation);
  }

  @Override
  public void onAfterAnyExecute(StatementInformation statementInformation, long timeElapsedNanos,
      SQLException e) {
    onAfter(e);
  }

  @Override public void onBeforeAnyAddBatch(StatementInformation statementInformation) {
    onBefore("Batch", statementInformation);
  }

  @Override
  public void onAfterAnyAddBatch(StatementInformation statementInformation, long timeElapsedNanos,
      SQLException e) {
    onAfter(e);
  }

  private void onBefore(String operationName, StatementInformation statementInformation) {
    final Tracer tracer = GlobalTracer.get();
    if (tracer == null) return;
    final Scope scope = buildSpan(tracer, operationName, statementInformation);
    currentScope.set(scope);
  }

  private void onAfter(SQLException e) {
    Scope scope = currentScope.get();
    if (scope == null) return;
    Tags.ERROR.set(scope.span(), e != null);
    scope.close();
  }

  private Scope buildSpan(Tracer tracer, String operationName, StatementInformation statementInformation) {
    try {
      final Scope activeScope = tracer.scopeManager().active();
      final String dbUrl =
          statementInformation.getConnectionInformation().getConnection().getMetaData().getURL();
      if (withActiveSpanOnly(dbUrl) && activeScope == null) {
        return NoopScopeManager.NoopScope.INSTANCE;
      }

      final Tracer.SpanBuilder spanBuilder = tracer
          .buildSpan(operationName)
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);
      if (activeScope != null) {
        spanBuilder.asChildOf(activeScope.span());
      }
      final Scope scope = spanBuilder.startActive();
      decorate(scope.span(), statementInformation);
      return scope;
    } catch (SQLException e) {
      return NoopScopeManager.NoopScope.INSTANCE;
    }
  }

  private void decorate(Span span, StatementInformation statementInformation)
      throws SQLException {
    final String dbUrl =
        statementInformation.getConnectionInformation().getConnection().getMetaData().getURL();
    final String extractedPeerName = extractPeerService(dbUrl);
    final String peerName =
        extractedPeerName != null && !extractedPeerName.isEmpty() ? extractedPeerName
            : defaultPeerService;
    final String dbUser = statementInformation.getConnectionInformation()
        .getConnection()
        .getMetaData()
        .getUserName();
    final String dbInstance =
        statementInformation.getConnectionInformation().getConnection().getCatalog();

    Tags.COMPONENT.set(span, "java-p6spy");
    Tags.DB_STATEMENT.set(span, statementInformation.getSql());
    Tags.DB_TYPE.set(span, extractDbType(dbUrl));
    Tags.DB_INSTANCE.set(span, dbInstance);
    span.setTag("peer.address", dbUrl);
    if (peerName != null && !peerName.isEmpty()) {
      Tags.PEER_SERVICE.set(span, peerName);
    }
    if (dbUser != null && !dbUser.isEmpty()) {
      Tags.DB_USER.set(span, dbUser);
    }
  }

  private static String extractDbType(String realUrl) {
    return realUrl.split(":")[1];
  }

  private static String extractPeerService(String url) {
    Matcher matcher = URL_PEER_SERVICE_FINDER.matcher(url);
    if (matcher.find() && matcher.groupCount() == 1) {
      return matcher.group(1);
    }
    return "";
  }

  private static boolean withActiveSpanOnly(String url) {
    return url != null && url.contains(TRACE_WITH_ACTIVE_SPAN_ONLY_FINDER);
  }
}
