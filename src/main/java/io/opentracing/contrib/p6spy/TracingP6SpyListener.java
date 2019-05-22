/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.p6spy;

import com.p6spy.engine.common.StatementInformation;
import com.p6spy.engine.event.SimpleJdbcEventListener;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopScopeManager;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TracingP6SpyListener extends SimpleJdbcEventListener {
  private static final Logger log = Logger.getLogger(TracingP6SpyListener.class.getName());
  private final static Pattern URL_PEER_SERVICE_FINDER =
      Pattern.compile("tracingPeerService=(\\w*)");

  private enum OptionalBoolean {
    TRUE, FALSE, OPTION_NOT_FOUND
  }

  private final static String TRACE_WITH_ACTIVE_SPAN_ONLY_FINDER = "traceWithActiveSpanOnly=true";
  private final static String TRACE_WITHOUT_ACTIVE_SPAN_ONLY_FINDER = "traceWithActiveSpanOnly=false";
  private final static String TRACE_WITH_STATEMENT_VALUES = "traceWithStatementValues=true";
  private final static String TRACE_WITHOUT_STATEMENT_VALUES = "traceWithStatementValues=false";

  private final String defaultPeerService;
  private final boolean defaultTraceWithActiveSpanOnly;
  private final boolean defaultTraceWithStatementValues;
  private final ThreadLocal<ScopedSpan> currentScope = new ThreadLocal<>();

  TracingP6SpyListener(String defaultPeerService, boolean defaultTraceWithActiveSpanOnly, boolean defaultTraceWithStatementValues) {
    this.defaultPeerService = defaultPeerService;
    this.defaultTraceWithActiveSpanOnly = defaultTraceWithActiveSpanOnly;
    this.defaultTraceWithStatementValues = defaultTraceWithStatementValues;
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
    final ScopedSpan scope = buildSpan(tracer, operationName, statementInformation);
    currentScope.set(scope);
  }

  private void onAfter(SQLException e) {
    ScopedSpan scopedSpan = currentScope.get();
    if (scopedSpan == null) return;
    Tags.ERROR.set(scopedSpan.span, e != null);
    scopedSpan.close();
  }

  private ScopedSpan buildSpan(Tracer tracer, String operationName, StatementInformation statementInformation) {
    final Span activeSpan = tracer.scopeManager().activeSpan();

    try {
      final String dbUrl =
          statementInformation.getConnectionInformation().getConnection().getMetaData().getURL();
      if (!allowTraceWithNoActiveSpan(dbUrl) && activeSpan == null) {
        return ScopedSpan.NOOP;
      }
    } catch (SQLException e) {
      return ScopedSpan.NOOP;
    }

    final Tracer.SpanBuilder spanBuilder = tracer
            .buildSpan(operationName)
            .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);
    if (activeSpan != null) {
      spanBuilder.asChildOf(activeSpan);
    }
    final Span span = spanBuilder.start();
    decorate(span, statementInformation);
    return new ScopedSpan(tracer.scopeManager().activate(span), span);
  }

  private void decorate(Span span, StatementInformation statementInformation) {
    try {
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
      if (!allowTraceWithStatementValues(dbUrl)) {
        Tags.DB_STATEMENT.set(span, statementInformation.getSql());
      } else {
        Tags.DB_STATEMENT.set(span, statementInformation.getSqlWithValues());
      }
      if (!isNullOrEmpty(dbUrl)) {
        span.setTag("peer.address", dbUrl);
        Tags.DB_TYPE.set(span, extractDbType(dbUrl));
      }
      if (!isNullOrEmpty(dbInstance)) {
        Tags.DB_INSTANCE.set(span, dbInstance);
      }
      if (!isNullOrEmpty(peerName)) {
        Tags.PEER_SERVICE.set(span, peerName);
      }
      if (!isNullOrEmpty(dbUser)) {
        Tags.DB_USER.set(span, dbUser);
      }
    } catch (SQLException ex) {
      if(log.isLoggable(Level.WARNING)) {
        log.warning("Failed to decorate the span[" + span + "].");
      }
    }
  }

  private static String extractDbType(String realUrl) {
    return realUrl.split(":")[1];
  }

  private static String extractPeerService(String url) {
    if (url != null) {
      Matcher matcher = URL_PEER_SERVICE_FINDER.matcher(url);
      if (matcher.find() && matcher.groupCount() == 1) {
        return matcher.group(1);
      }
    }
    return "";
  }

  private boolean allowTraceWithNoActiveSpan(String url) {
    final OptionalBoolean withActiveSpanOnly = withActiveSpanOnly(url);
    return withActiveSpanOnly != OptionalBoolean.OPTION_NOT_FOUND && withActiveSpanOnly == OptionalBoolean.FALSE || withActiveSpanOnly == OptionalBoolean.OPTION_NOT_FOUND && !defaultTraceWithActiveSpanOnly;
  }

  private boolean allowTraceWithStatementValues(String url) {
    final OptionalBoolean withStatementValues = withStatementValues(url);
    return withStatementValues != OptionalBoolean.OPTION_NOT_FOUND && withStatementValues == OptionalBoolean.TRUE || withStatementValues == OptionalBoolean.OPTION_NOT_FOUND && defaultTraceWithStatementValues;
  }

  private static OptionalBoolean withActiveSpanOnly(String url) {
    return withOrWithout(url, TRACE_WITH_ACTIVE_SPAN_ONLY_FINDER, TRACE_WITHOUT_ACTIVE_SPAN_ONLY_FINDER);
  }

  private static OptionalBoolean withStatementValues(String url) {
    return withOrWithout(url, TRACE_WITH_STATEMENT_VALUES, TRACE_WITHOUT_STATEMENT_VALUES);
  }

  private static OptionalBoolean withOrWithout(String url, String with, String without) {
    if(url == null) {
      return OptionalBoolean.OPTION_NOT_FOUND;
    }
    if(url.contains(with) && url.contains(without)) {
      if(log.isLoggable(Level.WARNING)) {
        log.warning("jdbc url contains contradictory " + with + " and " + without + " options. Defaulting to no options");
      }
      return OptionalBoolean.OPTION_NOT_FOUND;
    }
    if(url.contains(with)) {
      return OptionalBoolean.TRUE;
    } else if (url.contains(without)) {
      return OptionalBoolean.FALSE;
    }
    return OptionalBoolean.OPTION_NOT_FOUND;
  }

  private static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  private final static class ScopedSpan implements Closeable {
    static final ScopedSpan NOOP = new ScopedSpan(NoopScopeManager.NoopScope.INSTANCE, NoopSpan.INSTANCE);
    final Scope scope;
    final Span span;

    private ScopedSpan(Scope scope, Span span) {
      this.scope = scope;
      this.span = span;
    }

    @Override
    public void close() {
      scope.close();
      span.finish();
    }
  }
}
