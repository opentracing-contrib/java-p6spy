package io.opentracing.contrib.p6spy;

import com.p6spy.engine.spy.P6SpyOptions;
import com.p6spy.engine.spy.option.P6OptionsRepository;
import java.util.Map;

class TracingP6SpyOptions extends P6SpyOptions {
  private static final String PEER_SERVICE = "tracingPeerService";
  private static final String TRACE_WITH_ACTIVE_SPAN_ONLY = "traceWithActiveSpanOnly";

  private final P6OptionsRepository optionsRepository;

  TracingP6SpyOptions(P6OptionsRepository optionsRepository) {
    super(optionsRepository);
    this.optionsRepository = optionsRepository;
  }

  @Override public void load(Map<String, String> options) {
    super.load(options);

    optionsRepository.set(String.class, PEER_SERVICE, options.get(PEER_SERVICE));
    optionsRepository.set(Boolean.class, TRACE_WITH_ACTIVE_SPAN_ONLY, options.get(TRACE_WITH_ACTIVE_SPAN_ONLY));
  }

  String tracingPeerService() {
    return optionsRepository.get(String.class, PEER_SERVICE);
  }

  boolean traceWithActiveSpanOnly() {
    final Boolean traceWithActiveSpanOnly = optionsRepository.get(Boolean.class, TRACE_WITH_ACTIVE_SPAN_ONLY);
    return traceWithActiveSpanOnly != null && traceWithActiveSpanOnly;
  }
}
