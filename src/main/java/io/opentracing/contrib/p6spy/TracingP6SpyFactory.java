package io.opentracing.contrib.p6spy;

import com.p6spy.engine.event.JdbcEventListener;
import com.p6spy.engine.spy.P6Factory;
import com.p6spy.engine.spy.P6LoadableOptions;
import com.p6spy.engine.spy.option.P6OptionsRepository;

public class TracingP6SpyFactory implements P6Factory {

  private TracingP6SpyOptions options;

  public P6LoadableOptions getOptions(P6OptionsRepository p6OptionsRepository) {
    return options = new TracingP6SpyOptions(p6OptionsRepository);
  }

  public JdbcEventListener getJdbcEventListener() {
    return new TracingP6SpyListener(options.tracingPeerService());
  }
}
