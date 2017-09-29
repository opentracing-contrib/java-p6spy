package io.opentracing.contrib.p6spy;

import com.p6spy.engine.spy.P6SpyOptions;
import com.p6spy.engine.spy.option.P6OptionsRepository;
import java.util.Map;

class TracingP6SpyOptions extends P6SpyOptions {
  private static final String PEER_SERVICE = "tracingPeerService";

  private final P6OptionsRepository optionsRepository;

  TracingP6SpyOptions(P6OptionsRepository optionsRepository) {
    super(optionsRepository);
    this.optionsRepository = optionsRepository;
  }

  @Override public void load(Map<String, String> options) {
    super.load(options);

    optionsRepository.set(String.class, PEER_SERVICE, options.get(PEER_SERVICE));
  }

  String tracingPeerService() {
    return optionsRepository.get(String.class, PEER_SERVICE);
  }
}
