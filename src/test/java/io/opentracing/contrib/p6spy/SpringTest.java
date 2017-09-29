package io.opentracing.contrib.p6spy;

import io.opentracing.ActiveSpan;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.opentracing.util.ThreadLocalActiveSpanSource;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static io.opentracing.contrib.p6spy.SpanChecker.checkSameTrace;
import static io.opentracing.contrib.p6spy.SpanChecker.checkTags;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SpringTest {

  private static final MockTracer mockTracer = new MockTracer(new ThreadLocalActiveSpanSource(),
      MockTracer.Propagator.TEXT_MAP);

  @BeforeClass
  public static void init() {
    GlobalTracer.register(mockTracer);
  }

  @Before
  public void before() throws Exception {
    mockTracer.reset();
  }

  @Test
  public void test() throws SQLException {
    BasicDataSource dataSource = getDataSource("");

    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute("CREATE TABLE employee (id INTEGER)");

    dataSource.close();

    List<MockSpan> finishedSpans = mockTracer.finishedSpans();
    assertEquals(1, finishedSpans.size());
    checkTags(finishedSpans, "myservice", "jdbc:hsqldb:mem:spring");
    checkSameTrace(finishedSpans);

    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void testWithSpanOnlyNoParent() throws SQLException {
    BasicDataSource dataSource = getDataSource(";traceWithActiveSpanOnly=true");

    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute("CREATE TABLE skip_new_spans (id INTEGER)");

    dataSource.close();

    List<MockSpan> finishedSpans = mockTracer.finishedSpans();
    assertEquals(0, finishedSpans.size());

    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void testWithSpanOnlyWithParent() throws SQLException {
    try (ActiveSpan activeSpan = mockTracer.buildSpan("parent").startActive()) {
      BasicDataSource dataSource = getDataSource(";traceWithActiveSpanOnly=true");

      JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
      jdbcTemplate.execute("CREATE TABLE with_parent_skip (id INTEGER)");

      dataSource.close();
    }

    List<MockSpan> finishedSpans = mockTracer.finishedSpans();
    assertEquals(2, finishedSpans.size());
    checkSameTrace(finishedSpans);
    assertNull(mockTracer.activeSpan());
  }

  private BasicDataSource getDataSource(String options) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:p6spy:hsqldb:mem:spring" + options);
    dataSource.setUsername("sa");
    dataSource.setPassword("");
    return dataSource;
  }

}
