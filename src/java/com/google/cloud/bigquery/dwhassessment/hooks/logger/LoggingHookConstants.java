package com.google.cloud.bigquery.dwhassessment.hooks.logger;

import com.google.cloud.bigquery.dwhassessment.hooks.avro.AvroSchemaLoader;
import org.apache.avro.Schema;

public final class LoggingHookConstants {

  public static final Schema QUERY_EVENT_SCHEMA = AvroSchemaLoader.loadSchema("QueryEvents.avsc");

  public static final String HOOK_VERSION = "1.0";

  private LoggingHookConstants() {}
}
