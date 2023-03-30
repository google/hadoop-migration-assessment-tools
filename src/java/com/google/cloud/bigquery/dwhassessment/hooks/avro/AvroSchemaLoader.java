/*
 * Copyright 2022-2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.dwhassessment.hooks.avro;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.avro.Schema;

/** Loader for Avro schemas */
public final class AvroSchemaLoader {
  /** Loads Avro schema from files in path relative to this class */
  public static Schema loadSchema(String name) {
    URL scriptUrl = AvroSchemaLoader.class.getResource(name);
    String errorMessage = String.format("Resource '%s' does not exist.", name);
    checkArgument(scriptUrl != null, errorMessage);
    try (InputStream inputStream = scriptUrl.openStream()) {
      return new Schema.Parser().parse(inputStream);
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Error reading schema '%s'.", name), e);
    }
  }

  private AvroSchemaLoader() {}
}
