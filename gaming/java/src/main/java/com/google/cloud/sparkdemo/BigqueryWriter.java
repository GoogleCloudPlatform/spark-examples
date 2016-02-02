/*
 * Copyright (C) 2015 Google Inc.
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
 *
 * This is not an official Google product.
 */

package com.google.cloud.sparkdemo;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

// A simple implementation of streaming Bigquery writer.
public class BigqueryWriter implements Serializable {
  private String project;
  private String tableName;
  private String dataset;
  transient TableSchema schema = null;

  BigqueryWriter() {}

  public BigqueryWriter asProject(String project) {
    this.project = project;
    return this;
  }

  public BigqueryWriter withSchema(TableSchema schema) {
    this.schema = schema;
    return this;
  }

  public BigqueryWriter toDataset(String dataset) {
    this.dataset = dataset;
    return this;
  }

  public BigqueryWriter toTable(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public TableDataInsertAllResponse writeRow(TableRow row) throws IOException {
    Bigquery bigquery = createAuthorizedClient();
    List<TableDataInsertAllRequest.Rows> rows =
        Collections.singletonList(new TableDataInsertAllRequest.Rows().setJson(row));

    int retryAfter = 1; // in seconds
    while (true) {
      try {
        return bigquery
            .tabledata()
            .insertAll(project, dataset, tableName, new TableDataInsertAllRequest().setRows(rows))
            .execute();
      } catch (java.net.SocketTimeoutException e) {
        if (retryAfter <= 64) {
          wait(retryAfter);
          retryAfter <<= 1;
        } else {
          e.printStackTrace();
          System.exit(1);
          throw e;
        }
      }
    }
  }

  public Table createTable() throws IOException {
    Bigquery bigquery = createAuthorizedClient();
    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(dataset);
    tableRef.setProjectId(project);
    tableRef.setTableId(tableName);

    Table table = new Table().setTableReference(tableRef);
    table.setFriendlyName(tableName);
    table.setSchema(schema);

    try {
      return bigquery.tables().insert(project, dataset, table).execute();
    } catch (GoogleJsonResponseException e) {
      return null; // table could already exist; Ignore other errors.
    }
  }

  private Bigquery createAuthorizedClient() {
    try {
      // Create the credential
      HttpTransport transport = new NetHttpTransport();
      JsonFactory jsonFactory = new JacksonFactory();
      GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

      if (credential.createScopedRequired()) {
        Collection<String> bigqueryScopes = BigqueryScopes.all();
        credential = credential.createScoped(bigqueryScopes);
      }

      return new Bigquery.Builder(transport, jsonFactory, credential)
          .setApplicationName("Spark Gaming Samples")
          .build();
    } catch (IOException e) {
      // can not create bigquery client
      e.printStackTrace();
      return null;
    }
  }

  private void wait(int backoffTimeSeconds) {
    try {
      Thread.sleep(1000 * backoffTimeSeconds);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
