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
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.InterruptedException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.stream.Collectors;

public class CloudPubsubReceiver extends Receiver<String> {
  // 429 is not supported in either java.net.HttpURLConnection, org.apache.http.HttpStatus
  // or com.google.api.client.http.HttpStatusCodes;
  // TODO: Add additional codes to com.google.api.client.http.HttpStatusCodes package
  private final int HTTP_TOO_MANY_REQUESTS = 429;
  // Backoff time when pubsub is throttled
  private final int MIN_BACKOFF_SECONDS = 1;
  private final int MAX_BACKOFF_SECONDS = 64;
  // Maximum # of messages in each Pub/sub Pull request
  private final int BATCH_SIZE = 500;

  private String projectFullName;
  private String topicFullName;
  private String subscriptionFullName;

  public CloudPubsubReceiver(String projectName, String topicName, String subscriptionName) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.projectFullName = "projects/" + projectName;
    this.topicFullName = projectFullName + "/topics/" + topicName;
    this.subscriptionFullName = projectFullName + "/subscriptions/" + subscriptionName;
  }

  public void onStart() {
    Pubsub client = createAuthorizedClient();
    Subscription subscription = new Subscription().setTopic(topicFullName);
    try {
      // Create a subscription if it does not exist.
      subscription =
          client.projects().subscriptions().create(subscriptionFullName, subscription).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getDetails().getCode() == HttpURLConnection.HTTP_CONFLICT) {
        // Subscription already exists, but that's the expected behavior with multiple receivers.
      } else {
        reportSubscriptionCreationError(e);
      }
    } catch (IOException e) {
      reportSubscriptionCreationError(e);
    }

    // Start the thread that receives data over a connection
    // TODO: start a threadpool instead
    new Thread() {
      @Override
      public void run() {
        receive();
      }
    }.start();
  }

  public void onStop() {
    // Delete the subscription
    try {
      Pubsub client = createAuthorizedClient();
      client.projects().subscriptions().delete(subscriptionFullName).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getDetails().getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        // Subscription may has already been deleted, but that's the expected behavior
        // with multiple receivers.
      } else {
        reportSubscriptionDeleteionError(e);
      }
    } catch (IOException e) {
      reportSubscriptionDeleteionError(e);
    }
  }

  // Pull messages from Pubsub and store as RDD.
  private void receive() {
    Pubsub client = createAuthorizedClient();
    PullRequest pullRequest =
        new PullRequest().setReturnImmediately(false).setMaxMessages(BATCH_SIZE);

    int backoffTimeSeconds = MIN_BACKOFF_SECONDS;
    do {
      try {
        PullResponse pullResponse =
            client.projects().subscriptions().pull(subscriptionFullName, pullRequest).execute();

        List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
        if (receivedMessages != null) {
          // Store the message contents in batch
          store(
              receivedMessages
                  .stream()
                  .filter(m -> m.getMessage() != null)
                  .filter(m -> m.getMessage().decodeData() != null)
                  .map(
                      m -> {
                        try {
                          return new String(m.getMessage().decodeData(), "UTF-8");
                        } catch (UnsupportedEncodingException e) {
                          // Wrong encode
                          return null;
                        }
                      })
                  .filter(m -> m != null)
                  .iterator());

          AcknowledgeRequest ackRequest = new AcknowledgeRequest();
          ackRequest.setAckIds(
              receivedMessages
                  .stream()
                  .map(ReceivedMessage::getAckId)
                  .collect(Collectors.toList()));
          client.projects().subscriptions().acknowledge(subscriptionFullName, ackRequest).execute();
          // Reset backoff time
          backoffTimeSeconds = MIN_BACKOFF_SECONDS;
        }
      } catch (GoogleJsonResponseException e) {
        if (e.getDetails().getCode() == HTTP_TOO_MANY_REQUESTS) {
          // When PubSub is rate throttled, retry with exponential backoff.
          // TODO: Extract "retry-after" vaule from Http response, if available.
          reportError(
              "Reading from subscription "
                  + subscriptionFullName
                  + " is throttled. Will retry after "
                  + backoffTimeSeconds
                  + " seconds.",
              e);
          wait(backoffTimeSeconds);
          backoffTimeSeconds = Math.min(backoffTimeSeconds << 1, MAX_BACKOFF_SECONDS);
        } else {
          reportReadError(e);
        }
      } catch (IOException e) {
        reportReadError(e);
      }
    } while (!isStopped());
  }

  private Pubsub createAuthorizedClient() {
    try {
      // Create the credential
      HttpTransport httpTransport = Utils.getDefaultTransport();
      JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      GoogleCredential credential =
          GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);

      if (credential.createScopedRequired()) {
        credential = credential.createScoped(PubsubScopes.all());
      }
      HttpRequestInitializer initializer = new RetryHttpInitializerWrapper(credential);
      return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
          .setApplicationName("spark-pubsub-receiver")
          .build();
    } catch (IOException e) {
      reportError("Unable to create Cloud Pub/sub client.", e);
      return null;
    }
  }

  private void reportSubscriptionCreationError(Throwable e) {
    stop(
        "Unable to create subscription: " + subscriptionFullName + " for topic " + topicFullName,
        e);
  }

  private void reportReadError(Throwable e) {
    stop("Unable to read subscription: " + subscriptionFullName, e);
  }

  private void reportSubscriptionDeleteionError(Throwable e) {
    reportError("Unable to delete subscription: " + subscriptionFullName, e);
  }

  private void wait(int backoffTimeSeconds) {
    try {
      Thread.sleep(1000 * backoffTimeSeconds);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
