/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.sink.connector;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolStreamingMessageCallbackHandler implements JCSMPStreamingPublishEventHandler {
  private static final Logger log = LoggerFactory
      .getLogger(SolStreamingMessageCallbackHandler.class);

  @Override
  public void handleError(String messageId, JCSMPException cause, long timestamp2) {
    log.info("===========Error occurred for message: {}, with cause: {} "
        + "details: {}", messageId, cause.getCause(),
        cause.getStackTrace());
    cause.printStackTrace();

  }

  @Override
  public void responseReceived(String messageId) {
    log.trace("Received ACK for message with ID: {}", messageId);
    //System.out.println("Received ACK for message with ID: " + messageId);

  }

}
