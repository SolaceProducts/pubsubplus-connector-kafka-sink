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

package com.solace.connector.kafka.connect.sink;

import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPSessionStats;
import com.solacesystems.jcsmp.statistics.StatType;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import java.util.Enumeration;
import java.util.Optional;

import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolSessionHandler {
  private static final Logger log = LoggerFactory.getLogger(SolSessionHandler.class);

  private SolaceSinkConnectorConfig lconfig;

  final JCSMPProperties properties = new JCSMPProperties();
  final JCSMPChannelProperties chanProperties = new JCSMPChannelProperties();
  private JCSMPSession session = null;
  private TransactedSession txSession = null;

  public SolSessionHandler(SolaceSinkConnectorConfig lconfig) {
    this.lconfig = lconfig;
  }

  /**
   * Configure Session.
   */
  public void configureSession() {
    // Required Properties
    properties.setProperty(JCSMPProperties.USERNAME,
        lconfig.getString(SolaceSinkConstants.SOL_USERNAME));
    properties.setProperty(JCSMPProperties.PASSWORD,
            Optional.ofNullable(lconfig.getPassword(SolaceSinkConstants.SOL_PASSWORD))
                    .map(Password::value).orElse(null));
    properties.setProperty(JCSMPProperties.VPN_NAME,
        lconfig.getString(SolaceSinkConstants.SOL_VPN_NAME));
    properties.setProperty(JCSMPProperties.HOST,
        lconfig.getString(SolaceSinkConstants.SOL_HOST));

    // Channel Properties
    chanProperties.setConnectTimeoutInMillis(lconfig
        .getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_connectTimeoutInMillis));
    chanProperties.setReadTimeoutInMillis(lconfig
        .getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_readTimeoutInMillis));
    chanProperties.setConnectRetries(lconfig
        .getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_connectRetries));
    chanProperties.setReconnectRetries(lconfig
        .getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_reconnectRetries));

    chanProperties.setConnectRetriesPerHost(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_connectRetriesPerHost));
    chanProperties.setReconnectRetryWaitInMillis(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_reconnectRetryWaitInMillis));
    chanProperties.setKeepAliveIntervalInMillis(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_keepAliveIntervalInMillis));
    chanProperties.setKeepAliveLimit(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_keepAliveLimit));
    chanProperties.setSendBuffer(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_sendBuffer));
    chanProperties.setReceiveBuffer(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_receiveBuffer));
    chanProperties.setTcpNoDelay(
        lconfig.getBoolean(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_tcpNoDelay));
    chanProperties.setCompressionLevel(
        lconfig.getInt(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_compressionLevel));
    // Add channel properties to Session Properties
    properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, chanProperties);

    properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS,
        lconfig.getBoolean(SolaceSinkConstants.SOL_REAPPLY_SUBSCRIPTIONS));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS,
        lconfig.getBoolean(SolaceSinkConstants.SOL_GENERATE_SEND_TIMESTAMPS));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS,
        lconfig.getBoolean(SolaceSinkConstants.SOL_GENERATE_RCV_TIMESTAMPS));
    properties.setIntegerProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE,
        lconfig.getInt(SolaceSinkConstants.SOL_SUB_ACK_WINDOW_SIZE));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS,
        lconfig.getBoolean(SolaceSinkConstants.SOL_GENERATE_SEQUENCE_NUMBERS));
    properties.setBooleanProperty(JCSMPProperties.CALCULATE_MESSAGE_EXPIRATION,
        lconfig.getBoolean(SolaceSinkConstants.SOL_CALCULATE_MESSAGE_EXPIRATION));
    properties.setBooleanProperty(JCSMPProperties.PUB_MULTI_THREAD,
        lconfig.getBoolean(SolaceSinkConstants.SOL_PUB_MULTI_THREAD));
    properties.setBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR,
        lconfig.getBoolean(SolaceSinkConstants.SOL_MESSAGE_CALLBACK_ON_REACTOR));
    properties.setBooleanProperty(JCSMPProperties.IGNORE_DUPLICATE_SUBSCRIPTION_ERROR,
        lconfig.getBoolean(SolaceSinkConstants.SOL_IGNORE_DUPLICATE_SUBSCRIPTION_ERROR));
    properties.setBooleanProperty(JCSMPProperties.IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR,
        lconfig.getBoolean(SolaceSinkConstants.SOL_IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR));
    properties.setBooleanProperty(JCSMPProperties.NO_LOCAL,
        lconfig.getBoolean(SolaceSinkConstants.SOL_NO_LOCAL));
    properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
        lconfig.getString(SolaceSinkConstants.SOl_AUTHENTICATION_SCHEME));
    properties.setProperty(JCSMPProperties.KRB_SERVICE_NAME,
        lconfig.getString(SolaceSinkConstants.SOL_KRB_SERVICE_NAME));
    properties.setProperty(JCSMPProperties.SSL_CONNECTION_DOWNGRADE_TO,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_CONNECTION_DOWNGRADE_TO));
    properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_LOCAL_PRIORITY,
        lconfig.getInt(SolaceSinkConstants.SOL_SUBSCRIBER_LOCAL_PRIORITY));
    properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_NETWORK_PRIORITY,
        lconfig.getInt(SolaceSinkConstants.SOL_SUBSCRIBER_NETWORK_PRIORITY));


    log.info("=============Attempting to use SSL for PubSub+ connection");
    if (!(lconfig.getString(SolaceSinkConstants
        .SOL_SSL_CIPHER_SUITES).equals(""))) {
      properties.setProperty(JCSMPProperties.SSL_CIPHER_SUITES,
          lconfig.getString(SolaceSinkConstants.SOL_SSL_CIPHER_SUITES));
    }
    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE,
        lconfig.getBoolean(SolaceSinkConstants.SOL_SSL_VALIDATE_CERTIFICATE));
    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE,
        lconfig.getBoolean(SolaceSinkConstants.SOL_SSL_VALIDATE_CERTIFICATE_DATE));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_TRUST_STORE));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD,
        Optional.ofNullable(lconfig.getPassword(SolaceSinkConstants.SOL_SSL_TRUST_STORE_PASSWORD))
                .map(Password::value).orElse(null));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_TRUST_STORE_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_TRUSTED_COMMON_NAME_LIST,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_TRUSTED_COMMON_NAME_LIST));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_KEY_STORE));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD,
        Optional.ofNullable(lconfig.getPassword(SolaceSinkConstants.SOL_SSL_KEY_STORE_PASSWORD))
                .map(Password::value).orElse(null));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_KEY_STORE_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_NORMALIZED_FORMAT,
        lconfig.getString(SolaceSinkConstants.SOL_SSL_KEY_STORE_NORMALIZED_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD,
        Optional.ofNullable(lconfig.getPassword(SolaceSinkConstants.SOL_SSL_PRIVATE_KEY_PASSWORD))
                .map(Password::value).orElse(null));

  }

  /**
   * Create and connect JCSMPSession
   * @return
   * @throws JCSMPException
   */
  public void connectSession() throws JCSMPException {
      System.setProperty("java.security.auth.login.config",
                      lconfig.getString(SolaceSinkConstants.SOL_KERBEROS_LOGIN_CONFIG));
      System.setProperty("java.security.krb5.conf",
          lconfig.getString(SolaceSinkConstants.SOL_KERBEROS_KRB5_CONFIG));

      session = JCSMPFactory.onlyInstance().createSession(properties,
          null, new SolSessionEventCallbackHandler());
      session.connect();
  }

  /**
   * Create transacted session
   * @return TransactedSession
   * @throws JCSMPException
   */
  public void createTxSession() throws JCSMPException {
      txSession = session.createTransactedSession();
   }

  public JCSMPSession getSession() {
    return session;
  }

  public TransactedSession getTxSession() {
    return txSession;
  }

  public void printStats() {
    if (session != null) {
      JCSMPSessionStats lastStats = session.getSessionStats();
      Enumeration<StatType> estats = StatType.elements();
      while (estats.hasMoreElements()) {
        StatType statName = estats.nextElement();
        log.info("\t" + statName.getLabel() + ": " + lastStats.getStat(statName));
      }
      log.info("\n");
    }
  }

  /**
   * Shutdown Session.
   */
  public void shutdown() {
    if (session != null) {
      session.closeSession();
    }
  }

}
