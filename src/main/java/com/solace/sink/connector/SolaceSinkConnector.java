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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSinkConnector extends SinkConnector {
	private static final Logger log = LoggerFactory.getLogger(SolaceSinkConnector.class);

	SolaceSinkConfig sConfig;
	private Map<String, String> sConfigProperties;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("==================== Start a SolaceSinkConnector");
		sConfigProperties = props;
		sConfig = new SolaceSinkConfig(props);
	}

	@Override
	public Class<? extends Task> taskClass() {
		log.info("==================== Requesting SolaceSinkTask");
		return SolaceSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		log.info("==================== Requesting taskConfigs for SolaceSinkTask");
		List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
		Map<String, String> taskProps = new HashMap<>(sConfigProperties);
		for (int i = 0; i < maxTasks; i++)
		{
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

	@Override
	public void stop() {
		log.info("SolaceSourceConnector is shutting down");
	}

	@Override
	public ConfigDef config() {
		log.info("==================== Requesting Config for  SolaceSinkConnector");
		return SolaceSinkConfig.config;
	}

}
