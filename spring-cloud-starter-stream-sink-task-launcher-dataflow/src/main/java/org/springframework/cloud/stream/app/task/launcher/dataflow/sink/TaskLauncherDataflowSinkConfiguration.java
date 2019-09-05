/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.task.launcher.dataflow.sink;

import java.net.URI;
import java.time.Duration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.common.security.support.OAuth2AccessTokenProvidingClientHttpRequestInterceptor;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.DataFlowTemplate;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientAutoConfiguration;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientProperties;
import org.springframework.cloud.dataflow.rest.util.HttpClientConfigurer;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration class for the TaskLauncher Data Flow Sink.
 *
 * @author David Turanski
 * @author Gunnar Hillert
 */
@EnableBinding(PollingSink.class)
@EnableConfigurationProperties({ TriggerProperties.class, DataflowTaskLauncherSinkProperties.class, CustomDataFlowClientProperties.class })
public class TaskLauncherDataflowSinkConfiguration {

	private static Log logger = LogFactory.getLog(TaskLauncherDataflowSinkConfiguration.class);

	@Autowired(required = false)
	private RestTemplate restTemplate;

	@Value("${autostart:true}")
	private boolean autoStart;

	@Autowired
	private DataFlowClientProperties properties;

	@Autowired
	private CustomDataFlowClientProperties customProperties;

	@Bean
	public DynamicPeriodicTrigger periodicTrigger(TriggerProperties triggerProperties) {
		DynamicPeriodicTrigger trigger = new DynamicPeriodicTrigger(triggerProperties.getPeriod());
		trigger.setInitialDuration(Duration.ofMillis(triggerProperties.getInitialDelay()));
		return trigger;
	}

	@Bean
	public LaunchRequestConsumer launchRequestConsumer(PollableMessageSource input,
		DataFlowOperations dataFlowOperations, DynamicPeriodicTrigger trigger, TriggerProperties triggerProperties,
		DataflowTaskLauncherSinkProperties sinkProperties) {

		if (dataFlowOperations.taskOperations() == null) {
			throw new IllegalArgumentException("The SCDF server does not support task operations");
		}
		LaunchRequestConsumer consumer = new LaunchRequestConsumer(input, trigger, triggerProperties.getMaxPeriod(),
			dataFlowOperations.taskOperations());
		consumer.setAutoStartup(autoStart);
		consumer.setPlatformName(sinkProperties.getPlatformName());
		return consumer;
	}

	/**
	 * Once the task-launcher-dataflow sink has been updated for Boot 2.2.x, this bean can be removed
	 * as Data Flow 2.3.x provides this functionality via the {@link DataFlowClientAutoConfiguration}.
	 */
	@Bean
	public DataFlowOperations dataFlowOperations() throws Exception{
		RestTemplate template = DataFlowTemplate.prepareRestTemplate(restTemplate);
		final HttpClientConfigurer httpClientConfigurer = HttpClientConfigurer.create(new URI(properties.getServerUri()))
				.skipTlsCertificateVerification(properties.isSkipSslValidation());

		if (StringUtils.hasText(this.customProperties.getAccessToken())) {
			template.getInterceptors().add(new OAuth2AccessTokenProvidingClientHttpRequestInterceptor(this.customProperties.getAccessToken()));
			logger.debug("Configured OAuth2 Access Token for accessing the Data Flow Server");
		}
		else if(!StringUtils.isEmpty(properties.getAuthentication().getBasic().getUsername()) &&
				!StringUtils.isEmpty(properties.getAuthentication().getBasic().getPassword())){
			httpClientConfigurer.basicAuthCredentials(properties.getAuthentication().getBasic().getUsername(), properties.getAuthentication().getBasic().getPassword());
			template.setRequestFactory(httpClientConfigurer.buildClientHttpRequestFactory());
		}
		else {
			logger.debug("Not configuring security for accessing the Data Flow Server");
		}
		return new DataFlowTemplate(new URI(properties.getServerUri()), template);
	}
}
