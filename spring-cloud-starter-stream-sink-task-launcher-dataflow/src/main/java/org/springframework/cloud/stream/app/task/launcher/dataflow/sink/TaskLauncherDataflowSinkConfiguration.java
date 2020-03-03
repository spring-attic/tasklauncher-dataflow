/*
 * Copyright 2018-2020 the original author or authors.
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

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Configuration class for the TaskLauncher Data Flow Sink.
 *
 * @author David Turanski
 * @author Gunnar Hillert
 */
@EnableBinding(PollingSink.class)
@EnableConfigurationProperties({ TriggerProperties.class, DataflowTaskLauncherSinkProperties.class, DataFlowClientProperties.class })
public class TaskLauncherDataflowSinkConfiguration {

	@Value("${autostart:true}")
	private boolean autoStart;

	@Bean
	public DynamicPeriodicTrigger periodicTrigger(TriggerProperties triggerProperties) {
		DynamicPeriodicTrigger trigger = new DynamicPeriodicTrigger(triggerProperties.getPeriod());
		trigger.setInitialDuration(Duration.ofMillis(triggerProperties.getInitialDelay()));
		return trigger;
	}

	/*
	 * For backward compatibility with spring-cloud-stream-2.1.x
	 */
	@EventListener
	public void addInterceptorToPollableMessageSource(ApplicationPreparedEvent event) {
		DefaultPollableMessageSource pollableMessageSource = event.getApplicationContext().getBean(DefaultPollableMessageSource.class);
		pollableMessageSource.addInterceptor(new ChannelInterceptor() {
			@Override
			public Message<?> preSend(Message<?> message, MessageChannel channel) {
				Message<?> newMessage = message;
				if (message.getHeaders().containsKey("originalContentType")) {
					newMessage = MessageBuilder.fromMessage(message)
							.setHeader("contentType", message.getHeaders().get("originalContentType") )
							.build();
				}
				return newMessage;
			}
		});
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

}
