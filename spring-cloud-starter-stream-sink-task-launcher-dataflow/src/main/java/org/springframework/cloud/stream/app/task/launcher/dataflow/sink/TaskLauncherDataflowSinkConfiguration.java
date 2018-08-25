/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.task.launcher.dataflow.sink;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.PeriodicTrigger;

/**
 * Configuration class for the TaskLauncher Data Flow Sink.
 *
 * @author David Turanski
 */
@EnableBinding(PollingSink.class)
@EnableConfigurationProperties({ TriggerProperties.class})
public class TaskLauncherDataflowSinkConfiguration {

	@Value("${autostart:true}")
	private boolean autoStart;

	@Bean
	public DynamicPeriodicTrigger periodicTrigger(TriggerProperties triggerProperties) {
		DynamicPeriodicTrigger trigger = new DynamicPeriodicTrigger(triggerProperties.getFixedDelay(),
			triggerProperties.getTimeUnit());
		trigger.setInitialDelay(triggerProperties.getInitialDelay());
		return trigger;
	}

	@Bean
	public LaunchRequestConsumer launchRequestConsumer(PollableMessageSource input,
		DataFlowOperations dataFlowOperations, DynamicPeriodicTrigger trigger) {

		if (dataFlowOperations.taskOperations() == null) {
			throw new IllegalArgumentException("The SCDF server does not support task operations");
		}
		LaunchRequestConsumer consumer =
			new LaunchRequestConsumer(input, trigger, dataFlowOperations.taskOperations());
		consumer.setAutoStartup(autoStart);
		return consumer;
	}
}
