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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author David Turanski
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = LaunchRequestConsumerTests.TestConfig.class, properties = {
	"logging.level.org.springframework.cloud.stream.app.task.launcher.dataflow.sink=DEBUG", "trigger.fixed-delay=0", "trigger.initial-delay=0" })
public class LaunchRequestConsumerTests {
	private static Log log = LogFactory.getLog(LaunchRequestConsumerTests.class);

	@Autowired
	private CurrentTaskExecutionsResource currentTaskExecutionsResource;

	@Autowired
	private CountDownLatch countDownLatch;

	@Autowired
	private Phaser phaser;

	@Autowired
	private LaunchRequestConsumer consumer;

	@Test
	@DirtiesContext
	public void consumerPausesWhenMaxTaskExecutionsReached() throws InterruptedException {

		assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();

		assertThat(currentTaskExecutionsResource.getRunningExecutionCount()).isEqualTo(
			currentTaskExecutionsResource.getMaximumTaskExecutions());

		assertThat(consumer.isPaused() && consumer.isRunning()).isTrue();

		log.debug("Resetting execution count");
		currentTaskExecutionsResource.setRunningExecutionCount(0);
		phaser.awaitAdvance(0);
		assertThat(phaser.getPhase()).isEqualTo(1);
		assertThat(consumer.isPaused()).isFalse();
	}

	@SpringBootApplication
	@Import(TaskLauncherDataflowSinkConfiguration.class)
	static class TestConfig {

		@Autowired
		private ObjectMapper objectMapper;

		private DataFlowOperations dataFlowOperations;

		private TaskOperations taskOperations;

		private CurrentTaskExecutionsResource currentTaskExecutionsResource = new CurrentTaskExecutionsResource();

		@Bean
		public CurrentTaskExecutionsResource currentTaskExecutionsResource() {
			currentTaskExecutionsResource.setMaximumTaskExecutions(10);
			return currentTaskExecutionsResource;
		}

		@Bean
		public CountDownLatch countDownLatch(CurrentTaskExecutionsResource resource) {
			return new CountDownLatch((int) resource.getMaximumTaskExecutions());
		}

		@Bean
		public Phaser phaser() {
			return new Phaser(2);
		}

		@Bean
		DataFlowOperations dataFlowOperations(CurrentTaskExecutionsResource currentTaskExecutionsResource,
			CountDownLatch latch, Phaser phaser) {

			taskOperations = mock(TaskOperations.class);
			when(taskOperations.launch(anyString(), anyMap(), anyList())).thenAnswer((Answer<Long>) invocation -> {
				if (currentTaskExecutionsResource.getRunningExecutionCount() == 0) {
					phaser.arrive();
				}

				currentTaskExecutionsResource.setRunningExecutionCount(
					currentTaskExecutionsResource.getRunningExecutionCount() + 1);
				log.debug("running execution count " + currentTaskExecutionsResource.getRunningExecutionCount());

				if (currentTaskExecutionsResource.getRunningExecutionCount()
					>= currentTaskExecutionsResource.getMaximumTaskExecutions()) {
					phaser.arrive();
				}
				latch.countDown();

				return currentTaskExecutionsResource.getRunningExecutionCount();

			});

			when(taskOperations.currentTaskExecutions()).thenReturn(currentTaskExecutionsResource);
			dataFlowOperations = mock(DataFlowOperations.class);
			when(dataFlowOperations.taskOperations()).thenReturn(taskOperations);
			return dataFlowOperations;
		}

		@Bean
		public BeanPostProcessor beanPostProcessor() {
			return new BeanPostProcessor() {
				@Nullable
				@Override
				public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

					if (bean instanceof PollableMessageSource) {
						DefaultPollableMessageSource messageSource = (DefaultPollableMessageSource) bean;
						messageSource.setSource(() -> {
							LaunchRequest request = new LaunchRequest();
							request.setApplicationName("foo");
							request.setCommandlineArguments(Arrays.asList("bar"));

							Message<Object> message = null;

							try {
								message = MessageBuilder.withPayload((Object) objectMapper.writeValueAsBytes(request))
									.setHeader("contentType", "application/json")
									.build();
							}
							catch (JsonProcessingException e) {
								e.printStackTrace();
							}
							return message;

						});
					}

					return bean;
				}
			};

		}
	}
}
