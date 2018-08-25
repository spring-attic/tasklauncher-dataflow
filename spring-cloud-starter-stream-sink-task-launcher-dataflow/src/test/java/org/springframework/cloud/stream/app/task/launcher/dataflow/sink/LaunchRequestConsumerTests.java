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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
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
	"logging.level.org.springframework.cloud.stream.app.task.launcher.dataflow.sink=DEBUG", "maxExecutions=10" })
@DirtiesContext
public abstract class LaunchRequestConsumerTests {
	protected static Log log = LogFactory.getLog(LaunchRequestConsumerTests.class);

	//TODO: Test Binder not working with PollableMessageSource
	@MockBean
	BindingService bindingService;

	@TestPropertySource(properties = { "trigger.period=10", "trigger.initial-delay=0", "autostart=false" })
	public static class PauseAndResumeTests extends LaunchRequestConsumerTests {
		private static long MAX_WAIT = 1000;

		@Autowired
		private CurrentTaskExecutionsResource currentTaskExecutionsResource;

		@Autowired
		private CountDownLatch countDownLatch;

		@Autowired
		private LaunchRequestConsumer consumer;

		@Autowired
		private DynamicPeriodicTrigger trigger;

		@Test
		@DirtiesContext
		public void consumerPausesWhenMaxTaskExecutionsReached() throws InterruptedException {

			currentTaskExecutionsResource.setRunningExecutionCount(0);
			currentTaskExecutionsResource.setMaximumTaskExecutions(10);

			consumer.start();

			assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();

			assertThat(currentTaskExecutionsResource.getRunningExecutionCount()).isEqualTo(
				currentTaskExecutionsResource.getMaximumTaskExecutions());

			assertThat(eventually(c -> c.isPaused() && c.isRunning())).isTrue();

			assertThat(trigger.getPeriod()).isGreaterThanOrEqualTo(20);

			log.debug("Resetting execution count");
			currentTaskExecutionsResource.setRunningExecutionCount(0);

			assertThat(eventually(c -> !c.isPaused())).isTrue();
		}

		private synchronized boolean eventually(Predicate<LaunchRequestConsumer> condition) {
			long waitTime = 0;
			long sleepTime = 10;
			while (waitTime < MAX_WAIT) {
				if (condition.test(consumer)) {
					return true;
				}
				waitTime += sleepTime;
				try {
					Thread.sleep(sleepTime);
				}
				catch (InterruptedException e) {
					Thread.interrupted();
				}
			}
			return condition.test(consumer);
		}
	}

	@TestPropertySource(properties = { "trigger.period=10", "trigger.initial-delay=0" })
	public static class DynamicPeriodicTriggerTests extends LaunchRequestConsumerTests {

		@Autowired
		private DynamicPeriodicTrigger trigger;

		@Autowired
		private CurrentTaskExecutionsResource currentTaskExecutionsResource;

		@Autowired
		private LaunchRequestConsumer consumer;

		@Test
		@DirtiesContext
		public void testExponentialBackOff() throws InterruptedException {

			currentTaskExecutionsResource.setRunningExecutionCount(
				currentTaskExecutionsResource.getMaximumTaskExecutions());

			long waitTime = 0;
			while (trigger.getPeriod() < 80) {
				Thread.sleep(10);
				waitTime += 10;
				assertThat(waitTime).isLessThan(1000);
			}
			assertThat(consumer.isPaused() && consumer.isRunning()).isTrue();
		}
	}

	@TestPropertySource(properties = { "trigger.period=10",
		"trigger.initial-delay=0", "messageSourceDisabled=true", "countDown=3" })
	public static class BackoffWhenNoMessagesTest extends LaunchRequestConsumerTests {
		@Autowired
		private CountDownLatch countDownLatch;

		@Autowired
		private CurrentTaskExecutionsResource currentTaskExecutionsResource;

		@Autowired
		private DynamicPeriodicTrigger trigger;

		@Test
		public void backoffWhenNoMessages() throws InterruptedException {
			//CountDown for null messages when messageSourceDisabled.
			assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
			assertThat(currentTaskExecutionsResource.getRunningExecutionCount()).isZero();
			assertThat(trigger.getPeriod()).isGreaterThanOrEqualTo(40);
		}

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
		public CurrentTaskExecutionsResource currentTaskExecutionsResource(Environment environment) {
			currentTaskExecutionsResource.setMaximumTaskExecutions(
				Integer.valueOf(environment.getProperty("maxExecutions", "10")));
			return currentTaskExecutionsResource;
		}

		@Bean
		public CountDownLatch countDownLatch(CurrentTaskExecutionsResource resource, Environment environment) {
			return new CountDownLatch(environment.containsProperty("countDown") ?
				Integer.valueOf(environment.getProperty("countDown")) :
				(int) resource.getMaximumTaskExecutions());

		}

		@Bean
		DataFlowOperations dataFlowOperations(CurrentTaskExecutionsResource currentTaskExecutionsResource,
			CountDownLatch latch) {

			taskOperations = mock(TaskOperations.class);
			when(taskOperations.launch(anyString(), anyMap(), anyList())).thenAnswer((Answer<Long>) invocation -> {

				currentTaskExecutionsResource.setRunningExecutionCount(
					currentTaskExecutionsResource.getRunningExecutionCount() + 1);
				latch.countDown();

				return currentTaskExecutionsResource.getRunningExecutionCount();

			});

			when(taskOperations.currentTaskExecutions()).thenReturn(currentTaskExecutionsResource);

			dataFlowOperations = mock(DataFlowOperations.class);
			when(dataFlowOperations.taskOperations()).thenReturn(taskOperations);
			return dataFlowOperations;
		}

		@Bean
		public BeanPostProcessor beanPostProcessor(Environment environment, CountDownLatch countDownLatch) {
			return new BeanPostProcessor() {
				@Nullable
				@Override
				public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

					boolean messageSourceDisabled = Boolean.valueOf(
						environment.getProperty("messageSourceDisabled", "false"));
					if (bean instanceof PollableMessageSource) {
						DefaultPollableMessageSource messageSource = (DefaultPollableMessageSource) bean;
						messageSource.setSource(() -> {
							LaunchRequest request = new LaunchRequest();
							request.setApplicationName("foo");
							request.setCommandlineArguments(Arrays.asList("bar"));

							Message<Object> message = null;

							if (messageSourceDisabled) {
								countDownLatch.countDown();
							}
							else {
								try {
									message = MessageBuilder.withPayload(
										(Object) objectMapper.writeValueAsBytes(request))
										.setHeader("contentType", "application/json")
										.build();
								}
								catch (JsonProcessingException e) {
									e.printStackTrace();
								}
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
