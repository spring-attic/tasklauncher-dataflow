/*
 * Copyright 2019 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientProperties;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.dataflow.rest.resource.LauncherResource;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.hateoas.PagedResources;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author David Turanski
 **/
public class LaunchRequestConsumerTests {

	private ApplicationContextRunner contextRunner;

	@BeforeEach
	public void setUp() {
		contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(TaskLauncherDataflowSinkConfiguration.class, TestConfig.class);
	}

	@Test
	public void consumerPausesWhenMaxTaskExecutionsReached() {
		contextRunner = contextRunner.withPropertyValues(
			"trigger.period=10",
			"trigger.initial-delay=0",
			"autostart=false").run(context -> {
				CurrentTaskExecutionsResource currentTaskExecutionsResource = currentTaskExecutionsResource(context);

				LaunchRequestConsumer consumer = consumer(context);
				CountDownLatch countDownLatch = countDownLatch(context);
				DynamicPeriodicTrigger trigger = trigger(context);

				consumer.start();

				assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
				assertThat(currentTaskExecutionsResource.getRunningExecutionCount()).isEqualTo(
					currentTaskExecutionsResource.getMaximumTaskExecutions());
				assertThat(eventually(c -> c.isPaused() && c.isRunning(), consumer)).isTrue();

				currentTaskExecutionsResource.setRunningExecutionCount(0);
				assertThat(eventually(c -> !c.isPaused(), consumer)).isTrue();
			});
	}

	@Test
	public void exponentialBackOff() {
		contextRunner.withPropertyValues("trigger.period=10", "trigger.initial-delay=0")
			.run(context -> {
				LaunchRequestConsumer consumer = consumer(context);
				CurrentTaskExecutionsResource currentTaskExecutionsResource =
					currentTaskExecutionsResource(context);
				currentTaskExecutionsResource.setRunningExecutionCount(
					currentTaskExecutionsResource.getMaximumTaskExecutions());
				DynamicPeriodicTrigger trigger = trigger(context);

				long waitTime = 0;
				while (trigger.getDuration().compareTo(Duration.ofMillis(80)) < 0) {
					Thread.sleep(10);
					waitTime += 10;
					assertThat(waitTime).isLessThan(1000);
				}
				assertThat(consumer.isPaused() && consumer.isRunning()).isTrue();
			});
	}

	@Test
	public void backoffWhenNoMessages() {

		contextRunner.withPropertyValues(
			"trigger.period=10",
			"trigger.initial-delay=0",
			"messageSourceDisabled=true",
			"countDown=3")
			.run(context -> {
				CountDownLatch countDownLatch = countDownLatch(context);
				CurrentTaskExecutionsResource currentTaskExecutionsResource =
					currentTaskExecutionsResource(context);
				DynamicPeriodicTrigger trigger = trigger(context);

				assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isTrue();
				assertThat(currentTaskExecutionsResource.getRunningExecutionCount()).isZero();
				assertThat(trigger.getDuration()).isGreaterThanOrEqualTo(Duration.ofMillis(40));
			});
	}

	@Test
	public void launchRequestHasWrongPlatform() {
		final AtomicBoolean passed = new AtomicBoolean();
		contextRunner.withPropertyValues(
			"trigger.period=10",
			"trigger.initial-delay=0",
			"autostart=false",
			"requestWrongPlatform=true")
			.run(context -> {

				SubscribableChannel errorChannel = context.getBean(SubscribableChannel.class, "errorChannel");
				errorChannel.subscribe(message -> {
					try {
						assertThat(message).isInstanceOf(ErrorMessage.class);
						ErrorMessage errorMessage = (ErrorMessage) message;
						assertThat(errorMessage.getPayload()).isInstanceOf(Exception.class);
						Exception exception = (Exception) message.getPayload();
						assertThat(exception.getCause().getMessage()).isEqualTo(
							"Task Launch request for Task foo contains deployment property 'spring.cloud.dataflow"
								+ ".task.platformName=other' which does not match the platform configured for the Task"
								+ " Launcher: 'default'"
						);
						passed.set(true);
					}
					catch (Exception e) {
					}
				});
				LaunchRequestConsumer consumer = consumer(context);
				consumer.start();
				assertThat(eventually(c -> passed.get(), consumer)).isTrue();
			});
	}

	private CurrentTaskExecutionsResource currentTaskExecutionsResource(ApplicationContext context) {
		CurrentTaskExecutionsResource currentTaskExecutionsResource =
			context.getBean(CurrentTaskExecutionsResource.class);
		currentTaskExecutionsResource.setRunningExecutionCount(0);
		currentTaskExecutionsResource.setMaximumTaskExecutions(10);
		return currentTaskExecutionsResource;
	}

	private CountDownLatch countDownLatch(ApplicationContext context) {
		return context.getBean(CountDownLatch.class);
	}

	private LaunchRequestConsumer consumer(ApplicationContext context) {
		return context.getBean(LaunchRequestConsumer.class);
	}

	private DynamicPeriodicTrigger trigger(ApplicationContext context) {
		return context.getBean(DynamicPeriodicTrigger.class);
	}

	private synchronized boolean eventually(Predicate<LaunchRequestConsumer> condition,
		LaunchRequestConsumer consumer) {
		final long MAX_WAIT = 1000;
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

	static class TestConfig {

		private ObjectMapper objectMapper = new ObjectMapper();

		private DataFlowOperations dataFlowOperations;

		private TaskOperations taskOperations;

		private CurrentTaskExecutionsResource currentTaskExecutionsResource = new CurrentTaskExecutionsResource();

		@Bean
		public CurrentTaskExecutionsResource currentTaskExecutionsResource(Environment environment) {
			currentTaskExecutionsResource.setMaximumTaskExecutions(
				Integer.valueOf(environment.getProperty("maxExecutions", "10")));
			currentTaskExecutionsResource.setName("default");
			return currentTaskExecutionsResource;
		}

		@Bean
		public CountDownLatch countDownLatch(CurrentTaskExecutionsResource resource, Environment environment) {
			return new CountDownLatch(environment.containsProperty("countDown") ?
				Integer.valueOf(environment.getProperty("countDown")) :
				resource.getMaximumTaskExecutions());
		}

		@Bean
		public MessageChannel errorChannel() {
			return new DirectChannel();
		}

		@Bean
		DataFlowClientProperties dataFlowClientProperties() {
			return new DataFlowClientProperties();
		}

		@Bean
		DataFlowOperations dataFlowOperations(CurrentTaskExecutionsResource currentTaskExecutionsResource,
			CountDownLatch latch) {

			taskOperations = mock(TaskOperations.class);
			when(taskOperations.launch(anyString(), anyMap(), anyList())).thenAnswer((Answer<Long>) invocation -> {
				currentTaskExecutionsResource.setRunningExecutionCount(
					currentTaskExecutionsResource.getRunningExecutionCount() + 1);
				latch.countDown();
				return Long.valueOf(currentTaskExecutionsResource.getRunningExecutionCount());
			});

			List<LauncherResource> launcherResources = new ArrayList<>();
			LauncherResource launcherResource0 = mock(LauncherResource.class);
			when(launcherResource0.getName()).thenReturn("default");
			LauncherResource launcherResource1 = mock(LauncherResource.class);
			when(launcherResource1.getName()).thenReturn("other");

			when(taskOperations.currentTaskExecutions()).thenReturn(
				Collections.singletonList(currentTaskExecutionsResource));
			LauncherResource launcherResource = mock(LauncherResource.class);
			when(launcherResource.getName()).thenReturn("default");

			launcherResources.add(launcherResource0);
			launcherResources.add(launcherResource1);

			when(taskOperations.listPlatforms()).thenReturn(new PagedResources(launcherResources, null));

			dataFlowOperations = mock(DataFlowOperations.class);
			when(dataFlowOperations.taskOperations()).thenReturn(taskOperations);
			return dataFlowOperations;
		}

		@Bean
		public BeanPostProcessor beanPostProcessor(Environment environment, CountDownLatch countDownLatch,
			MessageChannel errorChannel) {
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
							request.setTaskName("foo");
							if (environment.getProperty("requestWrongPlatform", "false")
								.equals("true")) {
								request.getDeploymentProperties().put(LaunchRequestConsumer.TASK_PLATFORM_NAME,
									"other");
							}

							messageSource.setErrorChannel(errorChannel);

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
