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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.dataflow.rest.client.DataFlowClientException;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.VndErrors;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author David Turanski
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = LaunchRetryTests.TestConfig.class, properties = {
	"logging.level.org.springframework.cloud.stream.app.task.launcher.dataflow.sink=DEBUG",
	"logging.level.org.springframework.retry.support=DEBUG" })
public class LaunchRetryTests {

	//TODO: Test Binder not working with PollableMessageSource
	@MockBean
	private BindingService bindingService;

	@Autowired
	private CountDownLatch countDownLatch;

	@Autowired
	private TestRetryListener taskExecutionRetryListener;

	@Autowired
	private TestRetryListener messageSourceRetryListener;

	@Test
	public void retryLoopsWithConcurrencyFailureException() throws InterruptedException {
		assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(taskExecutionRetryListener.getRetries()).isEqualTo(2);

		assertThat(messageSourceRetryListener.getRetries()).isEqualTo(2);
	}

	@SpringBootApplication
	@Import({ TaskLauncherDataflowSinkConfiguration.class })
	static class TestConfig {

		@Autowired
		private ObjectMapper objectMapper;

		@Bean
		public DataFlowOperations dataFlowOperations(CountDownLatch countDownLatch) {

			CurrentTaskExecutionsResource currentTaskExecutionsResource = new CurrentTaskExecutionsResource();
			currentTaskExecutionsResource.setRunningExecutionCount(0);
			currentTaskExecutionsResource.setMaximumTaskExecutions(1);

			TaskOperations taskOperations = mock(TaskOperations.class);

			AtomicLong id = new AtomicLong();

			when(taskOperations.launch(anyString(), anyMap(), anyList())).thenAnswer((Answer<Long>) invocation -> {
				try {
					if (countDownLatch.getCount() >= 7) {
						id.set(1);
					/*
					 * Emulate a task execution limit exception. The consumer should convert an error matching this
					 * message format to a ConcurrencyFailureException since the currentTaskExecutionsResource state
					 * indicates that there are no tasks running.
					 */
						VndErrors.VndError error = new VndErrors.VndError("logref",
							"The maximum concurrent task executions [1] is at its limit.", new Link("href"));
						throw new DataFlowClientException(new VndErrors(error));

					}
					else if (countDownLatch.getCount() <= 3) {
						VndErrors.VndError error = new VndErrors.VndError("logref",
							"Connection refused " + countDownLatch.getCount(), new Link("href"));
						throw new DataFlowClientException(new VndErrors(error));

					}
				}
				finally {
					countDownLatch.countDown();
				}
				return id.getAndIncrement();
			});

			when(taskOperations.currentTaskExecutions()).thenReturn(currentTaskExecutionsResource);

			DataFlowOperations dataFlowOperations = mock(DataFlowOperations.class);
			when(dataFlowOperations.taskOperations()).thenReturn(taskOperations);
			return dataFlowOperations;
		}

		@Bean
		public CountDownLatch countDownLatch() {
			return new CountDownLatch(8);
		}

		@Bean
		public TestRetryListener taskExecutionRetryListener(
			@Qualifier("taskExecutionLimitRetryTemplate") RetryTemplate retryTemplate) {
			TestRetryListener retryListener = new TestRetryListener(TaskExecutionLimitException.class);
			retryTemplate.registerListener(retryListener);
			return retryListener;
		}

		@Bean
		public TestRetryListener messageSourceRetryListener(
			@Qualifier("messageSourceRetryTemplate") RetryTemplate retryTemplate) {
			TestRetryListener retryListener = new TestRetryListener(MessageHandlingException.class);
			retryTemplate.registerListener(retryListener);
			return retryListener;
		}

		@Bean
		public RetryTemplate messageSourceRetryTemplate() {

			RetryTemplate retryTemplate = new RetryTemplate();

			FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
			backOffPolicy.setBackOffPeriod(0);

			SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
			retryPolicy.setMaxAttempts(2);

			retryTemplate.setBackOffPolicy(backOffPolicy);
			retryTemplate.setRetryPolicy(retryPolicy);
			return retryTemplate;
		}

		@Bean
		public BeanPostProcessor beanPostProcessor(CountDownLatch countDownLatch,
			@Qualifier("messageSourceRetryTemplate") RetryTemplate retryTemplate) {
			return new BeanPostProcessor() {
				@Nullable
				@Override
				public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

					if (bean instanceof PollableMessageSource) {
						DefaultPollableMessageSource messageSource = (DefaultPollableMessageSource) bean;

						messageSource.setRetryTemplate(retryTemplate);

						AtomicInteger i = new AtomicInteger();
						messageSource.setSource(() -> {
							LaunchRequest request = new LaunchRequest();
							request.setApplicationName("foo");
							request.setCommandlineArguments(Arrays.asList("bar"));

							Message<Object> message = null;

							if (countDownLatch.getCount() > 0) {
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

	static class TestRetryListener implements RetryListener {
		private final Class<? extends Exception> expectedExceptionClass;
		private int retries;

		public TestRetryListener(Class<? extends Exception> expectedExceptionClass) {
			this.expectedExceptionClass = expectedExceptionClass;
		}

		public int getRetries() {
			return retries;
		}

		@Override
		public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
			return true;
		}

		@Override
		public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
			Throwable throwable) {
		}

		@Override
		public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
			Throwable throwable) {

			if (throwable != null && expectedExceptionClass.isAssignableFrom(throwable.getClass())) {
				this.retries = Integer.max(this.retries, context.getRetryCount());
			}

		}
	}
}
