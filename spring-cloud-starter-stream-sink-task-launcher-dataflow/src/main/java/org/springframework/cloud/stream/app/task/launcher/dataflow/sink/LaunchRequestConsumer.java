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

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.dataflow.rest.client.DataFlowClientException;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.util.Assert;

/**
 * A Message consumer that submits received task {@link LaunchRequest}s to a Data Flow server. This
 * polls a {@link PollableMessageSource} only if the Data Flow server is not at its concurrent task execution limit.
 * <p>
 * The consumer runs as a {@link ScheduledFuture} , configured with a {@link DynamicPeriodicTrigger} to
 * support exponential backoff up to a maximum period. Every period cycle, the poller first makes a REST call to the
 * Data Flow server to check if it can accept a new task LaunchRequest before checking the Message source. The
 * polling period will back off (increase) when either the server is not accepting requests or no request is received.
 * <p>
 * The period will revert to its initial value whenever both a request is received and the DataFlow Server is
 * accepting launch requests. The period remain at the maximum value when there are no requests to avoid hammering
 * the Data Flow server for no reason.
 *
 * @author David Turanski
 **/
public class LaunchRequestConsumer implements SmartLifecycle {
	private static final Log log = LogFactory.getLog(LaunchRequestConsumer.class);
	private static final int BACKOFF_MULTIPLE = 2;

	private final PollableMessageSource input;
	private final AtomicBoolean running = new AtomicBoolean();
	private final AtomicBoolean paused = new AtomicBoolean();
	private final TaskOperations taskOperations;
	private final DynamicPeriodicTrigger trigger;
	private final ConcurrentTaskScheduler taskScheduler;
	private final long initialPeriod;
	private final long maxPeriod;
	private final RetryTemplate retryTemplate;
	private volatile boolean autoStart = true;

	private ScheduledFuture<?> scheduledFuture;

	public LaunchRequestConsumer(PollableMessageSource input, DynamicPeriodicTrigger trigger, long maxPeriod,
		TaskOperations taskOperations, RetryTemplate retryTemplate) {
		Assert.notNull(input, "`input` cannot be null.");
		Assert.notNull(taskOperations, "`taskOperations` cannot be null.");
		this.input = input;
		this.trigger = trigger;
		this.initialPeriod = trigger.getPeriod();
		this.maxPeriod = maxPeriod;
		this.taskOperations = taskOperations;
		this.retryTemplate = retryTemplate;
		this.taskScheduler = new ConcurrentTaskScheduler();
	}

	/*
	 * Polling loop
	 */
	ScheduledFuture<?> consume() {

		return taskScheduler.schedule(() -> {
			if (!isRunning()) {
				return;
			}

			if (serverIsAcceptingNewTasks()) {
				if (paused.compareAndSet(true, false)) {
					log.info("Polling resumed");
				}

				if (!input.poll(message -> {
					LaunchRequest request = (LaunchRequest) message.getPayload();
					log.debug("Received a Task launch request - task name:  " + request.getApplicationName());
					retryLaunchTask(request);
				}, new ParameterizedTypeReference<LaunchRequest>() {
				})) {
					backoff("No task launch request received");
				}
				else {

					if (trigger.getPeriod() > initialPeriod) {
						trigger.setPeriod(initialPeriod);
						log.info(String.format("Polling period reset to %d ms.", trigger.getPeriod()));
					}
				}
			}
			else {
				paused.set(true);
				backoff("Polling paused");

			}
		}, trigger);
	}

	@Override
	public boolean isAutoStartup() {
		return autoStart;
	}

	public void setAutoStartup(boolean autoStart) {
		this.autoStart = autoStart;
	}

	@Override
	public synchronized void stop(Runnable callback) {
		if (callback != null) {
			callback.run();
		}
		this.stop();
	}

	@Override
	public void start() {
		if (running.compareAndSet(false, true)) {
			this.scheduledFuture = consume();
		}
	}

	@Override
	public void stop() {
		if (running.getAndSet(false)) {
			this.scheduledFuture.cancel(false);
		}
	}

	@Override
	public boolean isRunning() {
		return running.get();
	}

	public boolean isPaused() {
		return paused.get();
	}

	@Override
	public int getPhase() {
		return Integer.MAX_VALUE;
	}

	private void backoff(String message) {
		if (trigger.getPeriod() > 0 && trigger.getPeriod() < Duration.ofMillis(maxPeriod).toMillis()) {

			Duration duration = Duration.ofMillis(trigger.getPeriod());

			if (duration.multipliedBy(BACKOFF_MULTIPLE).compareTo(Duration.ofMillis(maxPeriod)) <= 0) {
				//If d >= 1, round to 1 seconds.
				if (duration.getSeconds() == 1) {
					duration = duration.ofSeconds(1);
				}
				duration = duration.multipliedBy(BACKOFF_MULTIPLE);
			}
			else {
				duration = Duration.ofMillis(maxPeriod);
			}
			if (trigger.getPeriod() < 1000) {
				log.info(String.format(message + " - increasing polling period to %d ms.", duration.toMillis()));
			}
			else {
				log.info(String.format(message + "- increasing polling period to %d seconds.", duration.getSeconds()));
			}

			trigger.setPeriod(duration.toMillis());
		}
		else if (trigger.getPeriod() == Duration.ofMillis(maxPeriod).toMillis()) {
			log.info(message);
		}
	}

	private boolean serverIsAcceptingNewTasks() {

		boolean availableForNewTasks = false;

		try {
			CurrentTaskExecutionsResource taskExecutionsResource = taskOperations.currentTaskExecutions();

			availableForNewTasks =
				taskExecutionsResource.getRunningExecutionCount() < taskExecutionsResource.getMaximumTaskExecutions();
			if (!availableForNewTasks) {
				log.warn(String.format("Data Flow server has reached its concurrent task execution limit: (%d)",
					taskExecutionsResource.getMaximumTaskExecutions()));
			}
		}
		// If cannot connect to Data Flow server, log the exception and return false so the poller will back off.
		catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		finally {
			return availableForNewTasks;
		}
	}

	/*
	 * Wrap launchTask() in a RetryTemplate.
	 */
	private void retryLaunchTask(LaunchRequest request) {
		this.retryTemplate.execute(context -> launchTask(request));
	}

	/*
	 * Ask the server to launch the task. This is only called when serverIsAcceptingNewTasks() returns true. However
	 * things may have changed in the meantime. If the DataFlowClientException message indicates the task execution
	 * limit has been reached, convert the exception to a TaskExecutionLimitException. The local RetryTemplate callback
	 * will only retry on this type of exception.
	 */
	private long launchTask(LaunchRequest request) {
		log.info(String.format("Launching Task %s", request.getApplicationName()));
		long taskId = 0;
		try {
			taskId = taskOperations.launch(request.getApplicationName(), request.getDeploymentProperties(),
				request.getCommandlineArguments());
			log.info(
				String.format(String.format("Task %s launched - task id = %d", request.getApplicationName(), taskId)));
		}
		catch (DataFlowClientException e) {
			log.error(e.getMessage().trim());
			TaskExecutionLimitException.throwOnMessageMatch(e);
			log.error("", e);
			throw e;

		}
		return taskId;
	}

}
