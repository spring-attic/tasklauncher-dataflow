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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.dataflow.rest.resource.LauncherResource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.PagedModel;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 * A Message consumer that submits received task {@link LaunchRequest}s to a Data Flow server. This
 * polls a {@link PollableMessageSource} only if the Data Flow server is not at its concurrent task execution limit.
 *
 * The consumer runs as a {@link ScheduledFuture} , configured with a {@link DynamicPeriodicTrigger} to
 * support exponential backoff up to a maximum period. Every period cycle, the poller first makes a REST call to the
 * Data Flow server to check if it can accept a new task LaunchRequest before checking the Message source. The
 * polling period will back off (increase) when either the server is not accepting requests or no request is received.
 *
 *  The period will revert to its initial value whenever both a request is received and the DataFlow Server is
 *  accepting launch requests. The period remain at the maximum value when there are no requests to avoid hammering
 *  the Data Flow server for no reason.
 *
 * @author David Turanski
 **/
public class LaunchRequestConsumer implements SmartLifecycle {
	private static final Log log = LogFactory.getLog(LaunchRequestConsumer.class);
	private static final int BACKOFF_MULTIPLE = 2;
	static final String TASK_PLATFORM_NAME = "spring.cloud.dataflow.task.platformName";

	private final PollableMessageSource input;
	private final AtomicBoolean running = new AtomicBoolean();
	private final AtomicBoolean paused = new AtomicBoolean();
	private final TaskOperations taskOperations;
	private final DynamicPeriodicTrigger trigger;
	private final ConcurrentTaskScheduler taskScheduler;
	private final long initialPeriod;
	private final long maxPeriod;
	private volatile boolean autoStart = true;

	private ScheduledFuture<?> scheduledFuture;

	private String platformName = "default";

	public LaunchRequestConsumer(PollableMessageSource input, DynamicPeriodicTrigger trigger,
		long maxPeriod, TaskOperations taskOperations) {
		Assert.notNull(input, "`input` cannot be null.");
		Assert.notNull(taskOperations, "`taskOperations` cannot be null.");
		this.input = input;
		this.trigger = trigger;
		this.initialPeriod = trigger.getDuration().toMillis();
		this.maxPeriod = maxPeriod;
		this.taskOperations = taskOperations;
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

			if (platformIsAcceptingNewTasks()) {
				if (paused.compareAndSet(true, false)) {
					log.info("Polling resumed");
				}

				if (!input.poll(message -> {
					LaunchRequest request = (LaunchRequest) message.getPayload();
					log.debug("Received a Task launch request - task name:  " + request.getTaskName());
					launchTask(request);
				}, new ParameterizedTypeReference<LaunchRequest>() {
				})) {
					backoff("No task launch request received");
				}
				else {
					if (trigger.getDuration().toMillis() > initialPeriod) {
						trigger.setDuration(Duration.ofMillis(initialPeriod));
						log.info(String.format("Polling period reset to %d ms.", trigger.getDuration().toMillis()));
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
			synchronized (trigger) {
			if (trigger.getDuration().compareTo(Duration.ZERO) > 0
				&& trigger.getDuration().compareTo(Duration.ofMillis(maxPeriod)) < 0) {

				Duration duration = trigger.getDuration();

				if (duration.multipliedBy(BACKOFF_MULTIPLE).compareTo(Duration.ofMillis(maxPeriod)) <= 0) {
					//If d >= 1, round to 1 seconds.
					if (duration.getSeconds() == 1) {
						duration = Duration.ofSeconds(1);
					}
					duration = duration.multipliedBy(BACKOFF_MULTIPLE);
				}
				else {
					duration = Duration.ofMillis(maxPeriod);
				}
				if (trigger.getDuration().toMillis() < 1000) {
					log.info(String.format(message + " - increasing polling period to %d ms.", duration.toMillis()));
				}
				else {
					log.info(
						String.format(message + "- increasing polling period to %d seconds.", duration.getSeconds()));
				}

				trigger.setDuration(duration);
			}
			else if (trigger.getDuration() == Duration.ofMillis(maxPeriod)) {
				log.info(message);
			}
		}
	}

	private boolean platformIsAcceptingNewTasks() {

		boolean availableForNewTasks = false;
		int maximumTaskExecutions = 0;
		int runningExecutionCount = 0;

		List<String> currentPlatforms = new ArrayList<>();
		try {
			boolean validPlatform = false;
			for (CurrentTaskExecutionsResource currentTaskExecutionsResource: taskOperations.currentTaskExecutions()){
				if (currentTaskExecutionsResource.getName().equals(platformName)){
					maximumTaskExecutions = currentTaskExecutionsResource.getMaximumTaskExecutions();
					runningExecutionCount = currentTaskExecutionsResource.getRunningExecutionCount();
					validPlatform = true;
				}
				currentPlatforms.add(currentTaskExecutionsResource.getName());
			}

			// Verify for each request as configuration may have changed on server.
			assertValidPlatform(validPlatform, currentPlatforms);

			availableForNewTasks = runningExecutionCount < maximumTaskExecutions;
			if (!availableForNewTasks) {
				log.warn(String.format(
					"The data Flow task platform %s has reached its concurrent task execution limit: (%d)",
					platformName,
					maximumTaskExecutions));
			}
		}
		// If cannot connect to Data Flow server, log the exception and return false so the poller will back off.
		catch( Exception e) {
			log.error(e.getMessage(), e);
		}
		finally {
			return availableForNewTasks;
		}
	}

	// Here we need to throw any exception to retry the message.
	private long launchTask(LaunchRequest request) {
		String requestPlatformName = request.getDeploymentProperties().get(TASK_PLATFORM_NAME);
		if (StringUtils.hasText(requestPlatformName) && !platformName.equals(requestPlatformName)) {
			throw new IllegalStateException(
				String.format("Task Launch request for Task %s contains deployment property '%s=%s' which does not " +
					"match the platform configured for the Task Launcher: '%s'",
					request.getTaskName(),
					TASK_PLATFORM_NAME,
					request.getDeploymentProperties().get(TASK_PLATFORM_NAME),
					platformName));
		}
		log.info(String.format("Launching Task %s on platform %s", request.getTaskName(), platformName));
		return taskOperations.launch(request.getTaskName(),
			enrichDeploymentProperties(request.getDeploymentProperties()),
			request.getCommandlineArguments(), null);
	}

	private Map<String, String> enrichDeploymentProperties(Map<String, String> deploymentProperties) {
		if (!deploymentProperties.containsKey(TASK_PLATFORM_NAME)) {
			Map<String, String> enrichedProperties = new HashMap<>();
			enrichedProperties.putAll(deploymentProperties);
			enrichedProperties.put(TASK_PLATFORM_NAME, platformName);
			return enrichedProperties;
		}
		return deploymentProperties;
	}

	public void setPlatformName(String platformName) {
		this.platformName = platformName;
	}

	@PostConstruct
	public void verifyTaskPlatform() {
		PagedModel<LauncherResource> launchers = taskOperations.listPlatforms();

		boolean validPlatform = false;
		List<String> currentPlatforms = new ArrayList<>();

		for (LauncherResource launcherResource : launchers) {
			currentPlatforms.add(launcherResource.getName());
			if (launcherResource.getName().equals(platformName)) {
				validPlatform = true;
			}
		}

		assertValidPlatform(validPlatform, currentPlatforms);
	}

	private void assertValidPlatform(boolean validPlatform, List<String> currentPlatforms) {
		Assert.notEmpty(currentPlatforms, "The Data Flow Server has no task platforms configured");

		Assert.isTrue(validPlatform, String.format(
			"The task launcher's platform name '%s' does not match one of the Data Flow server's configured task "
				+ "platforms: [%s].",
			platformName, StringUtils.collectionToCommaDelimitedString(currentPlatforms)));
	}

}
