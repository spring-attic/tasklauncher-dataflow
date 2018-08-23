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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.integration.util.DynamicPeriodicTrigger;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/
public class LaunchRequestConsumer implements SmartLifecycle {
	private static final Log log = LogFactory.getLog(LaunchRequestConsumer.class);
	private static final int BACKOFF_MULTIPLE = 2;
	private static final int BACKOFF_MAX_MULTIPLE = 8;

	private final PollableMessageSource input;
	private final AtomicBoolean running = new AtomicBoolean();
	private final AtomicBoolean paused = new AtomicBoolean();
	private final TaskOperations taskOperations;
	private final DynamicPeriodicTrigger trigger;
	private final ConcurrentTaskScheduler taskScheduler;
	private final long initialPeriod;

	private ScheduledFuture<?> scheduledFuture;

	public LaunchRequestConsumer(PollableMessageSource input, DynamicPeriodicTrigger trigger,
		TaskOperations taskOperations) {
		Assert.notNull(input, "`input` cannot be null.");
		Assert.notNull(taskOperations, "`taskOperations` cannot be null.");
		this.input = input;
		this.trigger = trigger;
		this.initialPeriod = trigger.getPeriod();
		this.taskOperations = taskOperations;
		this.taskScheduler = new ConcurrentTaskScheduler();

	}

	ScheduledFuture<?> consume() {

		return taskScheduler.schedule(() -> {
			if (!isRunning()) {
				return;
			}

			if (serverIsAcceptingNewTasks()) {
				if (paused.compareAndSet(true, false)) {
					log.info("Polling resumed");
					trigger.setPeriod(initialPeriod);
					if (initialPeriod > 0) {
						log.info(String.format("Polling period reset to %d ms.", trigger.getPeriod()));
					}
				}

				input.poll(message -> {
					LaunchRequest request = (LaunchRequest) message.getPayload();
					launchTask(request);
				}, new ParameterizedTypeReference<LaunchRequest>() {
				});
			}
			else {
				if (paused.compareAndSet(false, true)) {
					log.info("Polling paused");
				}
				if (trigger.getPeriod() > 0 && trigger.getPeriod() < initialPeriod * BACKOFF_MAX_MULTIPLE) {
					trigger.setPeriod(trigger.getPeriod() * BACKOFF_MULTIPLE);
					log.info(String.format("Polling period increased to %d ms.", trigger.getPeriod()));
				}
			}
		}, trigger);
	}

	@Override
	public boolean isAutoStartup() {
		return true;
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

	private boolean serverIsAcceptingNewTasks() {
		CurrentTaskExecutionsResource taskExecutionsResource = taskOperations.currentTaskExecutions();
		boolean availableForNewTasks =
			taskExecutionsResource.getRunningExecutionCount() < taskExecutionsResource.getMaximumTaskExecutions();
		return availableForNewTasks;
	}

	long launchTask(LaunchRequest request) {
		log.info(String.format("Launching Task %s", request.getApplicationName()));
		return taskOperations.launch(request.getApplicationName(), request.getDeploymentProperties(),
			request.getCommandlineArguments());
	}
}
