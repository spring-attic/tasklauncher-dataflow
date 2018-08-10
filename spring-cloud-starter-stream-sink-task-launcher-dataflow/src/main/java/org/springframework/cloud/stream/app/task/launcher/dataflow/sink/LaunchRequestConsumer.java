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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.util.Assert;

/**
 * @author David Turanski
 **/
public class LaunchRequestConsumer implements SmartLifecycle {
	private static final Log log = LogFactory.getLog(LaunchRequestConsumer.class);
	private final PollableMessageSource input;
	private final AtomicBoolean running = new AtomicBoolean();
	private final AtomicBoolean paused = new AtomicBoolean();
	private final TaskOperations taskOperations;
	private final Trigger trigger;
	private final ConcurrentTaskScheduler taskScheduler;

	public LaunchRequestConsumer(PollableMessageSource input, Trigger trigger, TaskOperations taskOperations) {
		Assert.notNull(input, "`input` cannot be null.");
		Assert.notNull(taskOperations, "`taskOperations` cannot be null.");
		this.input = input;
		this.trigger = trigger;
		this.taskOperations = taskOperations;
		this.taskScheduler = new ConcurrentTaskScheduler();

	}

	public void consume() {

		taskScheduler.schedule(() -> {
			if (!isRunning()) {
				return;
			}

			if (serverIsAcceptingNewTasks()) {
				if (paused.compareAndSet(true, false)) {
					log.info("Polling resumed");
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
			consume();
		}
	}

	@Override
	public void stop() {
		running.set(false);
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
		return taskExecutionsResource.getRunningExecutionCount() < taskExecutionsResource.getMaximumTaskExecutions();
	}

	long launchTask(LaunchRequest request) {
		log.info(String.format("Launching Task %s", request.getApplicationName()));
		return taskOperations.launch(request.getApplicationName(), request.getDeploymentProperties(),
			request.getCommandlineArguments());
	}
}
