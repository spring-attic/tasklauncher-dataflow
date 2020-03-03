/*
 * Copyright 2019-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientProperties;
import org.springframework.cloud.dataflow.rest.resource.LauncherResource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.PagedModel;

/**
 * @author David Turanski
 **/
public class DataflowTaskLauncherPlatformTests {

	private ApplicationContextRunner contextRunner;

	@BeforeEach
	public void setUp() {
		contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(TaskLauncherDataflowSinkConfiguration.class, TestConfig.class);
	}

	@Test
	public void defaultPlatform() {
		contextRunner.run(context -> context.isRunning());
	}

	@Test
	public void nonExistentPlatformFails() {
		Exception exception = assertThrows(IllegalStateException.class, () -> {
			contextRunner
				.withPropertyValues("platformName=foo")
				.run(context -> context.isRunning());
		});
		assertThat(exception.getCause()).isInstanceOf(BeanCreationException.class);
		assertThat(exception.getCause().getCause()).isInstanceOf(IllegalArgumentException.class);
		assertThat(exception.getCause().getCause().getMessage()).isEqualTo(
			"The task launcher's platform name 'foo' does not match one of the Data Flow server's configured task "
				+ "platforms: [default]."
		);
	}

	@Test
	public void noLaunchersConfigured() {
		Exception exception = assertThrows(IllegalStateException.class, () -> {
			contextRunner
				.withPropertyValues("spring.profiles.active=nolaunchers")
				.run(context -> context.isRunning());
		});

		assertThat(exception.getCause()).isInstanceOf(BeanCreationException.class);
		assertThat(exception.getCause().getCause()).isInstanceOf(IllegalArgumentException.class);
		assertThat(exception.getCause().getCause().getMessage()).isEqualTo(
			"The Data Flow Server has no task platforms configured"
		);
	}

	static class TestConfig {

		@Bean
		@Profile("default")
		TaskOperations taskOperations() {
			TaskOperations taskOperations = mock(TaskOperations.class);
			LauncherResource launcherResource = mock(LauncherResource.class);
			when(launcherResource.getName()).thenReturn("default");

			when(taskOperations.listPlatforms()).thenReturn(new PagedModel<>(
				Collections.singletonList(launcherResource), null));
			return taskOperations;
		}

		@Bean
		@Profile("nolaunchers")
		TaskOperations taskOperationsNoLaunchers() {
			TaskOperations taskOperations = mock(TaskOperations.class);
			when(taskOperations.listPlatforms()).thenReturn(new PagedModel<>(
				Collections.emptyList(), null));
			return taskOperations;
		}

		@Bean
		DataFlowOperations dataFlowOperations(TaskOperations taskOperations) {
			DataFlowOperations dataFlowOperations = mock(DataFlowOperations.class);
			when(dataFlowOperations.taskOperations()).thenReturn(taskOperations);
			return dataFlowOperations;
		}

		@Bean
		DataFlowClientProperties dataFlowClientProperties() {
			return new DataFlowClientProperties();
		}

	}
}
