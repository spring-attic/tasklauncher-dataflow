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

import javax.annotation.PostConstruct;
import javax.validation.constraints.Min;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author David Turanski
 **/
@ConfigurationProperties
public class DataflowTaskLauncherSinkProperties {

	private TriggerProperties trigger = new TriggerProperties();

	/**
	 * The Spring Cloud Data Flow platform to use for launching tasks.
	 */
	private String platformName = "default";

	public String getPlatformName() {
		return platformName;
	}

	public void setPlatformName(String platformName) {
		this.platformName = platformName;
	}

	public static class TriggerProperties {

		/**
		 * The initial delay in milliseconds.
		 */
		private long initialDelay = 1000;

		/**
		 * The polling period in milliseconds.
		 */
		private long period = 1000;

		/**
		 * The maximum polling period in milliseconds. Will be set to period if period > maxPeriod.
		 */
		private long maxPeriod = 30000;

		@Min(0)
		public long getInitialDelay() {
			return initialDelay;
		}

		public void setInitialDelay(int initialDelay) {
			this.initialDelay = initialDelay;
		}

		@Min(0)
		public long getPeriod() {
			return period;
		}

		public void setPeriod(int period) {
			this.period = period;
		}

		@Min(1000)
		public long getMaxPeriod() {
			return maxPeriod;
		}

		public void setMaxPeriod(int maxPeriod) {
			this.maxPeriod = maxPeriod;
		}

		@PostConstruct
		public void checkMaxPeriod() {
			maxPeriod = Long.max(maxPeriod, period);
		}
	}
}
