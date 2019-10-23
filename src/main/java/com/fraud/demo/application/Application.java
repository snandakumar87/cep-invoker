/**
 *  Copyright 2005-2016 Red Hat, Inc.
 *
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.fraud.demo.application;


import com.fraud.demo.transformer.ParseCaseData;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RestController;

/**
 * A spring-boot application that includes a Camel route builder to setup the
 * Camel routes
 */
@SpringBootApplication
@RestController
public class Application {

	// must have a main method spring-boot can run
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	private String kafkaBootstrap = System.getenv("BOOTSTRAP_SERVERS");

	private String consumerMaxPollRecords = System.getenv("CONSUMER_MAX_POLL_RECORDS");
	private String consumerCount = System.getenv("CONSUMER_COUNT");
	private String consumerSeekTo = System.getenv("CONSUMER_SEEK_TO");
	private String consumerGroup = System.getenv("CONSUMER_GROUP");

	@Bean
	public RouteBuilder routeBuilder() {

		return new RouteBuilder() {


			@Override
			public void configure() throws Exception {
				restConfiguration()
						.component("servlet")
						.bindingMode(RestBindingMode.auto)
						.producerComponent("http4").host("localhost:8090");


				System.out.println("Configuring Creditor Core Banking Routes");




				from("kafka:" + "inp-txn-topic" + "?brokers=" + kafkaBootstrap + "&maxPollRecords="
						+ consumerMaxPollRecords + "&consumersCount=" + consumerCount + "&seekTo=" + consumerSeekTo
						+ "&groupId=" + consumerGroup)
						.log("\n/// Checking my glue")
						.bean(ParseCaseData.class,"process")
						.log("parsed message for case ${body}")
						.filter(x-> null != x.getIn().getBody())
						.to("kafka:fraudres-txn-topic?brokers="+kafkaBootstrap)
						.log("To Direct BC");



			}
		};
	}

}
