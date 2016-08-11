/* ============================================================================
*
* FILE: Server.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.reactiva.hazelq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastInstanceFactory;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.Resource;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.reactiva.hazelq.core.QueueService;
import com.reactiva.hazelq.core.UIDGenerator;

@SpringBootApplication(exclude = {HazelcastAutoConfiguration.class})
@EnableConfigurationProperties(HazelcastProperties.class)
public class HQServer {
    private static final Logger log = LoggerFactory.getLogger(HQServer.class);
	@Configuration
	static class HazelcastConfigFileConfiguration {

		@Autowired
		private HazelcastProperties hazelcastProperties;

		@Bean
		public HazelcastInstance hazelcastInstance() throws IOException {
			log.info("::::::::: Startup sequence initiated ::::::::");
			log.info("Booting Hazelcast system..");
			Resource config = this.hazelcastProperties.resolveConfigLocation();
			if (config != null) {
				return new HazelcastInstanceFactory(config).getHazelcastInstance();
			}
			return Hazelcast.newHazelcastInstance();
		}

	}
	
  @Bean
  @DependsOn("hazelcastInstance")
  public QueueService service()
  {
    return new QueueService();
  }
  @Bean
  @DependsOn({"service"})
  public UIDGenerator uidgen()
  {
    return new UIDGenerator();
  }
  
  public static void main(String[] args) {
	  new SpringApplicationBuilder()
	    .sources(HQServer.class)
	    .run(args);
  }
  
}
