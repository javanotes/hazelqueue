package com.reactiva.hazelq.protoc;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.reactiva.hazelq.protoc.impl.StreamedHQCodec;

@Configuration
public class ProtoConfig {
	@Bean
	public StreamedHQCodec streamCodec()
	{
		return new StreamedHQCodec();
	}
}
