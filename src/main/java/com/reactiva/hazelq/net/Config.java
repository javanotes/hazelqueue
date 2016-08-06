package com.reactiva.hazelq.net;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.reactiva.hazelq.net.pipe.RequestProcessorHandler;
import com.reactiva.hazelq.net.pipe.RequestProcessorHandlerAsync;
import com.reactiva.hazelq.net.pipe.ResponseConvertorHandler;
import com.reactiva.hazelq.net.pipe.TerminalHandler;
import com.reactiva.hazelq.protoc.impl.HQCodecWrapper;

@Configuration
public class Config {

	public boolean isProxyMode() {
		return proxyMode;
	}
	public int getPort() {
		return port;
	}
	public int getMaxThread() {
		return ioThreadCount;
	}
	public int getProtoLenMax() {
		return protoLenMax;
	}
	public int getProtoLenOffset() {
		return protoLenOffset;
	}
	public int getProtoLenBytes() {
		return protoLenBytes;
	}
	@Value("${server.gw: false}")
	boolean proxyMode;
	@Value("${server.port}")
	int port;
	@Value("${server.io-threads: 1}")
	int ioThreadCount;
	@Value("${server.event-threads: 2}")
	int eventThreadCount;
	@Value("${server.proto.buffered: false}")
	boolean useByteBuf;
	
	@Value("${server.proto.charset:UTF8}")
	String charset;
	
	public int getEventThreadCount() {
		return eventThreadCount;
	}
	public void setEventThreadCount(int eventThreadCount) {
		this.eventThreadCount = eventThreadCount;
	}
	@Value("${server.exec-threads: 4}")
	int execThreadCount;
	@Value("${server.monitor.enable: false}")
	boolean monitor;
	
	public boolean isMonitorEnabled() {
		return monitor;
	}
	@Value("${server.proto.len.max: 1000}")
	int protoLenMax;
	@Value("${server.proto.len.offset: 0}")
	int protoLenOffset;
	@Value("${server.proto.len.bytes: 4}")
	int protoLenBytes;
	@Value("${server.proto.close-on-flush: false}")
	boolean closeOnFlush;
	@Bean
	@ConfigurationProperties(prefix = "server.gw")
	public HostAndPort targets()
	{
		return new HostAndPort();
	}
			
	public static class HostAndPort
	{
		@Override
		public String toString() {
			return "HostAndPort [target=" + target + "]";
		}

		public Map<String, String> getTarget() {
			return target;
		}

		public void setTarget(Map<String, String> target) {
			this.target = target;
		}

		private Map<String, String> target;
	}
	private Thread monitorThread;
	@PostConstruct
	public void init() throws Exception
	{
		server().startServer();
		if (isMonitorEnabled()) {
			//TODO: monitor thread should be a scheduled executor
			//just being lazy to not implement it
			monitorThread = new Thread(server(), "TCPMon");
			monitorThread.setDaemon(true);
			monitorThread.start();
		}
	}
	@PreDestroy
	public void destroy() throws Exception
	{
		server().stopServer();
		server().stopMonitor();
		if (monitorThread != null) {
			monitorThread.interrupt();
		}
		
	}
	@Bean
	@DependsOn({"service", "encoder", "processor", "processorAsync", "targets"})
	public TCPConnector server() throws Exception
	{
		TCPConnector s = new TCPConnector(port, ioThreadCount, execThreadCount, proxyMode);
		s.setConfig(this);
		//in server mode, initialize request handlers.
		if (!proxyMode) {
			s.setCodecHandler(codec());
			s.processorAsync = processorAsync();
			s.encoder = encoder();
			s.processor = processor();
			s.terminal = terminal();
		}
		else
		{
			//proxy mode
			System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++");
			System.err.println("|  ** WARN: Proxy implementation is unstable **  |");
			System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++");
		}
		
		return s;
	}
	
	
	@Bean
	RequestProcessorHandler processor() throws Exception
	{
		return new RequestProcessorHandler();
	}
	@Bean
	RequestProcessorHandlerAsync processorAsync() throws Exception
	{
		return new RequestProcessorHandlerAsync();
	}
	
	@Bean
	@DependsOn({"codec"})
	ResponseConvertorHandler encoder() throws Exception
	{
		return new ResponseConvertorHandler(codec(),useByteBuf);
	}
	@Bean
	HQCodecWrapper codec()
	{
		try {
			return new HQCodecWrapper(Charset.forName(charset), useByteBuf);
		} catch (IllegalCharsetNameException e) {
			log.warn("Unrecognized character set => "+e.getMessage()+". Using UTF8");
			return new HQCodecWrapper(StandardCharsets.UTF_8, useByteBuf);
		}
	}
	private static final Logger log = LoggerFactory.getLogger(Config.class);
	@Bean
	TerminalHandler terminal()
	{
		TerminalHandler t = new TerminalHandler();
		t.setCloseOnFlush(closeOnFlush);
		return t;
	}
	
}
