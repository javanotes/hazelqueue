package com.reactiva.hazelq.net;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.reactiva.hazelq.TCPConfig;
import com.reactiva.hazelq.protoc.LengthBasedCodec;
import com.reactiva.hazelq.protoc.impl.HQCodecWrapper;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
/**
 * The Netty server listener class that registers channel handler adapters.
 * @author esutdal
 *
 */
public class TCPConnector implements Runnable{
	private EventExecutorGroup eventExecutors, procExecutors;
	private HQCodecWrapper codecHandler;
	public LengthBasedCodec getCodecHandler() {
		return codecHandler;
	}
	public void setCodecHandler(HQCodecWrapper codecHandler) {
		this.codecHandler = codecHandler;
	}
	
	/**
	 * 
	 * @param ch
	 * @throws Exception
	 */
	protected void serverHandlers(SocketChannel ch) throws Exception
	{
		
		Assert.notNull(config);
		Assert.notNull(codecHandler);
		/**
		 * The maxFrameLength can be set as per the protocol design (if defined). That
		 * would enable rejection of too long messages. Right now, this an arbitrary number
		 * assuming the actual message size would be much lesser than that.
		 * TODO: make LengthFieldBasedFrameDecoder configurable?
		 */
		ch.pipeline().addLast(eventExecutors, new LengthFieldBasedFrameDecoder(config.getProtoLenMax(), config.getProtoLenOffset(), 
				config.getProtoLenBytes(), Math.negateExact(config.getProtoLenBytes()), 0));
		
		//ch.pipeline().addLast(executor, new LengthFieldBasedFrameDecoder(config.protoLenMax, config.protoLenOffset, config.protoLenBytes));
		
		ch.pipeline().addLast(eventExecutors, new RequestConvertorHandler(codecHandler));
		//ch.pipeline().addLast(concExecutor, processor);
		ch.pipeline().addLast(procExecutors, getProcessorAsync());
		ch.pipeline().addLast(eventExecutors, getEncoder());
		ch.pipeline().addLast(eventExecutors, getTerminal());
		
	}
		
	/**
	 * Strategy class for loading appropriate handlers.
	 * @author esutdal
	 *
	 */
	private final class HandlerInitializer extends ChannelInitializer<SocketChannel> {
		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			
			serverHandlers(ch);
			
		}
	}
	private static Logger log = LoggerFactory.getLogger(TCPConnector.class);
	private int port, ioThreads, execThreads;
	
	private RequestProcessorHandler processor;
	private ResponseConvertorHandler encoder;
	private TerminalHandler terminal;
	
	private final boolean proxy;
	private TCPConfig config;
	/**
	 * TCP server listener with provided IO threads and executor threads
	 * @param port
	 * @param ioThreadCount
	 * @param execThreadCount
	 */
	public TCPConnector(int port, int ioThreadCount, int execThreadCount) {
		this.port = port;
		ioThreads = ioThreadCount;
		execThreads = execThreadCount;
		this.proxy = false;
		
	}
	
	/**
	 * TCP server listener with provided IO threads and processor count based executor threads
	 * @param port
	 * @param workerThreadCount
	 */
	public TCPConnector(int port, int workerThreadCount) {
		this(port, workerThreadCount, Runtime.getRuntime().availableProcessors());
	}
	
	/**
	 * 
	 */
	private void initIOThreads()
	{
		eventLoop = new NioEventLoopGroup(ioThreads, new ThreadFactory() {
			int n = 1;

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "xcomm-io-" + (n++));
				return t;
			}
		});
		bossLoop = new NioEventLoopGroup(1, new ThreadFactory() {
			int n = 1;

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "xcomm-accept-" + (n++));
				return t;
			}
		});
	}
	/**
	 * 
	 */
	private void initExecThreads()
	{
		eventExecutors = new DefaultEventExecutorGroup(config.getEventThreadCount(), new ThreadFactory() {
			int n = 1;
			@Override
			public Thread newThread(Runnable arg0) {
				Thread t = new Thread(arg0, "xcomm-event-"+(n++));
				return t;
			}
		});
		
		procExecutors = new DefaultEventExecutorGroup(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable arg0) {
				Thread t = new Thread(arg0, "xcomm-execgrp");
				return t;
			}
		}) 
		{
			@Override
			protected EventExecutor newChild(Executor executor, Object... args) throws Exception {
				return new ConcurrentEventExecutor(this, executor, (Integer) args[0],
						(RejectedExecutionHandler) args[1], execThreads);
				
			}
		};
	}
	private ServerBootstrap server;
	private NioEventLoopGroup eventLoop, bossLoop;
	/**
	 * Setup the transport channel
	 */
	private void setup()
	{
		initIOThreads();
		initExecThreads();
				
		server = new ServerBootstrap()
				.group(bossLoop, eventLoop)
				.channel(NioServerSocketChannel.class)
				.childHandler(new HandlerInitializer())
				.option(ChannelOption.SO_BACKLOG, 256)    
	            ;
	}
	
	@Override
	public void run() {
		//TODO: can be implemented for some monitoring stuff.
		while(running)
		{
			try {
				doMonitorTask();
			} finally {
			}
		}

	}
	
	public void stopMonitor()
	{
		running = false;
	}
	protected void doMonitorTask() {
		log.debug("--Monitor task run --");
		
	}
	private volatile boolean running = false;
	/**
	 * 
	 */
	public void stopServer() {
		future.channel().closeFuture().syncUninterruptibly();
		eventLoop.shutdownGracefully().syncUninterruptibly();
		bossLoop.shutdownGracefully().syncUninterruptibly();
		if(eventExecutors != null)
			eventExecutors.shutdownGracefully().syncUninterruptibly();
		if(procExecutors != null)
			procExecutors.shutdownGracefully().syncUninterruptibly();
		log.info("Stopped transport on port "+port);
	}

	private ChannelFuture future;
	/**
	 * @throws InterruptedException 
	 * 
	 */
	public void startServer() throws InterruptedException {
		setup();
		future = server.bind(port).sync();
		log.info("Started TCP transport on port "+port + " in "+(proxy ? "PROXY" : "SERVER") + " mode");
		running = true;
	}
	private RequestProcessorHandlerAsync processorAsync;
	
	public TCPConfig getConfig() {
		return config;
	}
	public void setConfig(TCPConfig config) {
		this.config = config;
	}
	public RequestProcessorHandlerAsync getProcessorAsync() {
		return processorAsync;
	}
	public void setProcessorAsync(RequestProcessorHandlerAsync processorAsync) {
		this.processorAsync = processorAsync;
	}
	public ResponseConvertorHandler getEncoder() {
		return encoder;
	}
	public void setEncoder(ResponseConvertorHandler encoder) {
		this.encoder = encoder;
	}
	public RequestProcessorHandler getProcessor() {
		return processor;
	}
	public void setProcessor(RequestProcessorHandler processor) {
		this.processor = processor;
	}
	public TerminalHandler getTerminal() {
		return terminal;
	}
	public void setTerminal(TerminalHandler terminal) {
		this.terminal = terminal;
	}
	

}
