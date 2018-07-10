package com.louis.thrift.server;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.louis.thrift.RpcConstants;
import com.louis.thrift.client.props.ThriftServerProperties;
import com.louis.thrift.register.Registry;
import com.louis.thrift.register.ZkRegistry;
import com.louis.thrift.zk.CuratorFactory;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/****************************************************************************
 Copyright (c) 2017 Louis Y P Chen.
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
 ****************************************************************************/
public class DefaultTServerFactory implements TServerFactory, Server{

    private final Logger logger = LoggerFactory.getLogger(DefaultTServerFactory.class);

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private Set<String> pathSet = Sets.newCopyOnWriteArraySet();

    private TServer server;

    private TServerBuilder builder;

    private Registry registry;

    private DefaultTServerFactory(TServerBuilder builder){
        this.builder = builder;
        if(builder.curatorFactory != null){
            this.registry = ZkRegistry.build().curatorFactory(builder.curatorFactory);
        }
        //add shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop();
            if(executor != null && !executor.isShutdown()){
                executor.shutdown();
            }
        }));
    }

    public static TServerBuilder custom(){
        return new TServerBuilder();
    }

    /**
     * register the service
     * reconnect even the session is lost
     */
    private void registerGuaranteed(){
        if(this.registry != null){
            for (Map.Entry<String, TProcessor> entry : builder.processorMap.entrySet()){
                String path = String.format("/%s/%s/%s:%s", RpcConstants.ROOT, entry.getKey(), builder.serverIpResolve.getServerIp(), String.valueOf(builder.port));
                byte[] data = String.valueOf(System.currentTimeMillis()).getBytes();
                this.registry.register(path, data);
                this.registry.watch(path, event -> {
                    switch (event.getType()) {
                        case NODE_CHANGED:
                            registry.register(path, data);
                            break;
                        default:
                            break;
                    }
                });
                pathSet.add(path);
            }
        }
    }

    @Override
    public TServer create(){
        //Assert.notNull(builder.processorMap, "processorMap must not be null");
        TMultiplexedProcessor tMultiplexedProcessor = new TMultiplexedProcessor();
        for (Map.Entry<String, TProcessor> entry : builder.processorMap.entrySet()){
            tMultiplexedProcessor.registerProcessor(entry.getKey(), entry.getValue());
        }
        try {
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(builder.port);
            TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(serverTransport);
            int protocolIndex = 0;
            if(builder.thriftServerProperties != null){
                protocolIndex = builder.thriftServerProperties.getProtocol();
                args.selectorThreads(builder.thriftServerProperties.getSelectorCount());
                args.acceptQueueSizePerThread(builder.thriftServerProperties.getAcceptQueueSizePerThread());
                if(builder.thriftServerProperties.getWorkerCount() != 0){
                    args.executorService(Executors.newFixedThreadPool(builder.thriftServerProperties.getWorkerCount()));
                }else{
                    args.executorService(Executors.newCachedThreadPool());
                }
            }else{
                //set work thread as same as available processor
                args.executorService(Executors.newFixedThreadPool(Runtime.getRuntime()
                        .availableProcessors()));
            }
            //set protocol
            args.protocolFactory(ThriftServerProperties.PROFACT[protocolIndex]);
            //set processor
            args.processor(tMultiplexedProcessor);
            //NIO
            args.transportFactory(new TFramedTransport.Factory());
            server = new TThreadedSelectorServer(args);
        } catch (TTransportException e) {
            logger.error("create thrift rpc server failed : " + e.getMessage());
        }finally {
            //tMultiplexedProcessor = null;
        }
        return server;
    }

    @Override
    public void start() {
        if(this.server != null){
            executor.submit(() -> {
                logger.info("Thrift server starts up on port : " + builder.port);
                server.serve();
            });
            registerGuaranteed();
        }
    }

    @Override
    public void stop() {
        if(this.server != null){
            executor.submit(()-> {
                logger.info("Thrift server stops on port : " + builder.port);
                server.stop();
            });
        }
        executor.shutdown();
        try {
            if(registry != null){
                for (String path : pathSet){
                    registry.unregister(path);
                    pathSet.remove(path);
                }
                registry.shutdown();
            }
        } catch (IOException e) {
            //
        }
    }

    public static class TServerBuilder{

        private ServerIpResolve serverIpResolve = new DefaultServerIpResolve();

        private int port = 8080;

        private Map<String, TProcessor> processorMap = Maps.newConcurrentMap();

        private CuratorFactory curatorFactory = null;

        private ThriftServerProperties thriftServerProperties;

        public TServerBuilder processorMap(Map<String, TProcessor> map){
            this.processorMap = map;
            return this;
        }

        public TServerBuilder port(Integer port){
            if(port != null){
                this.port = port.intValue();
            }
            return this;
        }

        public TServerBuilder curatorFactory(CuratorFactory curatorFactory){
            if(curatorFactory != null){
                this.curatorFactory = curatorFactory;
            }
            return this;
        }

        public TServerBuilder thriftServerProperties(ThriftServerProperties thriftServerProperties){
            this.thriftServerProperties = thriftServerProperties;
            return this;
        }

        public DefaultTServerFactory build(){
            return new DefaultTServerFactory(this);
        }
    }
}
