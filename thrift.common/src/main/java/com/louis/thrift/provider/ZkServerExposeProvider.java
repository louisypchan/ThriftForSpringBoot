package com.louis.thrift.provider;


import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.louis.thrift.RpcConstants;
import com.louis.thrift.register.MonitorListener;
import com.louis.thrift.register.Registry;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;


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
public class ZkServerExposeProvider implements ServerExposeProvider {

    private final Logger logger = LoggerFactory.getLogger(ZkServerExposeProvider.class);

    private final Registry registry;

    private String service;

    private Set<InetSocketAddress> addresses = new HashSet<>();

    private volatile MonitorListener listener;

    private ZkServerExposeProvider(Registry registry){
        this.registry = registry;
        listener = event -> {
            ImmutablePair<String, byte[]> data = event.getData();
            switch (event.getType()){
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    addresses.add(getAddress(data.getKey()));
                    break;
                case CHILD_REMOVED:
                    addresses.removeIf(it -> {
                        InetSocketAddress inetSocketAddress = getAddress(data.getKey());
                        return inetSocketAddress.getHostName().equals(it.getHostName())
                                && inetSocketAddress.getPort() == it.getPort();
                    });
                    break;
                default:
                    break;
            }
        };
    }

    public static ZkServerExposeProvider build(Registry registry){
        return new ZkServerExposeProvider(registry);
    }

    public ZkServerExposeProvider service(String service){
        this.service = service;
        return this;
    }

    private InetSocketAddress getAddress(String key){
        Iterable<String> iterable = Splitter.on(":").split(key);
        List<String> list = Lists.newArrayList(iterable);
        String host = StringUtils.substringAfterLast(list.get(0), "/");
        int port = Integer.valueOf(list.get(1));
        return new InetSocketAddress(host, port);
    }

    public void buildPathChildrenCache(){
        Assert.notNull(service, "service must not be null");
        String path = String.format("/%s/%s", RpcConstants.ROOT, service);
        this.registry.subscribe(path, PathChildrenCache.StartMode.POST_INITIALIZED_EVENT, listener);
    }


    @Override
    public String getService() {
        return this.service;
    }

    @Override
    public List<InetSocketAddress> getServerAddressList() {
        return this.addresses.stream().collect(Collectors.toList());
    }

    @Override
    public InetSocketAddress select() {
        RandomPolicy randomPolicy = new RandomPolicy();
        return randomPolicy.get();
    }

    @Override
    public void close() throws IOException {
        this.registry.unSubscribe(String.format("/%s/%s", RpcConstants.ROOT, service), listener);
        this.registry.shutdown();
    }

    /**
     * random load balance
     */
    private class RandomPolicy{

        private List<InetSocketAddress> list = new ArrayList<>();

        public RandomPolicy(){
            list.addAll(addresses);
        }

        public InetSocketAddress get(){
            Random random = new Random();
            int pos = random.nextInt(list.size());
            return list.get(pos);
        }
    }
}
