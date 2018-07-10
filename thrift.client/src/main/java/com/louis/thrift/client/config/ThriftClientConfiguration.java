package com.louis.thrift.client.config;

import com.louis.thrift.client.Bella;
import com.louis.thrift.client.ClientProxyFactory;
import com.louis.thrift.client.props.ThriftClientProperties;
import com.louis.thrift.provider.ZkServerExposeProvider;
import com.louis.thrift.register.ZkRegistry;
import com.louis.thrift.zk.CuratorFactory;
import com.louis.thrift.zk.DefaultCuratorFactory;
import com.louis.thrift.zk.ZkProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

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
@Configuration
@EnableConfigurationProperties({ThriftClientProperties.class, ZkProperties.class})
public class ThriftClientConfiguration {

    @Autowired(required = false)
    private String[] services;
    //
    @ConditionalOnMissingBean
    @ConditionalOnClass(CuratorFactory.class)
    @Bean("curatorFactory")
    public CuratorFactory curatorFactory(ZkProperties zkProperties){
        return DefaultCuratorFactory.build(zkProperties);
    }
    //
    @ConditionalOnMissingBean
    @ConditionalOnClass(Bella.class)
    @Bean(name = "rpc")
    public Bella rpc(CuratorFactory curatorFactory, ThriftClientProperties thriftClientProperties){
        Assert.notNull(services, "services must not be null");
        Map<String, ClientProxyFactory> map = new HashMap<>();
        for (String service : services){
            ZkServerExposeProvider zkServerExposeProvider = ZkServerExposeProvider.build(ZkRegistry.build().curatorFactory(curatorFactory))
                    .service(service);
            zkServerExposeProvider.buildPathChildrenCache();
            try {
                map.put(service, ClientProxyFactory.create(thriftClientProperties).serverExposeProvider(zkServerExposeProvider).configure());
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                e.printStackTrace();
            }
        }
        return new Bella(map);
    }
}
