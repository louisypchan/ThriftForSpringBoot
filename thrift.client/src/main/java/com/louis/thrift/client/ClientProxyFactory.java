package com.louis.thrift.client;

import com.louis.thrift.client.props.ThriftClientProperties;
import com.louis.thrift.provider.ServerExposeProvider;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Proxy;

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
public class ClientProxyFactory implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(ClientProxyFactory.class);

    private final ThriftClientProperties thriftClientProperties;

    private Class<?> objectClass;

    private ServerExposeProvider serverExposeProvider;

    private GenericObjectPool<TServiceClient> pool;

    private Object proxy;

    private ClientProxyFactory(ThriftClientProperties thriftClientProperties){
        this.thriftClientProperties = thriftClientProperties;
    }

    /**
     * create a client proxy
     * @param thriftClientProperties
     * @return
     */
    public static ClientProxyFactory create(ThriftClientProperties thriftClientProperties){
        return new ClientProxyFactory(thriftClientProperties);
    }

    public ClientProxyFactory serverExposeProvider(ServerExposeProvider provider){
        this.serverExposeProvider = provider;
        return this;
    }

    /**
     *
     * @return
     */
    public ClientProxyFactory configure() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        //load Iface interface
        objectClass = classLoader.loadClass(serverExposeProvider.getService() + "$Iface");
        //load Client.Factory
        Class<TServiceClientFactory<TServiceClient>> clientFactoryClass = (Class<TServiceClientFactory<TServiceClient>>) classLoader.loadClass(serverExposeProvider.getService() + "$Client$Factory");
        TServiceClientFactory<TServiceClient> clientFactory = clientFactoryClass.newInstance();
        ThriftClientPoolFactory thriftClientPoolFactory = new ThriftClientPoolFactory(serverExposeProvider, clientFactory);
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(thriftClientProperties.getMaxActive());
        genericObjectPoolConfig.setMinIdle(thriftClientProperties.getMinIdle());
        genericObjectPoolConfig.setMaxIdle(thriftClientProperties.getMaxIdle());
        genericObjectPoolConfig.setMinEvictableIdleTimeMillis(thriftClientProperties.getIdleTime());
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(thriftClientProperties.getIdleTime() * 2L);
        pool = new GenericObjectPool<TServiceClient>(thriftClientPoolFactory, genericObjectPoolConfig);
        proxy = Proxy.newProxyInstance(classLoader, new Class[]{objectClass}, (proxy, method, args) -> {
            TServiceClient client = pool.borrowObject();
//            Object obj = method.invoke(client, args);
//            pool.returnObject(client);
//            return obj;
            boolean flag = true;
            try{
                return method.invoke(client, args);
            }catch (Exception e){
                flag = false;
                logger.error("invoke failed : {}", e);
                throw e;
            }finally {
                if(flag){
                    pool.returnObject(client);
                }else{
                    pool.invalidateObject(client);
                }
            }
        });

        //add shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (IOException e) {
                //e.printStackTrace();
            }
        }));
        return this;
    }

    public Object getProxy() {
        return proxy;
    }

    @Override
    public void close() throws IOException {
        if(pool != null){
            pool.close();
        }
        if(serverExposeProvider != null){
            serverExposeProvider.close();
        }
    }
}
