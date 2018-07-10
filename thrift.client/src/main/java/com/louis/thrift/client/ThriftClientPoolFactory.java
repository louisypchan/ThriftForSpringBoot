package com.louis.thrift.client;

import com.louis.thrift.provider.ServerExposeProvider;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

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
public class ThriftClientPoolFactory extends BasePooledObjectFactory<TServiceClient> {

    private final Logger logger = LoggerFactory.getLogger(ThriftClientPoolFactory.class);

    private final ServerExposeProvider serverExposeProvider;

    private final TServiceClientFactory<TServiceClient> clientFactory;

    private final PoolOperationCallBack callback;

    public ThriftClientPoolFactory(ServerExposeProvider serverExposeProvider, TServiceClientFactory<TServiceClient> clientFactory){
        this(serverExposeProvider, clientFactory, null);
    }

    public ThriftClientPoolFactory(ServerExposeProvider serverExposeProvider, TServiceClientFactory<TServiceClient> clientFactory, PoolOperationCallBack callback) {
        this.serverExposeProvider = serverExposeProvider;
        this.clientFactory = clientFactory;
        this.callback = callback;
    }

    @Override
    public TServiceClient create() throws Exception {
        InetSocketAddress address = serverExposeProvider.select();
        if(address == null){
            throw new Exception("No provider available");
        }
        TSocket tsocket = new TSocket(address.getHostName(), address.getPort());
        TTransport transport = new TFramedTransport(tsocket);
        TProtocol protocol = new TMultiplexedProtocol(new TCompactProtocol(transport), serverExposeProvider.getService());
        TServiceClient client = this.clientFactory.getClient(protocol);
        transport.open();
        if(callback != null){
            callback.create(client);
        }
        return client;
    }

    /**
     * close transport
     * @param p
     * @throws Exception
     */
    @Override
    public void destroyObject(PooledObject<TServiceClient> p) throws Exception {
        TServiceClient client = p.getObject();
        if(callback != null){
            callback.destory(client);
        }
        logger.info("destroyObject:{}", client);
        TTransport pin = client.getInputProtocol().getTransport();
        pin.close();
        TTransport pout = client.getOutputProtocol().getTransport();
        pout.close();
    }

    @Override
    public boolean validateObject(PooledObject<TServiceClient> p) {
        TServiceClient client = p.getObject();
        TTransport pin = client.getInputProtocol().getTransport();
        logger.info("validateObject input:{}", pin.isOpen());
        TTransport pout = client.getOutputProtocol().getTransport();
        logger.info("validateObject output:{}", pout.isOpen());
        return pin.isOpen() && pout.isOpen();
    }

    @Override
    public PooledObject<TServiceClient> wrap(TServiceClient tServiceClient) {
        return new DefaultPooledObject<>(tServiceClient);
    }


    public static interface PoolOperationCallBack {

        void  destory(TServiceClient client);

        void create(TServiceClient client);
    }
}
