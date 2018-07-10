package com.louis.thrift.zk;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.IOException;

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
public class DefaultCuratorFactory implements CuratorFactory, Closeable{

    private final Logger logger = LoggerFactory.getLogger(DefaultCuratorFactory.class);

    /**
     * client of zk
     */
    private CuratorFramework client;

    private NodeCache nodeCache;

    private PathChildrenCache pathChildrenCache;

    private DefaultCuratorFactory(ZkProperties zkProperties){
        Assert.notNull(zkProperties.getUri(), "the connect string of zookeeper must not be null");
        client = CuratorFrameworkFactory.builder().connectString(zkProperties.getUri())
                .sessionTimeoutMs(zkProperties.getSessionTimeOut())
                .retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 100))
                .connectionTimeoutMs(zkProperties.getTimeOut()).build();
        //start to connect
        client.start();
    }


    @Override
    public CuratorFramework client() {
        return this.client;
    }

    @Override
    public NodeCache getNodeCache(String path) {
        Preconditions.checkArgument(client != null && client.getState() == CuratorFrameworkState.STARTED,
                "Curator client is not started yet");
       nodeCache = new NodeCache(client, path);
        try {
            //sync zk node into path cache
            nodeCache.start(true);
        } catch (Exception e) {
            logger.error("error on trying to cache nodes : " + e.getMessage());
            throw new IllegalStateException(e.getMessage(), e);
        }
        return nodeCache;
    }

    @Override
    public PathChildrenCache getPathChildrenCache(String path, PathChildrenCache.StartMode startMode) {
        Preconditions.checkArgument(client != null && client.getState() == CuratorFrameworkState.STARTED,
                "Curator client is not started yet");
        pathChildrenCache = new PathChildrenCache(client, path, true);
        try {
            pathChildrenCache.start(startMode);
        } catch (Exception e) {
            logger.error("error on trying to cache nodes : " + e.getMessage());
            throw new IllegalStateException(e.getMessage(), e);
        }
        return pathChildrenCache;
    }


    @Override
    public void shutdown() throws IOException {
        this.close();
    }


    public static DefaultCuratorFactory build(ZkProperties zkProperties){
        return new DefaultCuratorFactory(zkProperties);
    }

    @Override
    public void close() throws IOException {
        if(pathChildrenCache != null){
            pathChildrenCache.close();
        }
        if(nodeCache != null){
            nodeCache.close();
        }
        if(client != null){
            client.close();
        }
    }
}
