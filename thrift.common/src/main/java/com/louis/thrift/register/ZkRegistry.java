package com.louis.thrift.register;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.louis.thrift.zk.CuratorFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
public class ZkRegistry implements Registry, Closeable {

    private final Logger logger = LoggerFactory.getLogger(ZkRegistry.class);

    private CuratorFactory curatorFactory = null;

    private NodeCacheManager nodeCacheManager = new NodeCacheManager();

    private final ConcurrentMap<String, NodeCache> zNodeMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, PathChildrenCache> childNodeMap = Maps
            .newConcurrentMap();

    private static final Set<Type> IgnoreTypes = ImmutableSet.of(
            Type.CONNECTION_SUSPENDED, Type.CONNECTION_RECONNECTED,
            Type.CONNECTION_LOST, Type.INITIALIZED);

    private ZkRegistry(){

    }

    public static ZkRegistry build(){
        return new ZkRegistry();
    }

    public ZkRegistry curatorFactory(CuratorFactory curatorFactory){
        this.curatorFactory = curatorFactory;
        //in case zk hasn't started yet
        if(this.curatorFactory.client().getState() == CuratorFrameworkState.LATENT){
            this.curatorFactory.client().start();
        }
        return this;
    }

    @Override
    public void register(String path, byte[] data) {
        Assert.notNull(curatorFactory, "curatorFactory must not be null");
        try {
            curatorFactory.client().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(path, data);
        }catch (KeeperException.NodeExistsException ex){
            logger.warn(ex.getMessage());
        }
        catch (Exception e) {
            logger.error("create node " + path + " failed : " + e.getMessage());
        }
    }

    @Override
    public void unregister(String path) {
        Assert.notNull(curatorFactory, "curatorFactory must not be null");
        try {
            curatorFactory.client().delete().guaranteed().forPath(path);
        } catch (Exception e) {
            logger.error("delete node " + path + " failed : " + e.getMessage());
        }
    }

    @Override
    public void watch(String path, MonitorListener listener) {
        nodeCacheManager.subscribe(path, PathChildrenCache.StartMode.BUILD_INITIAL_CACHE, listener);
    }

    @Override
    public void subscribe(String path, PathChildrenCache.StartMode startMode, MonitorListener listener) {
        nodeCacheManager.subscribe(path, startMode, listener);
    }

    @Override
    public void unSubscribe(String path, MonitorListener listener) {
        nodeCacheManager.unSubscribe(path, listener);
    }


    @Override
    public void shutdown() throws IOException{
        this.close();
    }

    @Override
    public void close() throws IOException {
        if(curatorFactory != null){
            curatorFactory.shutdown();
        }
    }

    public CuratorFactory getCuratorFactory() {
        return curatorFactory;
    }

    /**
     * To watch the changes both on nodeCache and childrenNodeCache
     * So that we don't need to care about disconnect or session invalid
     */
    private class NodeCacheManager{
        /**
         * map to cache
         */
        private ConcurrentMap<MonitorListener, NodeCacheListener> zNodeListenerMap = Maps.newConcurrentMap();

        private ConcurrentMap<MonitorListener, PathChildrenCacheListener> childrenListenerMap = Maps.newConcurrentMap();

        public void subscribe(String path, PathChildrenCache.StartMode startMode, final MonitorListener listener){
            watchNodeCache(path, listener);
            watchPathChildrenCache(path, startMode, listener);
        }

        public void unSubscribe(String path, final MonitorListener listener){
            NodeCache nodeCache = zNodeMap.get(path);
            PathChildrenCache pathChildrenCache = childNodeMap.get(path);
            if(nodeCache != null){
                nodeCache.getListenable().removeListener(zNodeListenerMap.get(listener));
            }
            if(pathChildrenCache != null){
                pathChildrenCache.getListenable().removeListener(childrenListenerMap.get(listener));
            }
        }

        /**
         * watch node cache
         * @param path
         * @param listener
         */
        private void watchNodeCache(String path, final MonitorListener listener){
            NodeCache nodeCache = zNodeMap.get(path);
            if(nodeCache == null){
                zNodeMap.putIfAbsent(path, curatorFactory.getNodeCache(path));
                nodeCache = zNodeMap.get(path);
            }
            final NodeCache nodeCacheTemporary = nodeCache;
            zNodeListenerMap.putIfAbsent(listener, () -> {
                logger.info("node " + path + "has been changed");
                ChildData childData = nodeCacheTemporary.getCurrentData();
                ImmutablePair<String, byte[]> pair = null;
                if(childData != null){
                    pair = ImmutablePair.of(childData.getPath(), childData.getData());
                }
                listener.changed(new MonitorEvent(MonitorEvent.EventType.NODE_CHANGED, pair));
            });
            //add listener
            nodeCache.getListenable().addListener(zNodeListenerMap.get(listener));
        }

        /**
         * watch patch children node cache
         * @param path
         * @param listener
         */
        private void watchPathChildrenCache(String path, PathChildrenCache.StartMode startMode, final MonitorListener listener){
            PathChildrenCache pathChildrenCache = childNodeMap.get(path);
            if(pathChildrenCache == null){
                childNodeMap.putIfAbsent(path, curatorFactory.getPathChildrenCache(path, startMode));
                pathChildrenCache = childNodeMap.get(path);
            }
            childrenListenerMap.putIfAbsent(listener, (client, event) -> {
                if(IgnoreTypes.contains(event.getType())) return;
                ImmutablePair<String, byte[]> pair = ImmutablePair.of(event.getData().getPath(), event.getData().getData());
                MonitorEvent.EventType eventType = null;
                switch (event.getType()){
                    case CHILD_ADDED:
                        eventType = MonitorEvent.EventType.CHILD_ADDED;
                        break;
                    case CHILD_UPDATED:
                        eventType = MonitorEvent.EventType.CHILD_UPDATED;
                        break;
                    case CHILD_REMOVED:
                        eventType = MonitorEvent.EventType.CHILD_REMOVED;
                        break;
                    default:
                        break;
                }
                //
                listener.changed(new MonitorEvent(eventType, pair));
            });
            pathChildrenCache.getListenable().addListener(childrenListenerMap.get(listener));
        }

    }
}
