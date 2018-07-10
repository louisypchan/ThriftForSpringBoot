package com.louis.thrift.client.props;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

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
@ConfigurationProperties(prefix = "thrift")
public class ThriftServerProperties {


    public static TProtocolFactory[] PROFACT = new TProtocolFactory[]{new TCompactProtocol.Factory(), new TBinaryProtocol.Factory(),
            new TJSONProtocol.Factory()};

    private int selectorCount = 2;

    /**
     *  0 presents to use Executors.newCachedThreadPool()
     */
    private int workerCount = 0;

    /**
     * 0 - TCompactProtocol default
     * 1 - TBinaryProtocol
     * 2 - TJSONProtocol
     */
    private int protocol = 0;

    /**
     * The size of the blocking queue per selector thread for passing accepted
     * connections to the selector thread
     */
    private int acceptQueueSizePerThread = 4;


    public int getSelectorCount() {
        return selectorCount;
    }

    public void setSelectorCount(int selectorCount) {
        this.selectorCount = selectorCount;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public int getProtocol() {
        return protocol;
    }

    public void setProtocol(int protocol) {
        this.protocol = protocol;
    }

    public int getAcceptQueueSizePerThread() {
        return acceptQueueSizePerThread;
    }

    public void setAcceptQueueSizePerThread(int acceptQueueSizePerThread) {
        this.acceptQueueSizePerThread = acceptQueueSizePerThread;
    }
}
