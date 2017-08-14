/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.connector.result;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.dataset.IDatasetInputChannelMonitor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Channel monitor to inform the client about the result status while reading.
 */
public class DatasetInputChannelMonitor implements IDatasetInputChannelMonitor {
    private final AtomicInteger nAvailableFrames;

    private final AtomicBoolean eos;

    private final AtomicBoolean failed;

    public DatasetInputChannelMonitor() {
        nAvailableFrames = new AtomicInteger(0);
        eos = new AtomicBoolean(false);
        failed = new AtomicBoolean(false);
    }

    @Override
    public synchronized void notifyFailure(IInputChannel channel) {
        failed.set(true);
        notifyAll();
    }

    @Override
    public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
        nAvailableFrames.addAndGet(nFrames);
        notifyAll();
    }

    @Override
    public synchronized void notifyEndOfStream(IInputChannel channel) {
        eos.set(true);
        notifyAll();
    }

    @Override
    public synchronized boolean eosReached() {
        return eos.get();
    }

    @Override
    public synchronized boolean failed() {
        return failed.get();
    }

    @Override
    public synchronized int getNFramesAvailable() {
        return nAvailableFrames.get();
    }

    @Override
    public synchronized void notifyFrameRead() {
        nAvailableFrames.decrementAndGet();
    }

}
