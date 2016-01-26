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


import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ResultUtils {

    private final Charset UTF_8 = Charset.forName("UTF-8");
    public final static int FRAME_SIZE = 32768;
    public static FrameManager resultDisplayFrameMgr = new FrameManager(FRAME_SIZE);
    private IFrameTupleAccessor fta;
    private IFrame frame;

    public ResultUtils() throws HyracksDataException
    {
        fta = new ResultFrameTupleAccessor();
        frame = new VSizeFrame(resultDisplayFrameMgr);
    }

    public List<String> displayResults(AsterixResultReader reader) throws HyracksDataException{
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        List<String> resultList = new ArrayList<>();

            do {
                try {
                    fta.reset(frame.getBuffer());
                    int last = fta.getTupleCount();
                    String result;
                    for (int tIndex = 0; tIndex < last; tIndex++) {
                        int start = fta.getTupleStartOffset(tIndex);
                        int length = fta.getTupleEndOffset(tIndex) - start;
                        if(length<0)
                            System.out.println("negative");
                        bbis.setByteBuffer(frame.getBuffer(), start);
                        byte[] recordBytes = new byte[length];
                        int numread = bbis.read(recordBytes, 0, length);

                        result = new String(recordBytes, 0, numread, UTF_8);
                        resultList.add(result);
                    }
                    frame.getBuffer().clear();
                } finally {
                    try {
                        bbis.close();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            } while (reader.read(frame) > 0);

        return resultList;
    }
}
