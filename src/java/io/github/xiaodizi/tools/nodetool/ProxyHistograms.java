/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xiaodizi.tools.nodetool;

import java.io.PrintStream;

import static java.lang.String.format;
import io.airlift.airline.Command;

import io.github.xiaodizi.tools.NodeProbe;
import io.github.xiaodizi.tools.NodeTool.NodeToolCmd;

@Command(name = "proxyhistograms", description = "Print statistic histograms for network operations")
public class ProxyHistograms extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        String[] percentiles = {"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
        Double[] readLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Read"));
        Double[] writeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Write"));
        Double[] rangeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("RangeSlice"));
        Double[] casReadLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("CASRead"));
        Double[] casWriteLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("CASWrite"));
        Double[] viewWriteLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("ViewWrite"));

        out.println("proxy histograms");
        out.println(format("%-10s%19s%19s%19s%19s%19s%19s",
                "Percentile", "Read Latency", "Write Latency", "Range Latency", "CAS Read Latency", "CAS Write Latency", "View Write Latency"));
        out.println(format("%-10s%19s%19s%19s%19s%19s%19s",
                "", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)"));
        for (int i = 0; i < percentiles.length; i++)
        {
            out.println(format("%-10s%19.2f%19.2f%19.2f%19.2f%19.2f%19.2f",
                    percentiles[i],
                    readLatency[i],
                    writeLatency[i],
                    rangeLatency[i],
                    casReadLatency[i],
                    casWriteLatency[i],
                    viewWriteLatency[i]));
        }
        out.println();
    }
}
