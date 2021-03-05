/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.jvm;

import io.grpc.Channel;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.commands.CommandService;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelListener;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelManager;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelStatus;
import org.apache.skywalking.apm.network.common.v3.Commands;
import org.apache.skywalking.apm.network.language.agent.v3.JVMMetric;
import org.apache.skywalking.apm.network.language.agent.v3.JVMMetricCollection;
import org.apache.skywalking.apm.network.language.agent.v3.JVMMetricReportServiceGrpc;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.GRPC_UPSTREAM_TIMEOUT;

/**
 * 指标信息发送服务
 */
@DefaultImplementor
public class JVMMetricsSender implements BootService, Runnable, GRPCChannelListener {
    private static final ILog LOGGER = LogManager.getLogger(JVMMetricsSender.class);

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;
    private volatile JVMMetricReportServiceGrpc.JVMMetricReportServiceBlockingStub stub = null;

    private LinkedBlockingQueue<JVMMetric> queue;

    @Override
    public void prepare() {
        queue = new LinkedBlockingQueue<>(Config.Jvm.BUFFER_SIZE);
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);
    }

    @Override
    public void boot() {

    }

    public void offer(JVMMetric metric) {
        // drop last message and re-deliver
        // TODO: 2021/2/27 为什么要放进去，拿出来，放进去？
        if (!queue.offer(metric)) {
            queue.poll();
            queue.offer(metric);
        }
    }

    @Override
    public void run() {
        if (status == GRPCChannelStatus.CONNECTED) {
            try {
                JVMMetricCollection.Builder builder = JVMMetricCollection.newBuilder();
                LinkedList<JVMMetric> buffer = new LinkedList<>();
                // 一次性从BlockingQueue获取所有可用的数据对象(还可以指定获取数据的个数)
                // 通过该方法，可以提升获取数据效率；不需要多次分批加锁或释放锁。
                queue.drainTo(buffer);
                if (!buffer.isEmpty()) {
                    builder.addAllMetrics(buffer);
                    builder.setService(Config.Agent.SERVICE_NAME);
                    builder.setServiceInstance(Config.Agent.INSTANCE_NAME);
                    Commands commands = stub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS)
                            .collect(builder.build());
                    // 上报指标信息
                    ServiceManager.INSTANCE.findService(CommandService.class).receiveCommand(commands);
                }
            } catch (Exception e) {
                LOGGER.error(e, "send JVM metrics to Collector fail.");
            }
        }
    }

    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            stub = JVMMetricReportServiceGrpc.newBlockingStub(channel);
        }
        this.status = status;
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {

    }
}
