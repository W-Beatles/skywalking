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

package org.apache.skywalking.apm.agent.core.remote;

import io.grpc.Channel;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.commands.CommandService;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.os.OSUtil;
import org.apache.skywalking.apm.network.common.v3.Commands;
import org.apache.skywalking.apm.network.common.v3.KeyStringValuePair;
import org.apache.skywalking.apm.network.management.v3.InstancePingPkg;
import org.apache.skywalking.apm.network.management.v3.InstanceProperties;
import org.apache.skywalking.apm.network.management.v3.ManagementServiceGrpc;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.GRPC_UPSTREAM_TIMEOUT;

/**
 * agent客户端管理服务，定时上传agent信息。默认心跳时间30s
 */
@DefaultImplementor
public class ServiceManagementClient implements BootService, Runnable, GRPCChannelListener {
    private static final ILog LOGGER = LogManager.getLogger(ServiceManagementClient.class);
    private static List<KeyStringValuePair> SERVICE_INSTANCE_PROPERTIES;

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;
    private volatile ManagementServiceGrpc.ManagementServiceBlockingStub managementServiceBlockingStub;
    private volatile ScheduledFuture<?> heartbeatFuture;
    private volatile AtomicInteger sendPropertiesCounter = new AtomicInteger(0);

    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            managementServiceBlockingStub = ManagementServiceGrpc.newBlockingStub(channel);
        } else {
            managementServiceBlockingStub = null;
        }
        this.status = status;
    }

    @Override
    public void prepare() {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);

        SERVICE_INSTANCE_PROPERTIES = new ArrayList<>();

        // 读取附带的服务实例属性
        for (String key : Config.Agent.INSTANCE_PROPERTIES.keySet()) {
            SERVICE_INSTANCE_PROPERTIES.add(KeyStringValuePair.newBuilder()
                    .setKey(key)
                    .setValue(Config.Agent.INSTANCE_PROPERTIES.get(key))
                    .build());
        }

        Config.Agent.INSTANCE_NAME = StringUtil.isEmpty(Config.Agent.INSTANCE_NAME)
                ? UUID.randomUUID().toString().replaceAll("-", "") + "@" + OSUtil.getIPV4()
                : Config.Agent.INSTANCE_NAME;
    }

    @Override
    public void boot() {
        // 默认心跳周期30s
        heartbeatFuture = Executors.newSingleThreadScheduledExecutor(
                new DefaultNamedThreadFactory("ServiceManagementClient")
        ).scheduleAtFixedRate(
                new RunnableWithExceptionProtection(
                        this,
                        t -> LOGGER.error("unexpected exception.", t)
                ), 0, Config.Collector.HEARTBEAT_PERIOD,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void shutdown() {
        heartbeatFuture.cancel(true);
    }

    @Override
    public void run() {
        LOGGER.debug("ServiceManagementClient running, status:{}.", status);

        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            try {
                if (managementServiceBlockingStub != null) {
                    // 默认每5分钟上传一次agent属性
                    // collector.heartbeat_period(心跳30s) * collector.properties_report_period_factor(周期因子10) = 5分钟
                    if (Math.abs(sendPropertiesCounter.getAndAdd(1)) % Config.Collector.PROPERTIES_REPORT_PERIOD_FACTOR == 0) {

                        managementServiceBlockingStub
                                .withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS)
                                .reportInstanceProperties(InstanceProperties.newBuilder()
                                        .setService(Config.Agent.SERVICE_NAME)
                                        .setServiceInstance(Config.Agent.INSTANCE_NAME)
                                        .addAllProperties(OSUtil.buildOSInfo(
                                                Config.OsInfo.IPV4_LIST_SIZE))
                                        .addAllProperties(SERVICE_INSTANCE_PROPERTIES)
                                        .build());
                    } else {
                        // 每30秒维持一次心跳
                        final Commands commands = managementServiceBlockingStub.withDeadlineAfter(
                                GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                        ).keepAlive(InstancePingPkg.newBuilder()
                                .setService(Config.Agent.SERVICE_NAME)
                                .setServiceInstance(Config.Agent.INSTANCE_NAME)
                                .build());

                        ServiceManager.INSTANCE.findService(CommandService.class).receiveCommand(commands);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error(t, "ServiceManagementClient execute fail.");
                ServiceManager.INSTANCE.findService(GRPCChannelManager.class).reportError(t);
            }
        }
    }
}
