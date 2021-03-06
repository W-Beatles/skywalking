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

package org.apache.skywalking.apm.plugin.rabbitmq.define;

import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.ConstructorInterceptPoint;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.InstanceMethodsInterceptPoint;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.ClassInstanceMethodsEnhancePluginDefine;
import org.apache.skywalking.apm.agent.core.plugin.match.ClassMatch;
import org.apache.skywalking.apm.agent.core.plugin.match.MultiClassNameMatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.skywalking.apm.agent.core.plugin.bytebuddy.ArgumentTypeNameMatch.takesArgumentWithType;

/**
 * 插件定义
 * 继承 ClassInstanceMethodsEnhancePluginDefine 拦截示例方法
 * 继承 ClassStaticMethodsEnhancePluginDefine  拦截静态方法
 */
public class RabbitMQProducerInstrumentation extends ClassInstanceMethodsEnhancePluginDefine {
    public static final String INTERCEPTOR_CLASS = "org.apache.skywalking.apm.plugin.rabbitmq.RabbitMQProducerInterceptor";
    public static final String ENHANCE_CLASS_PRODUCER = "com.rabbitmq.client.impl.ChannelN";
    public static final String ENHANCE_METHOD_DISPATCH = "basicPublish";
    public static final String INTERCEPTOR_CONSTRUCTOR = "org.apache.skywalking.apm.plugin.rabbitmq.RabbitMQProducerAndConsumerConstructorInterceptor";

    /**
     * 指定要拦截的构造方法
     */
    @Override
    public ConstructorInterceptPoint[] getConstructorsInterceptPoints() {
        return new ConstructorInterceptPoint[]{
                new ConstructorInterceptPoint() {

                    /**
                     * 匹配指定的构造方法
                     */
                    @Override
                    public ElementMatcher<MethodDescription> getConstructorMatcher() {
                        return takesArgumentWithType(3, "com.rabbitmq.client.MetricsCollector");
                    }

                    /**
                     * 返回要使用的拦截器全类名
                     */
                    @Override
                    public String getConstructorInterceptor() {
                        return INTERCEPTOR_CONSTRUCTOR;
                    }
                }
        };
    }

    /**
     * 指定要拦截的实例方法
     */
    @Override
    public InstanceMethodsInterceptPoint[] getInstanceMethodsInterceptPoints() {
        return new InstanceMethodsInterceptPoint[]{
                // 为每一个要拦截的实例方法创建一个切点
                new InstanceMethodsInterceptPoint() {

                    /**
                     * 匹配要拦截的方法
                     */
                    @Override
                    public ElementMatcher<MethodDescription> getMethodsMatcher() {
                        return named(ENHANCE_METHOD_DISPATCH).and(takesArgumentWithType(4, "com.rabbitmq.client.AMQP$BasicProperties"));
                    }

                    /**
                     * 返回要使用的拦截器全类名
                     */
                    @Override
                    public String getMethodsInterceptor() {
                        return INTERCEPTOR_CLASS;
                    }

                    /**
                     * 覆盖方法入参
                     */
                    @Override
                    public boolean isOverrideArgs() {
                        return true;
                    }
                }
        };
    }

    @Override
    protected ClassMatch enhanceClass() {
        return MultiClassNameMatch.byMultiClassMatch(ENHANCE_CLASS_PRODUCER);
    }
}
