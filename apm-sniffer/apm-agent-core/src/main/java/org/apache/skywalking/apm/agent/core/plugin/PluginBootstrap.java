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

package org.apache.skywalking.apm.agent.core.plugin;

import org.apache.skywalking.apm.agent.core.boot.AgentPackageNotFoundException;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.loader.AgentClassLoader;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Plugins finder. Use {@link PluginResourcesResolver} to find all plugins, and ask {@link PluginCfg} to load all plugin
 * definitions.
 */
public class PluginBootstrap {
    private static final ILog LOGGER = LogManager.getLogger(PluginBootstrap.class);

    /**
     * load all plugins.
     *
     * @return plugin definition list.
     */
    public List<AbstractClassEnhancePluginDefine> loadPlugins() throws AgentPackageNotFoundException {
        // 初始化默认的类加载器
        AgentClassLoader.initDefaultLoader();

        // 使用 PluginResourcesResolver 去查找 AgentClassLoader 加载的所有插件描述文件 skywalking-plugin.def 的路径
        PluginResourcesResolver resolver = new PluginResourcesResolver();
        List<URL> resources = resolver.getResources();

        if (resources == null || resources.isEmpty()) {
            LOGGER.info("no plugin files (skywalking-plugin.def) found, continue to start application.");
            return new ArrayList<>();
        }

        // 遍历插件描述文件的每一行，并构造成插件类信息的列表 List<PluginDefine>
        for (URL pluginUrl : resources) {
            try {
                PluginCfg.INSTANCE.load(pluginUrl.openStream());
            } catch (Exception e) {
                LOGGER.error(e, "plugin file [{}] init failure.", pluginUrl);
            }
        }

        // PluginDefine中记录了插件名称、插件类全类名。如：
        //   name        :  activemq-5.x
        //   defineClass :  org.apache.skywalking.apm.plugin.activemq.define.ActiveMQProducerInstrumentation
        List<PluginDefine> pluginClassList = PluginCfg.INSTANCE.getPluginClassList();

        List<AbstractClassEnhancePluginDefine> plugins = new ArrayList<>();
        for (PluginDefine pluginDefine : pluginClassList) {
            try {
                LOGGER.debug("loading plugin class {}.", pluginDefine.getDefineClass());
                // 通过反射构造插件对象
                AbstractClassEnhancePluginDefine plugin = (AbstractClassEnhancePluginDefine) Class.forName(pluginDefine.getDefineClass(), true, AgentClassLoader
                        .getDefault()).getDeclaredConstructor().newInstance();
                plugins.add(plugin);
            } catch (Exception e) {
                LOGGER.error(e, "load plugin [{}] failure.", pluginDefine.getDefineClass());
            }
        }

        // 支持apm-customize-enhance-plugin插件，只需要添加xml描述文件即可定义tag、log增强点
        // 更多参见: /skywalking/docs/en/setup/service-agent/java-agent/Customize-enhance-trace.md
        plugins.addAll(DynamicPluginLoader.INSTANCE.load(AgentClassLoader.getDefault()));
        return plugins;
    }
}
