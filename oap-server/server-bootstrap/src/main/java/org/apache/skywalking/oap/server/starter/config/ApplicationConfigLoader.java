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

package org.apache.skywalking.oap.server.starter.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.util.PropertyPlaceholderHelper;
import org.apache.skywalking.oap.server.library.module.ApplicationConfiguration;
import org.apache.skywalking.oap.server.library.module.ProviderNotFoundException;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.ResourceUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Initialize collector settings with following sources. Use application.yml as primary setting, and fix missing setting
 * by default settings in application-default.yml.
 * <p>
 * At last, override setting by system.properties and system.envs if the key matches moduleName.provideName.settingKey.
 */
@Slf4j
public class ApplicationConfigLoader implements ConfigLoader<ApplicationConfiguration> {
    private static final String DISABLE_SELECTOR = "-";
    private static final String SELECTOR = "selector";

    private final Yaml yaml = new Yaml();

    @Override
    public ApplicationConfiguration load() throws ConfigFileNotFoundException {
        ApplicationConfiguration configuration = new ApplicationConfiguration();
        // 读取 application.yml 配置
        this.loadConfig(configuration);
        // 系统配置优先级更高，覆盖文件中的配置 System.Properties(-D)
        this.overrideConfigBySystemEnv(configuration);
        return configuration;
    }

    @SuppressWarnings("unchecked")
    private void loadConfig(ApplicationConfiguration configuration) throws ConfigFileNotFoundException {
        try {
            // 读取resource目录下的application.yml配置
            Reader applicationReader = ResourceUtils.read("application.yml");
            // 多层嵌套的key-value形式
            Map<String, Map<String, Object>> moduleConfig = yaml.loadAs(applicationReader, Map.class);
            if (CollectionUtils.isNotEmpty(moduleConfig)) {
                // 遍历每一个模块配置，并根据select配置选择出有效的配置
                selectConfig(moduleConfig);
                // 遍历有效的模块配置
                moduleConfig.forEach((moduleName, providerConfig) -> {
                    if (providerConfig.size() > 0) {
                        log.info("Get a module define from application.yml, module name: {}", moduleName);
                        ApplicationConfiguration.ModuleConfiguration moduleConfiguration = configuration.addModule(
                                moduleName);
                        providerConfig.forEach((providerName, config) -> {
                            log.info(
                                    "Get a provider define belong to {} module, provider name: {}", moduleName,
                                    providerName
                            );
                            final Map<String, ?> propertiesConfig = (Map<String, ?>) config;
                            final Properties properties = new Properties();
                            if (propertiesConfig != null) {
                                propertiesConfig.forEach((propertyName, propertyValue) -> {
                                    // 转换map类型的配置
                                    if (propertyValue instanceof Map) {
                                        Properties subProperties = new Properties();
                                        ((Map) propertyValue).forEach((key, value) -> {
                                            subProperties.put(key, value);
                                            replacePropertyAndLog(key, value, subProperties, providerName);
                                        });
                                        properties.put(propertyName, subProperties);
                                    } else {
                                        properties.put(propertyName, propertyValue);
                                        replacePropertyAndLog(propertyName, propertyValue, properties, providerName);
                                    }
                                });
                            }
                            moduleConfiguration.addProviderConfiguration(providerName, properties);
                        });
                    } else {
                        log.warn(
                                "Get a module define from application.yml, but no provider define, use default, module name: {}",
                                moduleName
                        );
                    }
                });
            }
        } catch (FileNotFoundException e) {
            throw new ConfigFileNotFoundException(e.getMessage(), e);
        }
    }

    private void replacePropertyAndLog(final Object propertyName, final Object propertyValue, final Properties target,
                                       final Object providerName) {
        final String valueString = PropertyPlaceholderHelper.INSTANCE
                .replacePlaceholders(propertyValue + "", target);
        if (valueString != null) {
            if (valueString.trim().length() == 0) {
                target.replace(propertyName, valueString);
                log.info("Provider={} config={} has been set as an empty string", providerName, propertyName);
            } else {
                // Use YAML to do data type conversion.
                final Object replaceValue = yaml.load(valueString);
                if (replaceValue != null) {
                    target.replace(propertyName, replaceValue);
                    log.info(
                            "Provider={} config={} has been set as {}",
                            providerName,
                            propertyName,
                            replaceValue.toString()
                    );
                }
            }
        }
    }

    private void overrideConfigBySystemEnv(ApplicationConfiguration configuration) {
        for (Map.Entry<Object, Object> prop : System.getProperties().entrySet()) {
            overrideModuleSettings(configuration, prop.getKey().toString(), prop.getValue().toString());
        }
    }

    private void selectConfig(final Map<String, Map<String, Object>> moduleConfiguration) {
        final Set<String> modulesWithoutProvider = new HashSet<>();
        // 遍历每一个模块
        for (final Map.Entry<String, Map<String, Object>> entry : moduleConfiguration.entrySet()) {
            final String moduleName = entry.getKey();
            final Map<String, Object> providerConfig = entry.getValue();
            // 如果该模块没有selector节点，将忽略该模块的配置
            if (!providerConfig.containsKey(SELECTOR)) {
                continue;
            }
            // 解析模块selector的值
            final String selector = (String) providerConfig.get(SELECTOR);
            final String resolvedSelector = PropertyPlaceholderHelper.INSTANCE.replacePlaceholders(
                    selector, System.getProperties()
            );
            // 将该模块未选择的配置全部移除
            providerConfig.entrySet().removeIf(e -> !resolvedSelector.equals(e.getKey()));

            if (!providerConfig.isEmpty()) {
                continue;
            }
            // 未找到指定的选择器，则提示报错不存在该模块
            if (!DISABLE_SELECTOR.equals(resolvedSelector)) {
                throw new ProviderNotFoundException(
                        "no provider found for module " + moduleName + ", " +
                                "if you're sure it's not required module and want to remove it, " +
                                "set the selector to -"
                );
            }
            // 选择器值为-，则直接移除该模块 selector: ${SW_RECEIVER_ZABBIX:-}
            // now the module can be safely removed
            modulesWithoutProvider.add(moduleName);
        }

        // 这里真正移除模块。因为要删除多个元素，采用统一删除的方式时间降低到2n
        moduleConfiguration.entrySet().removeIf(e -> {
            final String module = e.getKey();
            final boolean shouldBeRemoved = modulesWithoutProvider.contains(module);

            if (shouldBeRemoved) {
                log.info("Remove module {} without any provider", module);
            }

            return shouldBeRemoved;
        });
    }

    private void overrideModuleSettings(ApplicationConfiguration configuration, String key, String value) {
        int moduleAndConfigSeparator = key.indexOf('.');
        if (moduleAndConfigSeparator <= 0) {
            return;
        }
        String moduleName = key.substring(0, moduleAndConfigSeparator);
        String providerSettingSubKey = key.substring(moduleAndConfigSeparator + 1);
        ApplicationConfiguration.ModuleConfiguration moduleConfiguration = configuration.getModuleConfiguration(
                moduleName);
        if (moduleConfiguration == null) {
            return;
        }
        int providerAndConfigSeparator = providerSettingSubKey.indexOf('.');
        if (providerAndConfigSeparator <= 0) {
            return;
        }
        String providerName = providerSettingSubKey.substring(0, providerAndConfigSeparator);
        String settingKey = providerSettingSubKey.substring(providerAndConfigSeparator + 1);
        if (!moduleConfiguration.has(providerName)) {
            return;
        }
        Properties providerSettings = moduleConfiguration.getProviderConfiguration(providerName);
        if (!providerSettings.containsKey(settingKey)) {
            return;
        }
        Object originValue = providerSettings.get(settingKey);
        Class<?> type = originValue.getClass();
        if (type.equals(int.class) || type.equals(Integer.class))
            providerSettings.put(settingKey, Integer.valueOf(value));
        else if (type.equals(String.class))
            providerSettings.put(settingKey, value);
        else if (type.equals(long.class) || type.equals(Long.class))
            providerSettings.put(settingKey, Long.valueOf(value));
        else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            providerSettings.put(settingKey, Boolean.valueOf(value));
        } else {
            return;
        }

        log.info(
                "The setting has been override by key: {}, value: {}, in {} provider of {} module through {}", settingKey,
                value, providerName, moduleName, "System.properties"
        );
    }
}
