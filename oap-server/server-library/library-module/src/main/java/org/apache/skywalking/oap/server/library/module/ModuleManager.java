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

package org.apache.skywalking.oap.server.library.module;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The <code>ModuleManager</code> takes charge of all {@link ModuleDefine}s in collector.
 */
public class ModuleManager implements ModuleDefineHolder {
    private boolean isInPrepareStage = true;
    private final Map<String, ModuleDefine> loadedModules = new HashMap<>();

    /**
     * Init the given modules
     */
    public void init(ApplicationConfiguration applicationConfiguration) throws ModuleNotFoundException, ProviderNotFoundException, ServiceNotProvidedException, CycleDependencyException, ModuleConfigException, ModuleStartException {
        // 基于SPI机制加载所有模块对象
        String[] moduleNames = applicationConfiguration.moduleList();
        // 加载的所有模块定义
        ServiceLoader<ModuleDefine> moduleServiceLoader = ServiceLoader.load(ModuleDefine.class);
        // 加载的所有模块实例
        ServiceLoader<ModuleProvider> moduleProviderLoader = ServiceLoader.load(ModuleProvider.class);

        HashSet<String> moduleSet = new HashSet<>(Arrays.asList(moduleNames));
        for (ModuleDefine module : moduleServiceLoader) {
            // 找到符合的待初始化的模块
            if (moduleSet.contains(module.name())) {
                // 初始化模块
                module.prepare(this, applicationConfiguration.getModuleConfiguration(module.name()), moduleProviderLoader);
                // 添加到初始化完成列表中
                loadedModules.put(module.name(), module);
                moduleSet.remove(module.name());
            }
        }
        // Finish prepare stage
        isInPrepareStage = false;

        if (moduleSet.size() > 0) {
            throw new ModuleNotFoundException(moduleSet.toString() + " missing.");
        }

        // 根据模块依赖调整模块顺序
        BootstrapFlow bootstrapFlow = new BootstrapFlow(loadedModules);

        // 调用模块的生命周期方法启动模块
        bootstrapFlow.start(this);
        // 调用模块的生声周期方法执行模块完成逻辑
        bootstrapFlow.notifyAfterCompleted();
    }

    @Override
    public boolean has(String moduleName) {
        return loadedModules.get(moduleName) != null;
    }

    @Override
    public ModuleProviderHolder find(String moduleName) throws ModuleNotFoundRuntimeException {
        assertPreparedStage();
        ModuleDefine module = loadedModules.get(moduleName);
        if (module != null)
            return module;
        throw new ModuleNotFoundRuntimeException(moduleName + " missing.");
    }

    private void assertPreparedStage() {
        if (isInPrepareStage) {
            throw new AssertionError("Still in preparing stage.");
        }
    }
}
