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
package io.github.xiaodizi.distributed.upgrade;

import com.vdurmont.semver4j.Semver;
import io.github.xiaodizi.config.Config;
import io.github.xiaodizi.distributed.UpgradeableCluster;
import io.github.xiaodizi.distributed.api.ICluster;
import io.github.xiaodizi.distributed.api.IInvokableInstance;
import io.github.xiaodizi.distributed.impl.AbstractCluster;
import io.github.xiaodizi.distributed.shared.Versions;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.util.Arrays;

import static io.github.xiaodizi.config.ConfigCompatabilityTest.*;

/**
 * This class is to generate YAML dumps per version, this is a manual process and should be updated for each release.
 */
public class ConfigCompatabilityTestGenerate
{
    public static void main(String[] args) throws Throwable
    {
        ICluster.setup();
        Versions versions = Versions.find();
        for (Semver version : Arrays.asList(UpgradeTestBase.v30, UpgradeTestBase.v3X, UpgradeTestBase.v40))
        {
            File path = new File(TEST_DIR, "version=" + version + ".yml");
            path.getParentFile().mkdirs();
            Versions.Version latest = versions.getLatest(version);
            // this class isn't present so the lambda can't be deserialized... so add to the classpath
            latest = new Versions.Version(latest.version, ArrayUtils.addAll(latest.classpath, AbstractCluster.CURRENT_VERSION.classpath));

            try (UpgradeableCluster cluster = UpgradeableCluster.create(1, latest))
            {
                IInvokableInstance inst = (IInvokableInstance) cluster.get(1);
                Class<?> klass = inst.callOnInstance(() -> Config.class);
                assert klass.getClassLoader() != ConfigCompatabilityTestGenerate.class.getClassLoader();
                dump(toTree(klass), path.getAbsolutePath());
            }
        }
    }
}
