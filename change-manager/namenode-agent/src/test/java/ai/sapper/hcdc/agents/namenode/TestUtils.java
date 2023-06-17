/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.namenode;

import lombok.NonNull;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import java.io.File;

public class TestUtils {

    public static XMLConfiguration readFile(@NonNull String configFile) throws Exception {
        File cf = new File(configFile);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        return configs.xml(cf);
    }
}
