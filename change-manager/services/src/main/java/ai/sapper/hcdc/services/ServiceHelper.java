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

package ai.sapper.hcdc.services;

import ai.sapper.cdc.core.NameNodeEnv;
import lombok.NonNull;

public class ServiceHelper {
    public static void checkService(@NonNull String name, Object processor) throws Exception {
        if (processor == null) {
            throw new Exception("Service not initialized...");
        } else if (!NameNodeEnv.get(name).state().isAvailable()) {
            throw new Exception(String.format("Service environment not available. [state=%s]",
                    NameNodeEnv.get(name).state().getState().name()));
        }
    }
}
