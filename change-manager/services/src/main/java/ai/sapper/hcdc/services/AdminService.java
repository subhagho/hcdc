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

import ai.sapper.cdc.common.model.services.BasicResponse;
import ai.sapper.cdc.common.model.services.EResponseState;
import ai.sapper.cdc.core.NameNodeEnv;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AdminService {
    @RequestMapping(value = "/namenode/terminate", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<Integer>> terminate() {
        try {
            int count = NameNodeEnv.disposeAll();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    count),
                    HttpStatus.OK);
        } catch (Throwable r) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    -1).withError(r),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
