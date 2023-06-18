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

package ai.sapper.hcdc.services.namenode;

import ai.sapper.cdc.common.model.services.BasicResponse;
import ai.sapper.cdc.common.model.services.ConfigSource;
import ai.sapper.cdc.common.model.services.EResponseState;
import ai.sapper.cdc.common.model.services.ReplicatorConfigSource;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.hcdc.agents.main.NameNodeReplicator;
import ai.sapper.hcdc.agents.main.SchemaScanner;
import com.google.common.base.Strings;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NameNodeServices {

    @RequestMapping(value = "/namenode/replicator/run", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<EHCdcProcessorState>> replicator(@RequestBody ReplicatorConfigSource config) {
        NameNodeReplicator replicator = new NameNodeReplicator();
        try {

            replicator.withFsImageDir(config.getFsImageDir())
                    .setConfigFile(config.getPath())
                    .setConfigSource(config.getType().name());
            if (!Strings.isNullOrEmpty(config.getTmpDir())) {
                replicator.withTmpDir(config.getTmpDir());
            }

            replicator.init();
            replicator.start();
            replicator.stop();
            DefaultLogger.info(replicator.getEnv().LOG,
                    String.format("Edits Delta processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    replicator.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    replicator.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/namenode/scanner/run", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<EHCdcProcessorState>> scanner(@RequestBody ConfigSource config) {
        SchemaScanner scanner = new SchemaScanner();
        try {
            scanner.setConfigFile(config.getPath())
                    .setConfigSource(config.getType().name());

            scanner.init();
            scanner.start();
            scanner.stop();
            DefaultLogger.info(scanner.getEnv().LOG,
                    String.format("Edits Delta processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    scanner.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    scanner.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
