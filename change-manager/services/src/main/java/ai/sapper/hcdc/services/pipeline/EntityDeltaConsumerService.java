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

package ai.sapper.hcdc.services.pipeline;

import ai.sapper.cdc.common.model.services.BasicResponse;
import ai.sapper.cdc.common.model.services.ConfigSource;
import ai.sapper.cdc.common.model.services.EResponseState;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.hcdc.agents.main.EntityChangeDeltaConsumer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EntityDeltaConsumerService {
    private EntityChangeDeltaConsumer handler;

    @RequestMapping(value = "/entity/consumer/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<ProcessorState.EProcessorState>> start(@RequestBody ConfigSource config) {
        try {
            handler = new EntityChangeDeltaConsumer();
            handler.setConfigFile(config.getPath())
                    .setConfigSource(config.getType().name());

            handler.init();
            handler.start();
            DefaultLogger.info(handler.getEnv().LOG,
                    String.format("Edits Delta processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    handler.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    handler.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/entity/consumer/status", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<ProcessorState.EProcessorState>> state() {
        try {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    handler.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    handler.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/entity/consumer/stop", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<ProcessorState.EProcessorState>> stop() {
        try {
            handler.stop();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    handler.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    handler.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
