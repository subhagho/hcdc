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
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.filters.DomainFilter;
import ai.sapper.cdc.core.filters.DomainFilters;
import ai.sapper.cdc.core.filters.Filter;
import ai.sapper.cdc.core.model.DomainFilterAddRequest;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.SnapshotDoneRequest;
import ai.sapper.cdc.core.model.dfs.DFSFileReplicaState;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.hcdc.agents.main.SnapshotRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
public class SnapshotService {
    private static SnapshotRunner processor;

    @RequestMapping(value = "/snapshot/filters/add/{domain}", method = RequestMethod.PUT)
    public ResponseEntity<List<DomainFilters>> addFilter(@PathVariable("domain") String domain,
                                                         @RequestBody DomainFilterAddRequest request) {
        try {
            processor.checkState();
            if (request.getFilters() == null || request.getFilters().isEmpty()) {
                throw new Exception("No filters specified...");
            }
            List<DomainFilters> filters = new ArrayList<>();
            for (int ii = 0; ii < request.getFilters().size(); ii++) {
                DomainFilters dfs = processor.getProcessor().addFilter(domain,
                        request.getFilters().get(ii),
                        request.getGroup());
                filters.add(dfs);
            }
            return new ResponseEntity<>(filters,
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<DomainFilters>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/update/{domain}/{entity}/{group}", method = RequestMethod.PUT)
    public ResponseEntity<DomainFilter> updateGroup(@PathVariable("domain") String domain,
                                                    @PathVariable("domain") String entity,
                                                    @PathVariable("domain") String group) {
        try {
            processor.checkState();
            DomainFilter filter = processor.getProcessor().updateGroup(domain, entity, group);
            return new ResponseEntity<>(filter,
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DomainFilter) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}", method = RequestMethod.DELETE)
    public ResponseEntity<Filter> removeFilter(@PathVariable("domain") String domain,
                                               @RequestBody Filter filter) {
        try {
            processor.checkState();
            Filter f = processor.getProcessor().removeFilter(domain, filter);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((Filter) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}/{entity}", method = RequestMethod.DELETE)
    public ResponseEntity<DomainFilter> removeFilter(@PathVariable("domain") String domain,
                                                     @PathVariable("entity") String entity) {
        try {
            processor.checkState();
            DomainFilter f = processor.getProcessor().removeFilter(domain, entity);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DomainFilter) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}/{entity}/{path}", method = RequestMethod.DELETE)
    public ResponseEntity<List<Filter>> removeFilter(@PathVariable("domain") String domain,
                                                     @PathVariable("entity") String entity,
                                                     @PathVariable("path") String path) {
        try {
            processor.checkState();
            List<Filter> f = processor.getProcessor().removeFilter(domain, entity, path);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<Filter>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/run", method = RequestMethod.POST)
    public synchronized ResponseEntity<BasicResponse<String>> run() {
        try {
            DefaultLogger.info("Snapshot run called...");
            processor.checkState();
            if (!processor.getProcessor().state().isInitialized()) {
                throw new Exception(String.format("[%s] Processor not initialized. [state=%s]",
                        processor.getProcessor().name(),
                        processor.getProcessor().state().getState().name()));
            }
            if (processor.getProcessor().state().isRunning()) {
                throw new Exception(String.format("[%s] Processor is running. [state=%s]",
                        processor.getProcessor().name(),
                        processor.getProcessor().state().getState().name()));
            }
            processor.getProcessor().state().setState(ProcessorState.EProcessorState.Running);
            processor.getProcessor().runOnce();
            processor.getProcessor().state().setState(ProcessorState.EProcessorState.Initialized);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    "Snapshot run successful..."),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    t.getLocalizedMessage()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/done", method = RequestMethod.POST)
    public ResponseEntity<DFSFileReplicaState> snapshotDone(@RequestBody SnapshotDoneRequest request) {
        try {
            processor.checkState();
            SchemaEntity entity = new SchemaEntity(request.getDomain(), request.getEntity());
            HCdcTxId tid = new HCdcTxId(request.getTransactionId());

            DFSFileReplicaState rState = processor.getProcessor()
                    .snapshotDone(request.getHdfsPath(),
                            entity,
                            tid);
            return new ResponseEntity<>(rState, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DFSFileReplicaState) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/status", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<ProcessorState.EProcessorState>> state() {
        try {
            processor.checkState();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/snapshot/start", method = RequestMethod.POST)
    public synchronized ResponseEntity<BasicResponse<ProcessorState.EProcessorState>> start(@RequestBody ConfigSource config) {
        try {
            if (processor != null) {
                if (processor.getProcessor().state().isInitialized()) {
                    return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                            processor.status().getState()),
                            HttpStatus.OK);
                }
            }
            processor = new SnapshotRunner();
            processor.setConfigFile(config.getPath())
                    .setConfigSource(config.getType().name());
            processor.init();

            DefaultLogger.info(processor.getEnv().LOG,
                    String.format("EditsLog processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            DefaultLogger.error(processor.getEnv().LOG, "Error starting service.", t);
            DefaultLogger.stacktrace(processor.getEnv().LOG, t);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/snapshot/stop", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<ProcessorState.EProcessorState>> stop() {
        try {
            processor.checkState();
            processor.stop();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status().getState()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status().getState()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
