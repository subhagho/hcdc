package ai.sapper.hcdc.services.namenode;

import ai.sapper.cdc.common.filters.DomainFilter;
import ai.sapper.cdc.common.filters.DomainFilters;
import ai.sapper.cdc.common.filters.Filter;
import ai.sapper.cdc.common.model.services.*;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.main.SnapshotRunner;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.services.ServiceHelper;
import com.google.common.base.Strings;
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
            ServiceHelper.checkService(processor.name(), processor);
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
            ServiceHelper.checkService(processor.name(), processor);
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
            ServiceHelper.checkService(processor.name(), processor);
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
            ServiceHelper.checkService(processor.name(), processor);
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
            ServiceHelper.checkService(processor.name(), processor);
            List<Filter> f = processor.getProcessor().removeFilter(domain, entity, path);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<Filter>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/run", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<Integer>> run() {
        try {
            DefaultLogger.LOGGER.info("Snapshot run called...");
            ServiceHelper.checkService(processor.name(), processor);
            int count = processor.getProcessor().run();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    count),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    -1).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/done", method = RequestMethod.POST)
    public ResponseEntity<DFSFileReplicaState> snapshotDone(@RequestBody SnapshotDoneRequest request) {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            SchemaEntity entity = new SchemaEntity(request.getDomain(), request.getEntity());
            DFSFileReplicaState rState = processor.getProcessor()
                    .snapshotDone(request.getHdfsPath(),
                            entity,
                            request.getTransactionId());
            return new ResponseEntity<>(rState, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DFSFileReplicaState) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/status", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNodeEnvState>> state() {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/snapshot/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNodeEnvState>> start(@RequestBody ConfigSource config) {
        try {
            processor = new SnapshotRunner();
            processor.setConfigFile(config.getPath())
                    .setConfigSource(config.getType().name());
            processor.init();
            DefaultLogger.info(processor.getEnv().LOG,
                    String.format("EditsLog processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            DefaultLogger.error(processor.getEnv().LOG, "Error starting service.", t);
            DefaultLogger.stacktrace(processor.getEnv().LOG, t);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/snapshot/stop", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNodeEnvState>> stop() {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            processor.stop();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/schema/expand/domain/{domain}", method = RequestMethod.GET)
    public ResponseEntity<List<PathOrSchema>> expandDomain(@PathVariable("domain") String domain) {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            SchemaManager schemaManager = NameNodeEnv.get(processor.name())
                    .schemaManager();
            if (schemaManager == null) {
                throw new Exception("SchemaManager not initialized...");
            }
            if (Strings.isNullOrEmpty(domain)
                    || domain.compareToIgnoreCase("null") == 0) {
                domain = null;
            }
            List<PathOrSchema> paths = schemaManager.domainNodes(domain);
            return new ResponseEntity<>(paths, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<PathOrSchema>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/schema/expand/path/{domain}", method = RequestMethod.GET)
    public ResponseEntity<List<PathOrSchema>> expandPath(@PathVariable("domain") String domain,
                                                         @RequestBody String path) {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            SchemaManager schemaManager = NameNodeEnv.get(processor.name())
                    .schemaManager();
            if (schemaManager == null) {
                throw new Exception("SchemaManager not initialized...");
            }
            List<PathOrSchema> paths = schemaManager.pathNodes(domain, path);
            return new ResponseEntity<>(paths, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<PathOrSchema>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
