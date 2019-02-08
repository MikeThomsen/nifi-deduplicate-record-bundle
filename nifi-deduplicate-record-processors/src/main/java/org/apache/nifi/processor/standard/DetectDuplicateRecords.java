package org.apache.nifi.processor.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "duplicate", "map", "cache", "detect"})
public class DetectDuplicateRecords extends AbstractProcessor {
    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("ddr-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for reading input flowfiles.")
        .required(true)
        .addValidator(Validator.VALID)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("ddr-record-writer")
        .displayName("Record Writer")
        .description("The record writer to use for writing output flowfiles.")
        .required(true)
        .addValidator(Validator.VALID)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();
    public static final PropertyDescriptor MAP_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("ddr-cache-service")
        .displayName("Cache Service")
        .description("The Map Cache service to use for accessing a lookup table to check for duplicates.")
        .identifiesControllerService(DistributedMapCacheClient.class)
        .required(true)
        .addValidator(Validator.VALID)
        .build();
    public static final PropertyDescriptor RECORD_PATH = new PropertyDescriptor.Builder()
        .name("ddr-record-path")
        .displayName("Lookup Record Path")
        .description("The record path operation to use for generating the lookup key for each record.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        RECORD_READER, RECORD_WRITER, MAP_CACHE_SERVICE, RECORD_PATH
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile DistributedMapCacheClient mapCacheClient;
    private RecordPathCache recordPathCache;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        mapCacheClient = context.getProperty(MAP_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        recordPathCache = new RecordPathCache(25);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }
    }
}
