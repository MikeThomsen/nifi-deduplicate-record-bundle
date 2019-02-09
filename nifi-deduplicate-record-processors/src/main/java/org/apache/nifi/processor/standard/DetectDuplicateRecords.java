package org.apache.nifi.processor.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.RecordWriter;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    public static final Relationship REL_DUPLICATES = new Relationship.Builder()
        .name("duplicates")
        .description("Duplicates are assembled into a record set for this relationship.")
        .build();
    public static final Relationship REL_NOT_DUPLICATE = new Relationship.Builder()
        .name("not duplicates")
        .description("Records that have not been detected as duplicates are assembled into a record set for this relationship.")
        .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original input flowfile is sent to this relationship unless there is a fatal error in the processing.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("When processing fails, the input flowfile goes to this relationship.")
        .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_DUPLICATES, REL_NOT_DUPLICATE, REL_ORIGINAL, REL_FAILURE
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile DistributedMapCacheClient mapCacheClient;
    private RecordPathCache recordPathCache;
    private Serializer<String> serializer = new StringSerializer();

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

        FlowFile duplicates = session.create(input);
        FlowFile notDuplicates = session.create(input);

        try (InputStream is = session.read(input);
             OutputStream dOS = session.write(duplicates);
            OutputStream ndOS = session.write(notDuplicates)) {

            RecordReader reader = readerFactory.createRecordReader(input, is, getLogger());
            RecordSetWriter dupeWriter = writerFactory.createWriter(getLogger(), writerFactory.getSchema(input.getAttributes(), null), dOS);
            RecordSetWriter notDupeWriter = writerFactory.createWriter(getLogger(), writerFactory.getSchema(input.getAttributes(), null), ndOS);

            String recordPath = context.getProperty(RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath path = recordPathCache.getCompiled(recordPath);

            Record record;
            dupeWriter.beginRecordSet();
            notDupeWriter.beginRecordSet();

            long dupeCount = 0;
            long notDupeCount = 0;
            while ((record = reader.nextRecord()) != null) {
                RecordPathResult result = path.evaluate(record);
                Optional<FieldValue> fieldValue = result.getSelectedFields().findFirst();
                if (fieldValue.isPresent()) {
                    FieldValue value = fieldValue.get();
                    String valueAsString = value.getValue().toString();

                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(String.format("Doing lookup using result %s", valueAsString));
                    }

                    boolean exists = mapCacheClient.containsKey(valueAsString, serializer);
                    if (exists) {
                        dupeWriter.write(record);
                        dupeCount++;
                    } else {
                        mapCacheClient.putIfAbsent(valueAsString, "exists", serializer, serializer);
                        notDupeWriter.write(record);
                        notDupeCount++;
                    }
                }
            }
            dupeWriter.finishRecordSet();
            notDupeWriter.finishRecordSet();
            dupeWriter.close();
            notDupeWriter.close();
            ndOS.close();
            dOS.close();
            is.close();

            duplicates = session.putAttribute(duplicates, "record.count", String.valueOf(dupeCount));
            notDuplicates = session.putAttribute(notDuplicates, "record.count", String.valueOf(notDupeCount));

            session.transfer(duplicates, REL_DUPLICATES);
            session.transfer(notDuplicates, REL_NOT_DUPLICATE);
            session.transfer(input, REL_ORIGINAL);
        } catch (Exception ex) {
            getLogger().error("Failed in detecting duplicate records.", ex);
            session.remove(duplicates);
            session.remove(notDuplicates);
            session.transfer(input, REL_FAILURE);
        }
    }
}
