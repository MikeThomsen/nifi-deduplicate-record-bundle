package org.apache.nifi.processor

import org.apache.nifi.processor.standard.DetectDuplicateRecords
import org.apache.nifi.serialization.record.MockRecordParser
import org.apache.nifi.serialization.record.MockRecordWriter
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test
import sun.security.krb5.internal.crypto.DesCbcCrcEType

class TestDetectDuplicateRecords {
    TestRunner runner

    @Before
    void setup() {
        def mockCache = new MockCacheService()
        def reader = new MockRecordParser()
        def writer = new MockRecordWriter()

        reader.addSchemaField(new RecordField("firstName", RecordFieldType.STRING.dataType))
        reader.addSchemaField(new RecordField("middleName", RecordFieldType.STRING.dataType))
        reader.addSchemaField(new RecordField("lastName", RecordFieldType.STRING.dataType))

        runner = TestRunners.newTestRunner(DetectDuplicateRecords.class)
        runner.addControllerService("cache", mockCache)
        runner.addControllerService("reader", reader)
        runner.addControllerService("writer", writer)
        runner.setProperty(DetectDuplicateRecords.RECORD_READER, "reader")
        runner.setProperty(DetectDuplicateRecords.RECORD_WRITER, "writer")
        runner.setProperty(DetectDuplicateRecords.MAP_CACHE_SERVICE, "cache")
        runner.setProperty(DetectDuplicateRecords.RECORD_PATH, "concat(/firstName, '-', /middleName, '-', /lastName)")
        runner.enableControllerService(mockCache)
        runner.enableControllerService(reader)
        runner.enableControllerService(writer)
        runner.assertValid()
    }

    @Test
    void testDetectDuplicates() {

    }
}
