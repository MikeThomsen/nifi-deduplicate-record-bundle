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
import static org.junit.Assert.*

class TestDetectDuplicateRecords {
    TestRunner runner
    MockRecordParser reader

    @Before
    void setup() {
        def mockCache = new MockCacheService()
        reader = new MockRecordParser()
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

    void doCountTests(int failure, int original, int duplicates, int notDuplicates, int notDupeCount, int dupeCount) {
        runner.assertTransferCount(DetectDuplicateRecords.REL_FAILURE, failure)
        runner.assertTransferCount(DetectDuplicateRecords.REL_ORIGINAL, original)
        runner.assertTransferCount(DetectDuplicateRecords.REL_DUPLICATES, duplicates)
        runner.assertTransferCount(DetectDuplicateRecords.REL_NOT_DUPLICATE, notDuplicates)

        assertEquals(String.valueOf(dupeCount), runner.getFlowFilesForRelationship(DetectDuplicateRecords.REL_DUPLICATES)[0].getAttribute("record.count"))
        assertEquals(String.valueOf(notDupeCount), runner.getFlowFilesForRelationship(DetectDuplicateRecords.REL_NOT_DUPLICATE)[0].getAttribute("record.count"))
    }

    @Test
    void testDetectDuplicates() {
        [
            [
                "John", "Q", "Smith"
            ],
            [
                "John", "Q", "Smith"
            ],
            [
                "Jane", "X", "Doe"
            ]
        ].each { record ->
            reader.addRecord(record.toArray())
        }

        runner.enqueue("")
        runner.run()

        doCountTests(0, 1, 1, 1, 2, 1)
    }

    @Test
    void testNoDuplicates() {
        [
            [
                "John", "Q", "Smith"
            ],
            [
                "Jack", "Z", "Brown"
            ],
            [
                "Jane", "X", "Doe"
            ]
        ].each { record ->
            reader.addRecord(record.toArray())
        }

        runner.enqueue("")
        runner.run()

        doCountTests(0, 1, 1, 1, 3, 0)
    }
}
