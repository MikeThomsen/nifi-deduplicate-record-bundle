package org.apache.nifi.processor

import org.apache.nifi.processor.standard.DetectDuplicateRecords
import org.apache.nifi.processor.standard.StringSerializer
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
    MockCacheService mockCache

    @Before
    void setup() {
        mockCache = new MockCacheService()
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

        def dupeFFs = runner.getFlowFilesForRelationship(DetectDuplicateRecords.REL_DUPLICATES)
        if (dupeFFs) {
            assertEquals(String.valueOf(dupeCount), dupeFFs[0].getAttribute("record.count"))
        }

        def notDupeFFs = runner.getFlowFilesForRelationship(DetectDuplicateRecords.REL_NOT_DUPLICATE)
        if (notDupeFFs) {
            assertEquals(String.valueOf(notDupeCount), notDupeFFs[0].getAttribute("record.count"))
        }
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


    void testAllDuplicates(boolean removeEmpty) {
        runner.setProperty(DetectDuplicateRecords.RECORD_PATH, "/firstName")
        def serializer = new StringSerializer()
        ["John", "Jack", "Jane"].each { name ->
            mockCache.putIfAbsent(name, "exists", serializer, serializer)
        }

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

        runner.setProperty(DetectDuplicateRecords.REMOVE_EMPTY, String.valueOf(removeEmpty))
        runner.enqueue("")
        runner.run()

        doCountTests(0, 1, 1, removeEmpty ? 0 : 1, 0, 3)
    }

    @Test
    void testAllAreDuplicates() {
        testAllDuplicates(false)
    }

    @Test
    void testRemoveEmpty() {
        testAllDuplicates(true)
    }

    @Test
    void testCacheValueFromRecordPath() {
        runner.setProperty(DetectDuplicateRecords.CACHE_VALUE_STRATEGY, DetectDuplicateRecords.STRAGEGY_RECORD_PATH)
        runner.setProperty(DetectDuplicateRecords.CACHE_VALUE, "/firstName")
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

        mockCache.assertContains("John-Q-Smith", "John")
        mockCache.assertContains("Jane-X-Doe", "Jane")
    }
}
