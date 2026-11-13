/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_SKIP_SYNC;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Runs the commit-log replay and rollover tests with mmap preparation enabled. */
@RunWith(Theories.class)
public class CommitLogZeroFillTest extends BatchCommitLogTest
{
    @ClassRule
    public static final ExternalResource zeroFill = new ExternalResource()
    {
        private final WithProperties properties = new WithProperties();
        private boolean previousZeroFill;
        private DiskAccessMode previousAccessMode;

        @Override
        protected void before()
        {
            properties.set(TEST_CASSANDRA_SKIP_SYNC, false);
            DatabaseDescriptor.daemonInitialization();
            previousZeroFill = DatabaseDescriptor.getCommitLogPrefillImprovements();
            previousAccessMode = DatabaseDescriptor.getCommitLogWriteDiskAccessMode();
            DatabaseDescriptor.setCommitLogPrefillImprovements(true);
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(DiskAccessMode.mmap);
        }

        @Override
        protected void after()
        {
            DatabaseDescriptor.setCommitLogPrefillImprovements(previousZeroFill);
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(previousAccessMode);
            properties.close();
        }
    };

    @DataPoints
    public static final boolean[] booleanValues = { false, true };

    public CommitLogZeroFillTest()
    {
        super(null, EncryptionContextGenerator.createDisabledContext());
    }

    @BeforeClass
    public static void assertPreparationEnabled()
    {
        assertTrue(DatabaseDescriptor.getCommitLogPrefillImprovements());
        assertFalse("Recovery tests require real file syncs", SyncUtil.SKIP_SYNC);
    }

    @Test
    public void testZeroFillEnabled()
    {
        assertTrue(DatabaseDescriptor.getCommitLogPrefillImprovements());
        assertTrue(CommitLog.instance.segmentManager.allocatingFrom() instanceof MemoryMappedSegment);
    }

    @Test
    public void testRecoveryWithPartiallyPreparedFinalSegment() throws Exception
    {
        File partiallyFilled = tmpFile(CommitLogDescriptor.current_version);
        Files.write(partiallyFilled.toPath(), new byte[1024 * 1024]);
        // OSS tolerates a missing descriptor only in the newest segment.
        assertEquals(0, CommitLog.instance.recoverFiles(partiallyFilled));
    }

    @Theory
    public void testRecoveryWithZeroFilledTail(boolean partiallyPreparedNextSegment, boolean tornFinalMutation) throws Exception
    {
        assertFalse(COMMITLOG_IGNORE_REPLAY_ERRORS.getBoolean());
        ColumnFamilyStore table = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        table.clearUnsafe();
        Path directory = Files.createTempDirectory("mmap-recovery");
        try
        {
            int validMutations = 8;
            SegmentSnapshot snapshot = writeAndSnapshotSegment(directory, table, validMutations + (tornFinalMutation ? 1 : 0));
            if (tornFinalMutation)
            {
                byte[] image = Files.readAllBytes(snapshot.file.toPath());
                int cutPosition = snapshot.lastRecordStart + (snapshot.lastRecordEnd - snapshot.lastRecordStart) / 2;
                // Model a torn final record while preserving the full prefilled file length.
                Arrays.fill(image, cutPosition, image.length, (byte) 0);
                Files.write(snapshot.file.toPath(), image);
            }

            List<File> recoveryFiles = new ArrayList<>(Collections.singletonList(snapshot.file));
            if (partiallyPreparedNextSegment)
            {
                CommitLogDescriptor descriptor = new CommitLogDescriptor(CommitLogDescriptor.current_version,
                                                                         CommitLogDescriptor.idFromFileName(snapshot.file.name()) + 1,
                                                                         null, DatabaseDescriptor.getEncryptionContext());
                File partial = new File(directory.resolve(descriptor.fileName()));
                Files.write(partial.toPath(), new byte[1024 * 1024]);
                assertTrue(partial.length() < DatabaseDescriptor.getCommitLogSegmentSize());
                recoveryFiles.add(partial);
            }

            CommitLog.instance.resetUnsafe(true);
            Util.assertEmpty(Util.cmd(table).build());
            if (!tornFinalMutation)
                assertReplaysWithoutTruncation(snapshot.file, validMutations);

            CommitLogReplayer replayer = CommitLogReplayer.construct(CommitLog.instance, StorageService.instance.getLocalHostUUID());
            try
            {
                if (partiallyPreparedNextSegment && tornFinalMutation)
                {
                    // OSS retains headerless segments in its replay list, so the damaged segment is not last.
                    // Preserve its existing strict failure policy rather than silently changing recovery here.
                    assertEquals(recoveryFiles, CommitLogReader.filterCommitLogFiles(recoveryFiles.toArray(new File[0])));
                    assertThatThrownBy(() -> replayer.replayFiles(recoveryFiles.toArray(new File[0])))
                    .isInstanceOf(CommitLogReplayer.CommitLogReplayException.class)
                    .hasMessageContaining("Mutation checksum failure");
                }
                else
                {
                    replayer.replayFiles(recoveryFiles.toArray(new File[0]));
                }
            }
            finally
            {
                // Complete queued mutations even if OSS rejects the crash image.
                assertEquals(validMutations, replayer.blockForWrites());
            }
            assertEquals(validMutations, Util.getAll(Util.cmd(table).build()).size());
            for (int key = 0; key < validMutations; key++)
                assertEquals(recoveryValue(key), Util.getOnlyRow(Util.cmd(table, "prefill-" + key).columns("val").build())
                                                    .cells().iterator().next().buffer());
        }
        finally
        {
            table.clearUnsafe();
            CommitLog.instance.resetUnsafe(true);
            FileUtils.deleteRecursive(new File(directory));
        }
    }

    private static SegmentSnapshot writeAndSnapshotSegment(Path directory, ColumnFamilyStore table, int mutationCount) throws IOException
    {
        int lastRecordStart = 0;
        int lastRecordEnd = 0;
        for (int key = 0; key < mutationCount; key++)
        {
            Mutation mutation = new RowUpdateBuilder(table.metadata(), FBUtilities.timestampMicros(), "prefill-" + key)
                                .clustering("bytes").add("val", recoveryValue(key)).build();
            // Write only to the log so table contents must come exclusively from recovery.
            lastRecordEnd = CommitLog.instance.add(mutation).position;
            int recordSize = Math.toIntExact(Mutation.serializer.serializedSize(mutation, MessagingService.current_version)
                                            + CommitLogSegment.ENTRY_OVERHEAD_SIZE);
            lastRecordStart = lastRecordEnd - recordSize;
        }
        CommitLog.instance.sync(true);
        assertEquals(1, CommitLog.instance.segmentManager.getActiveSegments().size());
        CommitLogSegment segment = CommitLog.instance.segmentManager.allocatingFrom();
        assertTrue(segment instanceof MemoryMappedSegment);

        byte[] image = Files.readAllBytes(segment.logFile.toPath());
        assertEquals(DatabaseDescriptor.getCommitLogSegmentSize(), image.length);
        assertTrue(segment.contentSize() < image.length);
        assertArrayEquals(new byte[image.length - (int) segment.contentSize()],
                          Arrays.copyOfRange(image, (int) segment.contentSize(), image.length));
        // Capture the crash image outside the live directory before shutdown can alter its final marker.
        File snapshot = new File(directory.resolve(segment.logFile.name()));
        Files.write(snapshot.toPath(), image);
        return new SegmentSnapshot(snapshot, lastRecordStart, lastRecordEnd);
    }

    private static void assertReplaysWithoutTruncation(File file, int expectedMutations) throws IOException
    {
        List<ByteBuffer> replayedKeys = new ArrayList<>();
        CommitLogReplayer replayer = new CommitLogReplayer(CommitLog.instance, CommitLogPosition.NONE,
                                                         Collections.emptyMap(), CommitLogReplayer.ReplayFilter.create())
        {
            @Override
            public void handleMutation(Mutation mutation, int size, int entryLocation, CommitLogDescriptor descriptor)
            {
                replayedKeys.add(mutation.key().getKey());
            }
        };
        replayer.replayPath(file, false);
        List<ByteBuffer> expectedKeys = new ArrayList<>();
        for (int key = 0; key < expectedMutations; key++)
            expectedKeys.add(bytes("prefill-" + key));
        assertEquals(expectedKeys, replayedKeys);
    }

    private static ByteBuffer recoveryValue(int key)
    {
        byte[] value = new byte[1024 * (key + 1)];
        Arrays.fill(value, (byte) (key + 1));
        return ByteBuffer.wrap(value);
    }

    private static final class SegmentSnapshot
    {
        final File file;
        final int lastRecordStart;
        final int lastRecordEnd;

        SegmentSnapshot(File file, int lastRecordStart, int lastRecordEnd)
        {
            this.file = file;
            this.lastRecordStart = lastRecordStart;
            this.lastRecordEnd = lastRecordEnd;
        }
    }
}
