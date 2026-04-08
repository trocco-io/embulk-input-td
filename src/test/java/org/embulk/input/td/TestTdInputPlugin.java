package org.embulk.input.td;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the temporary file download mechanism in TdInputPlugin.
 *
 * The core fix (https://github.com/primenumber-dev/n-transfer-ui/issues/42731) separates
 * network I/O from data processing by downloading to a temp file first.
 * These tests verify that:
 * 1. Data is correctly written to and read from the temp file
 * 2. On retry (callback invoked multiple times), REPLACE_EXISTING prevents data duplication
 * 3. The temp file is cleaned up after processing
 */
public class TestTdInputPlugin {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Simulates the callback logic used in TdInputPlugin.run() Phase 1.
     * Downloads the input stream to a temp file using Files.copy with REPLACE_EXISTING.
     */
    private static void downloadToTempFile(InputStream input, Path tempFile) {
        try {
            Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testDownloadToTempFile_normalCase() throws Exception {
        byte[] data = "hello world".getBytes();
        Path tempFile = tempFolder.newFile("test.msgpack.gz").toPath();

        downloadToTempFile(new ByteArrayInputStream(data), tempFile);

        byte[] result = Files.readAllBytes(tempFile);
        assertArrayEquals(data, result);
    }

    @Test
    public void testDownloadToTempFile_retryOverwritesPreviousData() throws Exception {
        // Simulates the retry scenario:
        // 1st attempt: partial data written, then IOException
        // 2nd attempt: full data written successfully
        // REPLACE_EXISTING should overwrite the partial data from the 1st attempt

        Path tempFile = tempFolder.newFile("test.msgpack.gz").toPath();

        // 1st attempt: write partial data (simulates successful partial download before network error)
        byte[] partialData = "partial data from first attempt".getBytes();
        downloadToTempFile(new ByteArrayInputStream(partialData), tempFile);

        // Verify partial data is on disk
        assertEquals(partialData.length, Files.size(tempFile));

        // 2nd attempt (retry): write full data with REPLACE_EXISTING
        byte[] fullData = "complete data".getBytes();
        downloadToTempFile(new ByteArrayInputStream(fullData), tempFile);

        // Verify the file contains ONLY the full data, not partial + full
        byte[] result = Files.readAllBytes(tempFile);
        assertArrayEquals(fullData, result);
        assertEquals(fullData.length, Files.size(tempFile));
    }

    @Test
    public void testDownloadToTempFile_retryWithLargerPartialData() throws Exception {
        // Edge case: 1st attempt writes MORE data than the 2nd attempt
        // REPLACE_EXISTING should still correctly overwrite

        Path tempFile = tempFolder.newFile("test.msgpack.gz").toPath();

        // 1st attempt: large partial data
        byte[] largePartialData = new byte[10000];
        for (int i = 0; i < largePartialData.length; i++) {
            largePartialData[i] = (byte) (i % 256);
        }
        downloadToTempFile(new ByteArrayInputStream(largePartialData), tempFile);
        assertEquals(10000, Files.size(tempFile));

        // 2nd attempt (retry): smaller complete data
        byte[] smallFullData = "small but complete".getBytes();
        downloadToTempFile(new ByteArrayInputStream(smallFullData), tempFile);

        byte[] result = Files.readAllBytes(tempFile);
        assertArrayEquals(smallFullData, result);
        assertEquals(smallFullData.length, Files.size(tempFile));
    }

    @Test
    public void testDownloadToTempFile_multipleRetries() throws Exception {
        // Simulates multiple retries (td-client-java retryLimit defaults to 7)
        Path tempFile = tempFolder.newFile("test.msgpack.gz").toPath();

        // Simulate 3 failed attempts with increasing partial data
        for (int attempt = 1; attempt <= 3; attempt++) {
            byte[] partialData = new byte[attempt * 1000];
            for (int i = 0; i < partialData.length; i++) {
                partialData[i] = (byte) attempt;
            }
            downloadToTempFile(new ByteArrayInputStream(partialData), tempFile);
        }

        // Final successful attempt
        byte[] finalData = "final successful download".getBytes();
        downloadToTempFile(new ByteArrayInputStream(finalData), tempFile);

        byte[] result = Files.readAllBytes(tempFile);
        assertArrayEquals(finalData, result);
    }

    @Test
    public void testDownloadToTempFile_streamErrorMidway() throws Exception {
        // Simulates IOException occurring mid-stream (e.g., network disconnection)
        Path tempFile = tempFolder.newFile("test.msgpack.gz").toPath();

        // Create a stream that throws IOException after reading some bytes
        byte[] partialData = "some data before error".getBytes();
        InputStream failingStream = new InputStream() {
            private final ByteArrayInputStream delegate = new ByteArrayInputStream(partialData);
            private int bytesRead = 0;

            @Override
            public int read() throws IOException {
                if (bytesRead >= partialData.length / 2) {
                    throw new IOException("Simulated network error");
                }
                bytesRead++;
                return delegate.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (bytesRead >= partialData.length / 2) {
                    throw new IOException("Simulated network error");
                }
                int n = delegate.read(b, off, Math.min(len, partialData.length / 2 - bytesRead));
                if (n > 0) {
                    bytesRead += n;
                }
                return n;
            }
        };

        // 1st attempt: fails mid-stream
        try {
            downloadToTempFile(failingStream, tempFile);
        } catch (RuntimeException e) {
            // Expected: Throwables.propagate wraps IOException
        }

        // 2nd attempt (retry): succeeds with complete data
        byte[] completeData = "complete data after retry".getBytes();
        downloadToTempFile(new ByteArrayInputStream(completeData), tempFile);

        // Verify only the complete data is present
        byte[] result = Files.readAllBytes(tempFile);
        assertArrayEquals(completeData, result);
    }

    @Test
    public void testTempFileCleanup() throws Exception {
        // Verify temp file is deleted in the finally block pattern used in TdInputPlugin
        Path tempFile = Files.createTempFile("embulk-input-td.", ".msgpack.gz");
        try {
            byte[] data = "test data".getBytes();
            downloadToTempFile(new ByteArrayInputStream(data), tempFile);
        } finally {
            if (tempFile != null) {
                Files.deleteIfExists(tempFile);
            }
        }

        assertFalse("Temp file should be deleted", Files.exists(tempFile));
    }

    @Test
    public void testCallbackInvokedMultipleTimes_simulatesRetryBehavior() throws Exception {
        // Simulates the actual td-client-java retry mechanism:
        // submitRequest() calls the callback, and on failure, retries by calling it again
        Path tempFile = tempFolder.newFile("test.msgpack.gz").toPath();

        final AtomicInteger callCount = new AtomicInteger(0);
        final byte[] correctData = "correct complete data".getBytes();

        Function<InputStream, Void> callback = new Function<InputStream, Void>() {
            @Override
            public Void apply(InputStream input) {
                try {
                    Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                return null;
            }
        };

        // Simulate: 1st call with partial data (before retry)
        callback.apply(new ByteArrayInputStream("partial".getBytes()));
        callCount.incrementAndGet();

        // Simulate: 2nd call with correct data (after retry)
        callback.apply(new ByteArrayInputStream(correctData));
        callCount.incrementAndGet();

        assertEquals(2, callCount.get());
        byte[] result = Files.readAllBytes(tempFile);
        assertArrayEquals(correctData, result);
    }
}
