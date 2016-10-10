package com.epam.cc.java.ftp.prototype;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.integration.metadata.ConcurrentMetadataStore;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class AbstractPersistentAcceptOnceRetriableFileListFilterTest {

    private static int MAX_ACCEPT = 3;
    private static int MAX_TRY = 2;
    private static long TIMEOUT_SKIP = 5L;
    private static long TIMEOUT_RETRY = 100L;

    private static String FILE_A = "A";
    private static String FILE_B = "B";
    private static String FILE_C = "C";
    private static String FILE_D = "D";

    DummyPersistentAcceptOnceRetriableFileListFilter filter;
    ConcurrentMetadataStore store;

    ArgumentCaptor<String> statusCapture;

    @Before
    public void init() {
        store = Mockito.mock(ConcurrentMetadataStore.class);
        filter = new DummyPersistentAcceptOnceRetriableFileListFilter(store, "");

        filter.setMaxAcceptedFileListLength(MAX_ACCEPT);
        filter.setMaxTry(MAX_TRY);
        long RETRY_TIMEOUT = 10L;
        filter.setRetryTimeoutSeconds(RETRY_TIMEOUT);

        statusCapture = ArgumentCaptor.forClass(String.class);
    }

    @Test
    public void testSimpleUsageCase() {
        DummyFile file1 = new DummyFile(FILE_A);
        DummyFile file2 = new DummyFile(FILE_B);

        mockStatus(FILE_A, null);
        mockStatus(FILE_B, null);

        List<DummyFile> result = filter.filterFiles(files(file1, file2));

        assertThat(result, hasSize(2));
        assertThat(result, hasItems(file1, file2));
    }

    @Test
    public void testRetryCase() {
        DummyFile file1 = new DummyFile(FILE_A);

        long timestamp = currentTimestamp() - TIMEOUT_RETRY;
        String fileStatus = fileInProgressStatus(1, timestamp);

        mockStatus(FILE_A, fileStatus);
        mockConcurrentSuccess(FILE_A, true);

        List<DummyFile> result = filter.filterFiles(files(file1));

        assertThat(result, hasSize(1));
        assertThat(result, hasItems(file1));
        verify(store).replace(eq(FILE_A), eq(fileStatus), statusCapture.capture());
        assertStatus(statusCapture.getValue(), FileAcceptStatus.IN_PROGRESS);
        assertTries(statusCapture.getValue(), 2);
        verifyNoConcurrentFlow(FILE_A);
    }

    @Test
    public void testSkipInProgressCase() {
        DummyFile file1 = new DummyFile(FILE_A);

        long timestamp = currentTimestamp() - TIMEOUT_SKIP;
        String fileStatus = fileInProgressStatus(1, timestamp);

        mockStatus(FILE_A, fileStatus);
        mockConcurrentSuccess(FILE_A, true);

        List<DummyFile> result = filter.filterFiles(files(file1));

        assertThat(result, empty());
        verifyNoConcurrentFlow(FILE_A);
    }

    @Test
    public void testSkipAfterRetryAndMarkRejectedCase() {
        DummyFile file1 = new DummyFile(FILE_A);

        long timestamp = currentTimestamp() - TIMEOUT_RETRY;
        String fileStatus = fileInProgressStatus(MAX_TRY, timestamp);

        mockStatus(FILE_A, fileStatus);
        mockConcurrentSuccess(FILE_A, true);

        List<DummyFile> result = filter.filterFiles(files(file1));

        assertThat(result, empty());
        verify(store).replace(eq(FILE_A), eq(fileStatus), statusCapture.capture());
        assertStatus(statusCapture.getValue(), FileAcceptStatus.REJECTED);
        verifyNoConcurrentFlow(FILE_A);
    }

    @Test
    public void testLimitedAcceptListCase() {
        DummyFile file1 = new DummyFile(FILE_A);
        DummyFile file2 = new DummyFile(FILE_B);
        DummyFile file3 = new DummyFile(FILE_C);
        DummyFile file4 = new DummyFile(FILE_D);

        mockStatus(FILE_A, null);
        mockStatus(FILE_B, null);
        mockStatus(FILE_C, null);
        mockStatus(FILE_D, null);

        List<DummyFile> result = filter.filterFiles(files(file1, file2, file3, file4));

        assertThat(result, hasSize(MAX_ACCEPT));
        assertThat(result, contains(file1, file2, file3));
    }

    @Test
    public void testConcurrentCase() {
        DummyFile file1 = new DummyFile(FILE_A);

        long timestamp = currentTimestamp() - TIMEOUT_RETRY;
        String fileInProgressStatusStatus = fileInProgressStatus(1, timestamp);
        String fileDoneStatus = fileDoneStatus();

        when(store.putIfAbsent(eq(FILE_A), Mockito.anyString())).thenReturn(fileInProgressStatusStatus);
        when(store.get(eq(FILE_A))).thenReturn(fileInProgressStatusStatus, fileDoneStatus);

        mockConcurrentSuccess(FILE_A, false);

        List<DummyFile> result = filter.filterFiles(files(file1));

        assertThat(result, empty());
        verify(store, times(2)).get(FILE_A);
    }

    @Test
    public void testCommitCase() {
        DummyFile file1 = new DummyFile(FILE_A);

        long timestamp = currentTimestamp();
        String fileStatus = fileInProgressStatus(1, timestamp);

        when(store.get(eq(FILE_A))).thenReturn(fileStatus);
        when(store.replace(eq(FILE_A), Mockito.anyString(), Mockito.anyString())).thenReturn(true);

        filter.commit(file1);

        verify(store).replace(eq(FILE_A), eq(fileStatus), statusCapture.capture());
        assertStatus(statusCapture.getValue(), FileAcceptStatus.DONE);
        verifyNoConcurrentFlow(FILE_A);
    }

    @Test
    public void testCommitConcurrentCase() {
        DummyFile file1 = new DummyFile(FILE_A);

        long timestamp = currentTimestamp();
        String fileStatus = fileInProgressStatus(1, timestamp);
        String fileDoneStatus = fileDoneStatus();

        when(store.get(eq(FILE_A))).thenReturn(fileStatus, fileDoneStatus);
        when(store.replace(eq(FILE_A), eq(fileStatus), Mockito.anyString())).thenReturn(false);

        filter.commit(file1);

        verify(store).replace(eq(FILE_A), eq(fileStatus), statusCapture.capture());
        assertStatus(statusCapture.getValue(), FileAcceptStatus.DONE);
        verify(store, times(2)).get(FILE_A);
    }

    private void verifyNoConcurrentFlow(String key) {
        verify(store, times(1)).get(key);
    }

    private void mockStatus(String key, String status) {
        when(store.putIfAbsent(eq(key), Mockito.anyString())).thenReturn(status);
        if (status != null) {
            when(store.get(eq(key))).thenReturn(status);
        }
    }

    private void mockConcurrentSuccess(String key, boolean success) {
        when(store.replace(eq(key), Mockito.anyString(), Mockito.anyString())).thenReturn(success);
    }

    private static long currentTimestamp() {
        return Instant.now().getEpochSecond();
    }

    private static String fileInProgressStatus(int tries, long timestamp) {
        return fileStatus(FileAcceptStatus.IN_PROGRESS, tries, timestamp);
    }

    private static String fileDoneStatus() {
        return fileStatus(FileAcceptStatus.DONE, 1, currentTimestamp());
    }

    private static String fileStatus(int status, int tries, long timestamp) {
        FileAcceptStatus acceptStatus = new FileAcceptStatus();

        acceptStatus.setStatus(status);
        acceptStatus.setTries(tries);
        acceptStatus.setLastTryTimestamp(timestamp);

        return StatusSerializer.toString(acceptStatus);
    }

    private static void assertStatus(String statusSerialized, int expected) {
        FileAcceptStatus status = StatusSerializer.fromString(statusSerialized);
        assertThat(status.getStatus(), equalTo(expected));
    }

    private static void assertTries(String statusSerialized, int expected) {
        FileAcceptStatus status = StatusSerializer.fromString(statusSerialized);
        assertThat(status.getTries(), equalTo(expected));
    }


    private static DummyFile[] files(DummyFile... files) {
        return files;
    }

}
