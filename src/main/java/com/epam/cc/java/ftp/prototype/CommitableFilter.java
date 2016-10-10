package com.epam.cc.java.ftp.prototype;

import org.springframework.integration.file.filters.FileListFilter;

/**
 * Interface for file filters with ability to commit file after processing.
 * <p>
 * Created by Maksym Bruner.
 */
public interface CommitableFilter<F> extends FileListFilter<F> {

    /**
     * Indicate that file that was previously passed by this filter (in {@link #filterFiles(Object[])}
     * have been processed successfully.
     *
     * @param file file that was processed.
     */
    void commit(F file);
}
