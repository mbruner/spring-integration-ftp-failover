package com.epam.cc.java.ftp.prototype;

import org.springframework.integration.metadata.ConcurrentMetadataStore;

import java.io.File;

/**
 * Implementation for {link File} filter.
 * <p>
 * Created by Maksym Bruner.
 */
public class FilePersistentAcceptOnceRetriableFileListFilter extends AbstractPersistentAcceptOnceRetriableFileListFilter<File> {

    public FilePersistentAcceptOnceRetriableFileListFilter(ConcurrentMetadataStore store, String prefix) {
        super(store, prefix);
    }

    @Override
    protected String fileName(File file) {
        return file.getName();
    }

}
