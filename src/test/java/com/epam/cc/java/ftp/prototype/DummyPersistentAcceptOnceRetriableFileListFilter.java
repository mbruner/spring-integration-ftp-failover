package com.epam.cc.java.ftp.prototype;

import org.springframework.integration.metadata.ConcurrentMetadataStore;

/**
 * Implementation for {link DummyFile} filter.
 * <p>
 * Created by Maksym Bruner.
 */
public class DummyPersistentAcceptOnceRetriableFileListFilter extends AbstractPersistentAcceptOnceRetriableFileListFilter<DummyFile> {

    public DummyPersistentAcceptOnceRetriableFileListFilter(ConcurrentMetadataStore store, String prefix) {
        super(store, prefix);
    }

    @Override
    protected String fileName(DummyFile file) {
        return file.getName();
    }
}
