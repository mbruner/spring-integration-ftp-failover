package com.epam.cc.java.ftp.prototype;

import org.apache.commons.net.ftp.FTPFile;
import org.springframework.integration.metadata.ConcurrentMetadataStore;

/**
 * Implementation for {link FTPFile} filter.
 * <p>
 * Created by minim on 9/15/16.
 */
public class FtpPersistentAcceptOnceRetriableFileListFilter extends AbstractPersistentAcceptOnceRetriableFileListFilter<FTPFile> {

    public FtpPersistentAcceptOnceRetriableFileListFilter(ConcurrentMetadataStore store, String prefix) {
        super(store, prefix);
    }

    @Override
    protected String fileName(FTPFile file) {
        return file.getName();
    }

}
