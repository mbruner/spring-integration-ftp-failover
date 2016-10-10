package com.epam.cc.java.ftp.prototype;

import org.apache.commons.net.ftp.FTPFile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;

import java.io.File;
import java.io.IOException;

/**
 * Extended version of {link AbstractInboundFileSynchronizer} with support of {link CommitableFilter}.
 * <p>
 * Created by Maksym Bruner.
 */
public class FtpExtendedInboundFileSynchronizer extends AbstractInboundFileSynchronizer<FTPFile> {


    private CommitableFilter<FTPFile> commitableFilter;

    /**
     * Create a synchronizer with the {@link SessionFactory} used to acquire {@link Session} instances.
     *
     * @param sessionFactory The session factory.
     */
    public FtpExtendedInboundFileSynchronizer(SessionFactory<FTPFile> sessionFactory) {
        super(sessionFactory);
        setRemoteDirectoryExpression(new LiteralExpression(null));
    }

    @Override
    protected void copyFileToLocalDirectory(String remoteDirectoryPath, FTPFile remoteFile, File localDirectory,
                                            Session<FTPFile> session) throws IOException {
        super.copyFileToLocalDirectory(remoteDirectoryPath, remoteFile, localDirectory, session);
        if (commitableFilter != null) {
            commitableFilter.commit(remoteFile);
        }
    }

    @Override
    protected boolean isFile(FTPFile file) {
        return file != null && file.isFile();
    }

    @Override
    protected String getFilename(FTPFile file) {
        return (file != null ? file.getName() : null);
    }

    @Override
    protected long getModified(FTPFile file) {
        return file.getTimestamp().getTimeInMillis();
    }

    public void setCommitableFilter(CommitableFilter<FTPFile> commitableFilter) {
        this.commitableFilter = commitableFilter;
    }

}
