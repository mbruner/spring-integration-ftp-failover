package com.epam.cc.java.ftp.prototype.demo;

import com.epam.cc.java.ftp.prototype.CommitableFilter;
import com.epam.cc.java.ftp.prototype.FilePersistentAcceptOnceRetriableFileListFilter;
import com.epam.cc.java.ftp.prototype.FtpExtendedInboundFileSynchronizer;
import com.epam.cc.java.ftp.prototype.FtpPersistentAcceptOnceRetriableFileListFilter;
import org.apache.commons.net.ftp.FTPFile;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.ftp.inbound.FtpInboundFileSynchronizingMessageSource;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.redis.metadata.RedisMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.transaction.*;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;

/**
 * Example of Java Configuration for demo application.
 * <p>
 * Created by Maksym Bruner.
 */
@Configuration
@EnableIntegration
@ImportResource("classpath:spring-integration-beans.xml")
public class FtpChannelConfig {

    private static final String LOCAL_FILTER_PREFIX = "fileLocalAcceptOnceRetriableFilter-";
    private static final String REMOTE_FILTER_PREFIX = "ftpRemoteAcceptOnceFilter-";

    private String host = "localhost";
    private int port = 21;
    private String user = "bob";
    private String password = "bob";
    private boolean deleteRemoteFiles = false;
    private String remoteDirectory = "/in";
    private String localProcessingDirectory = "./build/tmp/ftpInbound";
    private int filePollingRate = 5000;
    private int maxMessagesPerPoll = 1;
    private int corePoolSize = 2;
    private int maxPoolSize = 2;
    private int maxTries = 3;
    private int maxAcceptedFilesListLength = 2;


    private String redisHost = "localhost";
    private int redisPort = 6379;
    private String redisKey = "si";


    @Bean
    MessageChannel ftpInbound() {
        return new DirectChannel();
    }

    @Bean
    MessageChannel filesOutbound() {
        return new DirectChannel();
    }

    @Bean
    public AbstractInboundFileSynchronizer ftpInboundFileSynchronizer() {
        FtpExtendedInboundFileSynchronizer fileSynchronizer = new FtpExtendedInboundFileSynchronizer(ftpSessionFactory());
        fileSynchronizer.setDeleteRemoteFiles(deleteRemoteFiles);
        fileSynchronizer.setRemoteDirectory(remoteDirectory);
        fileSynchronizer.setFilter(ftpRemoteCompositeFilter());
        fileSynchronizer.setCommitableFilter(ftpPersistentFilter());
        return fileSynchronizer;
    }

    @Bean
    @InboundChannelAdapter(channel = "ftpInbound", poller = @Poller("poller"))
    public MessageSource<File> ftpMessageSource() {
        FtpInboundFileSynchronizingMessageSource source =
                new FtpInboundFileSynchronizingMessageSource(ftpInboundFileSynchronizer());
        source.setLocalDirectory(new File(localProcessingDirectory));
        source.setAutoCreateLocalDirectory(true);
        source.setLocalFilter(ftpLocalCompositeFilter());
        return source;
    }

    @Bean
    @ServiceActivator(inputChannel = "ftpInbound", outputChannel = "filesOutbound")
    public DelayedProcessor processor() {
        DelayedProcessor processor = new DelayedProcessor();
        return processor;
    }

    @Bean
    public CompositeFileListFilter<FTPFile> ftpRemoteCompositeFilter() {
        return new CompositeFileListFilter<>(Arrays.asList(ftpPersistentFilter()));
    }

    @Bean
    public CommitableFilter<FTPFile> ftpPersistentFilter() {
        FtpPersistentAcceptOnceRetriableFileListFilter persistentAcceptOnceFilter =
                new FtpPersistentAcceptOnceRetriableFileListFilter(metadataStore(), REMOTE_FILTER_PREFIX);

        persistentAcceptOnceFilter.setMaxAcceptedFileListLength(maxAcceptedFilesListLength);

        return persistentAcceptOnceFilter;
    }

    @Bean
    public CompositeFileListFilter<File> ftpLocalCompositeFilter() {
        return new CompositeFileListFilter<>(Arrays.asList(fileLocalAcceptOnceRetriableFilter()));
    }

    @Bean
    public FilePersistentAcceptOnceRetriableFileListFilter fileLocalAcceptOnceRetriableFilter() {
        FilePersistentAcceptOnceRetriableFileListFilter filter = new FilePersistentAcceptOnceRetriableFileListFilter(metadataStore(),
                LOCAL_FILTER_PREFIX);

        filter.setMaxTry(maxTries);
        filter.setMaxAcceptedFileListLength(maxAcceptedFilesListLength);

        return filter;
    }

    @Bean
    public PollerMetadata poller() {
        return Pollers.fixedRate(filePollingRate)
                      .maxMessagesPerPoll(maxMessagesPerPoll)
                      .transactional(transactionManager())
                      .transactionSynchronizationFactory(customTransactionSynchronizationFactory())
                      .taskExecutor(executor())
                      .get();
    }

    @Bean
    public ThreadPoolTaskExecutor executor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
        threadPoolTaskExecutor.setMaxPoolSize(maxPoolSize);

        return threadPoolTaskExecutor;
    }

    @Bean
    public TransactionSynchronizationProcessor customTransactionSynchronizationProcessor() {
        return new CustomTransactionSynchronizationProcessor(fileLocalAcceptOnceRetriableFilter());
    }

    @Bean
    public TransactionSynchronizationFactory customTransactionSynchronizationFactory() {
        return new DefaultTransactionSynchronizationFactory(customTransactionSynchronizationProcessor());
    }

    @Bean
    public PseudoTransactionManager transactionManager() {
        return new PseudoTransactionManager();
    }

    @Bean
    public SessionFactory<FTPFile> ftpSessionFactory() {
        DefaultFtpSessionFactory ftpSessionFactory = new DefaultFtpSessionFactory();
        ftpSessionFactory.setHost(host);
        ftpSessionFactory.setPort(port);
        ftpSessionFactory.setUsername(user);
        ftpSessionFactory.setPassword(password);
        ftpSessionFactory.setClientMode(2);
        return new CachingSessionFactory<>(ftpSessionFactory);
    }

    @Bean
    public ConcurrentMetadataStore metadataStore() {
        return new RedisMetadataStore(redisConnectionFactory(), redisKey);
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        JedisConnectionFactory redisConnectionFactory = new JedisConnectionFactory();

        redisConnectionFactory.setHostName(redisHost);
        redisConnectionFactory.setPort(redisPort);

        return redisConnectionFactory;
    }


    private static class CustomTransactionSynchronizationProcessor implements TransactionSynchronizationProcessor {
        private FilePersistentAcceptOnceRetriableFileListFilter filePersistentAcceptOnceRetriableFileListFilter;

        public CustomTransactionSynchronizationProcessor(
                FilePersistentAcceptOnceRetriableFileListFilter filePersistentAcceptOnceRetriableFileListFilter) {
            this.filePersistentAcceptOnceRetriableFileListFilter = filePersistentAcceptOnceRetriableFileListFilter;
        }


        @Override
        public void processBeforeCommit(IntegrationResourceHolder holder) {
            // Do nothing for now
        }

        @Override
        public void processAfterCommit(IntegrationResourceHolder holder) {
            Object payload = Optional.ofNullable(holder).map(IntegrationResourceHolder::getMessage)
                                     .map(Message::getPayload).orElse(null);
            if (payload == null || !(payload instanceof File)) {
                return;
            }

            File file = (File) holder.getMessage().getPayload();

            filePersistentAcceptOnceRetriableFileListFilter.commit(file);
        }

        @Override
        public void processAfterRollback(IntegrationResourceHolder holder) {
            // Do nothing for now
        }
    }
}
