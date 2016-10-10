package com.epam.cc.java.ftp.prototype.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Simple file processor that just wait for 10s and returns file as is.
 * <p>
 * Created by Maksym Bruner.
 */
public class DelayedProcessor {

    private static Logger logger = LoggerFactory.getLogger(DelayedProcessor.class);

    public File process(File file) {
        try {
            logger.info("Starting processing file: {}", file.getName());
            for (int i = 1; i <= 10; i++) {
                logger.info("{}: {}0% done", file.getName(), i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            //
        }
        return file;
    }
}
