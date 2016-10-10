package com.epam.cc.java.ftp.prototype;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Example of serializer for {link FileAcceptStatus}.
 * <p>
 * Created by Maksym Bruner.
 */
public class StatusSerializer {

    public static String toString(FileAcceptStatus status) {
        ObjectMapper mapper = new ObjectMapper();
        String value = "";
        try {
            value = mapper.writeValueAsString(status);
        } catch (JsonProcessingException e) {
            // handle exception
        }
        return value;
    }

    public static FileAcceptStatus fromString(String value) {
        ObjectMapper mapper = new ObjectMapper();
        FileAcceptStatus status = null;
        try {
            status = mapper.readValue(value, FileAcceptStatus.class);
        } catch (IOException e) {
            // handle exception
        }
        return status;
    }

}
