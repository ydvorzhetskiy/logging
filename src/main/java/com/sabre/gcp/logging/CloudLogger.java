package com.sabre.gcp.logging;

import com.google.cloud.MonitoredResource;
import com.google.cloud.logging.*;
import lombok.val;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.Arrays;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public final class CloudLogger implements Serializable {

    private static final String DEFAULT_LOG_NAME = null;
    private static String APP_NAME = null;

    public static void setAppName(String appName) {
        APP_NAME = appName;
    }

    public static CloudLogger getLogger(Class<?> clazz, KV... labels) {
        return new CloudLogger(DEFAULT_LOG_NAME, clazz.getName(), labels);
    }

    private final String logName;
    private final String loggerName;
    private final KV[] permanentLabels;

    private CloudLogger(String logName, String loggerName, KV<Object, Object>[] labels) {
        this.logName = logName;
        this.loggerName = loggerName;
        this.permanentLabels = labels;
    }

    private void logEvent(Severity severity, String message, KV<Object, Object>[] labels) {
        Logging logging = LoggingOptions.getDefaultInstance().getService();
        val builder = LogEntry.newBuilder(Payload.JsonPayload.of(singletonMap("message", message)))
            .setSeverity(severity)
            .addLabel("loggerName", this.loggerName);

        if (permanentLabels !=null){
            Arrays.stream(permanentLabels)
                    .forEach(item -> builder.addLabel(String.valueOf(item.getKey()), String.valueOf(item.getValue())));
        }
        if (labels != null) {
            Arrays.stream(labels)
                .forEach(item -> builder.addLabel(String.valueOf(item.getKey()), String.valueOf(item.getValue())));
        }
        logging.write(
            singletonList(builder.build()),
            Logging.WriteOption.logName(logName),
            Logging.WriteOption.resource(MonitoredResource.newBuilder(APP_NAME).build()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void debug(String message, KV... labels) {
        this.logEvent(Severity.DEBUG, message, labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void debug(String message, Throwable e, KV... labels) {
        this.logEvent(Severity.DEBUG, message + "\n" + e.toString(), labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void error(String message, KV... labels) {
        this.logEvent(Severity.ERROR, message, labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void error(String message, Throwable e, KV... labels) {
        this.logEvent(Severity.ERROR, message + "\n" + e.toString(), labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void info(String message, KV... labels) {
        this.logEvent(Severity.INFO, message, labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void info(String message, Throwable e, KV... labels) {
        this.logEvent(Severity.INFO, message + "\n" + e.toString(), labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void warn(String message, KV... labels) {
        this.logEvent(Severity.WARNING, message, labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void warn(String message, Throwable e, KV... labels) {
        this.logEvent(Severity.WARNING, message + "\n" + e.toString(), labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void critical(String message, KV... labels) {
        this.logEvent(Severity.CRITICAL, message, labels);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void critical(String message, Throwable e, KV... labels) {
        this.logEvent(Severity.CRITICAL, message + "\n" + e.toString(), labels);
    }
}
