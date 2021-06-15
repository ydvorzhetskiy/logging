package com.sabre.gcp.logging;

import lombok.val;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class LoggingUtils {

    public static <T> PTransform<PCollection<T>, WriteResult> logResult(
        CloudLogger logger,
        String successMessage,
        String errorMessage,
        PTransform<PCollection<T>, WriteResult> originalPTransform) {
        return new LogWrapper<T>(logger, successMessage, errorMessage, originalPTransform);
    }

    private static class LogWrapper<T> extends PTransform<PCollection<T>, WriteResult> {

        private final CloudLogger logger;
        private final String successMessage;
        private final String errorMessage;
        private final PTransform<PCollection<T>, WriteResult> originalPTransform;

        public LogWrapper(CloudLogger logger, String successMessage, String errorMessage,
                          PTransform<PCollection<T>, WriteResult> originalPTransform) {
            this.logger = logger;
            this.successMessage = successMessage;
            this.errorMessage = errorMessage;
            this.originalPTransform = originalPTransform;
        }

        @Override
        public String getName() {
            return originalPTransform.getName();
        }

        @Override
        public WriteResult expand(PCollection<T> input) throws RuntimeException {
            try {
                val result = originalPTransform.expand(input);
                if (successMessage != null) {
                    logger.info(successMessage);
                }
                return result;
            } catch (RuntimeException ex) {
                if (errorMessage != null) {
                    logger.error(errorMessage, ex);
                }
                throw ex;
            }
        }
    }
}
