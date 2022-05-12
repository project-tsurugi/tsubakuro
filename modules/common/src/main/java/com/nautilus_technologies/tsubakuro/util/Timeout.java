package com.nautilus_technologies.tsubakuro.util;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.ResponseTimeoutException;
import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Awaits {@link FutureResponse} with timeout.
 */
public final class Timeout implements Cloneable {

    static final Logger LOG = LoggerFactory.getLogger(Timeout.class);

    /**
     * A disabled instance.
     */
    public static final Timeout DISABLED = new Timeout();

    private final long timeout;

    private final TimeUnit unit;

    private final Policy policy;

    /**
     * Creates a new disabled instance.
     */
    public Timeout() {
        this(0, TimeUnit.MILLISECONDS, Policy.IGNORE);
    }

    /**
     * Creates a new instance.
     * @param timeout the maximum time to wait, or {@code 0} to disable
     * @param unit the time unit of {@code timeout}
     * @param policy the timeout policy
     */
    public Timeout(long timeout, @Nonnull TimeUnit unit, @Nonnull Policy policy) {
        Objects.requireNonNull(unit);
        Objects.requireNonNull(policy);
        this.timeout = timeout;
        this.unit = unit;
        this.policy = policy;
    }

    /**
     * Returns whether or not timeout is enabled.
     * @return {@code true} if it is enabled, or {@code false} otherwise
     */
    public boolean isEnabled() {
        return timeout > 0;
    }

    /**
     * Wait for response using the current timeout setting.
     * @param future the target future
     * @throws ResponseTimeoutException if the current policy is {@link Policy#ERROR},
     *      then waiting for response is timeout
     * @throws IOException if {@link FutureResponse#get()} throws
     * @throws ServerException if {@link FutureResponse#get()} throws
     * @throws InterruptedException if {@link FutureResponse#get()} throws
     */
    public void waitFor(
            @Nonnull FutureResponse<Void> future) throws IOException, ServerException, InterruptedException {
        Objects.requireNonNull(future);
        try {
            if (isEnabled()) {
                future.get(timeout, unit);
            } else {
                future.get();
            }
        } catch (TimeoutException e) {
            switch (policy) {
            case IGNORE:
                LOG.debug("request is timeout", e);
                break;
            case WARN:
                LOG.warn("request is timeout", e);
                break;
            case ERROR:
            default:  // to suppress checkstyle warning
                throw new ResponseTimeoutException("request is timeout", e);
            }
        }
    }

    /**
     * Policy kind when waiting for response is timeout.
     */
    public enum Policy {

        /**
         * Report log as {@code DEBUG} level.
         */
        IGNORE,

        /**
         * Report log as {@code WARN} level.
         */
        WARN,

        /**
         * Raise an {@link ResponseTimeoutException}.
         */
        ERROR,
    }
}
