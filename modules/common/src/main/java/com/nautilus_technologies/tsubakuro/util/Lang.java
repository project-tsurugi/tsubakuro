package com.nautilus_technologies.tsubakuro.util;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

/**
 * Common Java language utilities.
 */
public final class Lang {

    /**
     * Executes an action with suppressing occurred exception.
     * @param handler the exception handler
     * @param executable the action
     */
    public static void suppress(@Nonnull Consumer<? super Exception> handler, @Nonnull Executable executable) {
        Objects.requireNonNull(handler);
        Objects.requireNonNull(executable);
        try {
            executable.execute();
        } catch (Exception e) {
            handler.accept(e);
        }
    }

    /**
     * Executes an action with suppressing occurred exception.
     * The suppressed exception will be recorded to {@link Throwable#addSuppressed(Throwable) context.addSuppressed()}.
     * @param context the context exception
     * @param executable the action
     */
    public static void suppress(@Nonnull Throwable context, @Nonnull Executable executable) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(executable);
        try {
            executable.execute();
        } catch (Exception e) {
            context.addSuppressed(e);
        }
    }

    /**
     * An executable interface.
     */
    @FunctionalInterface
    public interface Executable {

        /**
         * Executes an action.
         * @throws Exception if exception was occurred
         */
        void execute() throws Exception;
    }

    private Lang() {
        throw new AssertionError();
    }
}
