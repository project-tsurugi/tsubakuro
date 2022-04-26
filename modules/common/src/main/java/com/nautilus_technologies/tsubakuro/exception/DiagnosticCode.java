package com.nautilus_technologies.tsubakuro.exception;

/**
 * An abstract interface of structured diagnostic code.
 */
public interface DiagnosticCode {

    /**
     * Returns the structured code text of this.
     * The structured code is form of {@code "PPP-NNNNN"}.
     * {@code "PPP"} part is three or more alphabet characters as structured code prefix,
     * and {@code "NNNNN"} part is code number by five digits.
     * @return the structured code text
     */
    String getStructuredCode();

    /**
     * Returns the number part of this structured code.
     * @return the number part.
     * @see #getCodeNumber()
     */
    int getCodeNumber();

    /**
     * Returns a human readable label of this code.
     * @return the label
     */
    String name();

    /**
     * Provides {@link DiagnosticCode}.
     */
    @FunctionalInterface
    public interface Provider {

        /**
         * Returns diagnostic code.
         * @return the diagnostic code
         */
        DiagnosticCode getDiagnosticCode();
    }
}
