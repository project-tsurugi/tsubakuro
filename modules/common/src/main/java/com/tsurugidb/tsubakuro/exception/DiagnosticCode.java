/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.exception;

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
