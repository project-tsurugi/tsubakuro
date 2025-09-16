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
package com.tsurugidb.tsubakuro.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for elements to be mentioned in documents.
 * <p>
 * This annotation is designed primary for automatic document generations.
 * For example, to attach this annotation to the definition of each structured error codes,
 * then you can easily create a table of those codes.
 * </p>
 * @version 1.7.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
    ElementType.TYPE,
    ElementType.FIELD,
    ElementType.CONSTRUCTOR,
    ElementType.METHOD
})
public @interface Doc {

    /**
     * A delimiter character to separate reference title and its location.
     */
    char REFERENCE_DELIMITER = '@';

    /**
     * An unspecified value for {@link #code()}.
     * @since 1.7.0
     */
    int CODE_UNSPECIFIED = -1;

    /**
     * Description of the target element.
     * @return Description of the target element
     */
    String[] value();

    /**
     * Additional notes for the target element.
     * @return additional notes for the target element
     */
    String[] note() default {};

    /**
     * Optional references for the target element.
     * <p>
     * Any of following notations is allowed:
     * </p>
     * <ul>
     * <li> <code>https://...</code> </li>
     * <li> <code>page-title&#64;https://...</code> </li>
     * </ul>
     * @return optional references for the target element.
     * @see #REFERENCE_DELIMITER
     */
    String[] reference() default {};

    /**
     * An optional code number for the target element.
     * @return the element code if defined, otherwise {@link #CODE_UNSPECIFIED}
     * @see #CODE_UNSPECIFIED
     * @since 1.7.0
     */
    int code() default CODE_UNSPECIFIED;
}
