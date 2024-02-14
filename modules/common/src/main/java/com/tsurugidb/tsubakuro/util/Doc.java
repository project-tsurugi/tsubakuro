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
     */
    String[] reference() default {};
}
