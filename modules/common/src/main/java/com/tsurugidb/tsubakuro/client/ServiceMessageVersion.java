package com.tsurugidb.tsubakuro.client;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for {@link ServiceClient service clients}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ServiceMessageVersion {

    /**
     * The destination service symbolic ID.
     * @return the service name
     */
    String service();

    /**
     * The major part of service message version used by the client.
     * @return the major version
     */
    int major();

    /**
     * The minor part of service message version used by the client.
     * @return the minor version
     */
    int minor();
}
