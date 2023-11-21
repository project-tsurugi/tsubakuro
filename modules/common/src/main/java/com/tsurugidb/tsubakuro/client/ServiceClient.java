package com.tsurugidb.tsubakuro.client;

/**
 * A marker interface for clients of services on tsurugidb.
 *
 * <p>
 * The followings are RECOMMENDED for each service client that inherits this:
 * </p>
 * <ul>
 * <li> Put {@link ServiceMessageVersion} annotation to the client declaration. </li>
 * <li> Add class name to the service client definition file (defined in {@link ServiceClientCollector}). </li>
 * </ul>
 * @see ServiceMessageVersion
 * @see ServiceClientCollector
 */
public interface ServiceClient {
    // no special members
}
