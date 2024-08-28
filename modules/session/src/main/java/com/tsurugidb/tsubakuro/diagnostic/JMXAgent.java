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
package com.tsurugidb.tsubakuro.diagnostic;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.diagnostic.common.SessionInfo;

public final class JMXAgent {
    private static MBeanServer mbs;
    private static ObjectName name;
    private static SessionInfo mbean;

    private static AtomicBoolean created = new AtomicBoolean();

    private static void setUp() {
        try {
            mbs = ManagementFactory.getPlatformMBeanServer();
            name = new ObjectName("com.tsurugidb.tsubakuro.diagnostic.common:type=SessionInfo");
            mbean = new SessionInfo();
            mbs.registerMBean(mbean, name);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (NotCompliantMBeanException e) {
            e.printStackTrace();
        } catch (MBeanRegistrationException e) {
            e.printStackTrace();
        } catch (InstanceAlreadyExistsException e) {
            e.printStackTrace();
        }
    }

    public static synchronized SessionInfo sessionInfo() {
        if (!created.getAndSet(true)) {
            setUp();
        }
        return mbean;
    }

    private JMXAgent() {
    }
}
