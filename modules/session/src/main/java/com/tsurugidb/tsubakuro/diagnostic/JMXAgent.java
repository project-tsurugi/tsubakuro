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
