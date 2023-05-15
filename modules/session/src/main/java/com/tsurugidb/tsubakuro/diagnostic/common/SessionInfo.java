package com.tsurugidb.tsubakuro.diagnostic.common;

import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;

import com.tsurugidb.tsubakuro.common.impl.SessionImpl;

public class SessionInfo implements SessionInfoMBean {
    private WeakHashMap<SessionImpl, Void> sessions = new WeakHashMap<SessionImpl, Void>();

    public void addSession(SessionImpl s) {
        synchronized (sessions) {
            sessions.put(s, null);
        }
    }

    static class SessionInfoAction implements BiConsumer<SessionImpl, Void> {
        String diagnosticInfo = "";

        @Override
        public void accept(SessionImpl s, Void v) {
            if (Objects.nonNull(s)) {
                diagnosticInfo += s.diagnosticInfo();
            }
        }
        public String diagnosticInfo() {
            return diagnosticInfo;
        }
    }

    @Override
    public String getSessionInfo() {
        var sessionInfoAction = new SessionInfoAction();
        sessions.forEach(sessionInfoAction);
        return sessionInfoAction.diagnosticInfo();
    }
}
