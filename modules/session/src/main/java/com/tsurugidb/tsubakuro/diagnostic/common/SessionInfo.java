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
package com.tsurugidb.tsubakuro.diagnostic.common;

import java.util.WeakHashMap;
import java.util.function.BiConsumer;

import com.tsurugidb.tsubakuro.common.impl.SessionImpl;

/**
 * Session information for diagnostics.
 */
public class SessionInfo implements SessionInfoMBean {
    private WeakHashMap<SessionImpl, Void> sessions = new WeakHashMap<SessionImpl, Void>();

    /**
     * Add a session.
     * @param s the session to add
     */
    public void addSession(SessionImpl s) {
        synchronized (sessions) {
            sessions.put(s, null);
        }
    }

    static class SessionInfoAction implements BiConsumer<SessionImpl, Void> {
        String diagnosticInfo = "";

        @Override
        public void accept(SessionImpl s, Void v) {
            if (s != null) {
                diagnosticInfo += s.diagnosticInfo();
            }
        }
        public String diagnosticInfo() {
            return diagnosticInfo;
        }
    }

    @Override
    public String getSessionInfo() {
        synchronized (sessions) {
            var sessionInfoAction = new SessionInfoAction();
            sessions.forEach(sessionInfoAction);
            return sessionInfoAction.diagnosticInfo();
        }
    }
}
