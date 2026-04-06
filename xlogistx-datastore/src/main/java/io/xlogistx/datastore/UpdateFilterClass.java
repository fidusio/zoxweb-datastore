/*
 * Copyright (c) 2012-2017 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.xlogistx.datastore;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APICredentialsDAO;
import org.zoxweb.shared.filters.ValueFilter;
import org.zoxweb.shared.util.NVEntity;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Filter controlling whether a given class is allowed to be updated via the datastore.
 * <p>
 * The blacklist is mutable at runtime ({@link #addExcluded(Class)} / {@link #removeExcluded(Class)})
 * and is backed by a concurrent set, so reads are lock-free.
 * <p>
 * The Shiro "management override" is only consulted when the class is actually blacklisted —
 * non-blacklisted classes short-circuit to {@code true} without ever calling {@link SecurityUtils}.
 */
public class UpdateFilterClass
        implements ValueFilter<Class<?>, Boolean> {
    public static final UpdateFilterClass SINGLETON = new UpdateFilterClass();

    /** Principal that is allowed to override the blacklist. */
    public static final String MANAGEMENT_PRINCIPAL = "management@zoxweb.com";

    private final Set<Class<?>> excluded = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public UpdateFilterClass() {
        excluded.add(APIConfigInfoDAO.class);
        // API tokens must not be updatable during token generation/refresh.
        excluded.add(APICredentialsDAO.class);
    }

    /** Add a class to the blacklist. Thread-safe. */
    public void addExcluded(Class<?> clazz) {
        if (clazz != null) excluded.add(clazz);
    }

    /** Remove a class from the blacklist. Thread-safe. */
    public void removeExcluded(Class<?> clazz) {
        if (clazz != null) excluded.remove(clazz);
    }

    public Set<Class<?>> getExcluded() {
        return Collections.unmodifiableSet(excluded);
    }

    @Override
    public String toCanonicalID() {
        return null;
    }

    @Override
    public Boolean validate(Class<?> in)
            throws NullPointerException, IllegalArgumentException {
        return isValid(in);
    }

    @Override
    public boolean isValid(Class<?> in) {
        // Fast path: not blacklisted → always valid. No Shiro call needed.
        if (!excluded.contains(in)) {
            return true;
        }

        // Slow path: blacklisted → the management principal can still override.
        try {
            Subject subject = SecurityUtils.getSubject();
            if (subject.isAuthenticated() && MANAGEMENT_PRINCIPAL.equals(subject.getPrincipal())) {
                return true;
            }
        } catch (Exception e) {
            // No security manager configured, or subject resolution failed — fall through.
        }

        return false;
    }

    public boolean isValid(NVEntity in) {
        if (in != null) {
            return isValid(in.getClass());
        }

        return false;
    }

}
