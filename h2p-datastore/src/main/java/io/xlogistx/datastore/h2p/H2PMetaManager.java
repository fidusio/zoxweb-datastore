/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.xlogistx.datastore.h2p;

import org.zoxweb.shared.util.DataEncoder;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.RegistrarMapDefault;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-instance registry of the tables a {@link H2PDataStore} has created or confirmed,
 * keyed case-insensitively by table name (= {@code NVConfigEntity.getName()}). Mirrors
 * {@code XlogistxMongoMetaManager}: each datastore owns its own, because the table set is
 * per-database state ({@link #getTables()} backs {@code getStoreTables()}). Registration
 * has putIfAbsent semantics — first registration of a name wins.
 */
public class H2PMetaManager {

    private final RegistrarMapDefault<String, NVConfigEntity> cache = new RegistrarMapDefault<String, NVConfigEntity>(new ConcurrentHashMap<>()).setKeyFilter(DataEncoder.StringLower);

    public H2PMetaManager() {
    }

    public boolean isRegistered(String tableName) {
        return cache.containsKey(tableName);

    }

    public void register(NVConfigEntity nvce) {
        if (nvce != null) {
            cache.lookup(nvce.getName(), k -> nvce);
        }
    }

    public NVConfigEntity lookup(String tableName) {
        return cache.lookup(tableName);
    }

    /** @return a read-only view of the registered table names. */
    public Set<String> getTables() {
        return Collections.unmodifiableSet(cache.getCacheMap().keySet());
    }

    public void clear() {
        cache.clear(false);
    }
}
