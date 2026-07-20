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

import org.zoxweb.shared.util.NVConfigEntity;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-instance cache of NVConfigEntity → table metadata. Mirrors
 * {@code XlogistxMongoMetaManager}: each {@link H2PDataStore} owns its own.
 * Lookups are lock-free; writes use {@code putIfAbsent} so concurrent
 * table-creation never holds a class-wide lock during DDL.
 *
 * <p>Skeleton — fill in as table-creation / column-mapping logic comes online.
 */
public class H2PMetaManager {

    private final Map<String, NVConfigEntity> registeredTables = new ConcurrentHashMap<>();

    public H2PMetaManager() {
    }

    public boolean isRegistered(String tableName) {
        return registeredTables.containsKey(tableName.toLowerCase());
    }

    public void register(NVConfigEntity nvce) {
        if (nvce != null) {
            registeredTables.putIfAbsent(nvce.getName().toLowerCase(), nvce);
        }
    }

    public NVConfigEntity lookup(String tableName) {
        return registeredTables.get(tableName.toLowerCase());
    }

    public Set<String> getTables() {
        return registeredTables.keySet();
    }

    public void clear() {
        registeredTables.clear();
    }
}
