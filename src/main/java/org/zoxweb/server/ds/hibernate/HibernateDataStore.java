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
package org.zoxweb.server.ds.hibernate;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.shared.api.APIBatchResult;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.GetName;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.SharedUtil;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("serial")
public class HibernateDataStore
    implements APIDataStore<SessionFactory>
{

    private APIConfigInfo configInfo;
    private SessionFactory sessionFactory;
    private String name =  "HibernateDataStore";
    private String description = "Hibernate based data store";

    public static final String RESOURCE = "resource";

    public HibernateDataStore(APIConfigInfo configInfo)
    {
        this();
        setAPIConfigInfo(configInfo);
    }

    public HibernateDataStore()
    {

    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public void setDescription(String str)
    {
        this.description = str;
    }

    @Override
    public APIConfigInfo getAPIConfigInfo()
    {
        return configInfo;
    }

    @Override
    public void setAPIConfigInfo(APIConfigInfo configInfo)
    {
        this.configInfo = configInfo;
    }

    @Override
    public String toCanonicalID()
    {
        return null;
    }

    @Override
    public String getStoreName()
    {
        return null;
    }

    @Override
    public Set<String> getStoreTables()
    {
        return null;
    }

    @Override
    public SessionFactory connect()
            throws APIException
    {
        if (sessionFactory == null)
        {
            synchronized (this)
            {
                if (sessionFactory == null)
                {
                    if (configInfo == null || configInfo.getConfigParameters() == null)
                    {
                        throw new NullPointerException("Missing configuration info.");
                    }

                    String resource = SharedUtil.lookupValue(configInfo.getConfigParameters().get(RESOURCE));

                    SharedUtil.checkIfNulls("Resource (e.g. hibernate.cfg.xml) is null.", resource);

                    Configuration configuration = new Configuration();

                    try
                    {
                            sessionFactory = configuration.configure(resource).buildSessionFactory();
                    }
                    catch (HibernateException e)
                    {
                    	e.printStackTrace();
                        throw new APIException("Connect failed: " + e.getMessage());
                    }
                }
            }
        }

        return sessionFactory;
    }

    @Override
    public void close()
            throws APIException
    {
        if (sessionFactory != null)
        {
            try
            {
                sessionFactory.close();
                sessionFactory = null;
            }
            catch (HibernateException e)
            {
                e.printStackTrace();
                throw new APIException("Disconnect failed: " + e.getMessage());
            }
        }
    }

    @Override
    public SessionFactory newConnection()
            throws APIException
    {
        return null;
    }

    @Override
    public  <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public boolean isProviderActive()
    {
        return false;
    }

    @Override
    public APIExceptionHandler getAPIExceptionHandler()
    {
        return null;
    }

    @Override
    public void setAPIExceptionHandler(APIExceptionHandler exceptionHandler)
    {

    }

    @Override
    public  <V extends NVEntity> List<V> search(String className, List<String> fieldNames, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <T> T lookupProperty(GetName propertyName)
    {
        return null;
    }

    @Override
    public long lastTimeAccessed()
    {
        return 0;
    }

    @Override
    public long inactivityDuration()
    {
        return 0;
    }

    @Override
    public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public boolean isBusy()
    {
        return false;
    }

    @Override
    public <T> APISearchResult<T> batchSearch(String className, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> results, int startIndex, int batchSize)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, NVConfigEntity nvce, List<String> fieldNames, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, String className, List<String> fieldNames, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(NVConfigEntity nvce, String... ids)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(String className, String... ids)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> userSearchByID(String userID, NVConfigEntity nvce, String... ids)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

    @Override
    public <V extends NVEntity> V insert(V nve)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        SharedUtil.checkIfNulls("NVEntity is null.", nve);

        Session session = null;
        Transaction transaction = null;

        if (nve.getReferenceID() == null) 
        {
            nve.setReferenceID(UUID.randomUUID().toString());
        }
        
        if (nve.getGlobalID() == null)
        {
        	 nve.setGlobalID(UUID.randomUUID().toString());
        }

        try 
        {
        	session = connect().openSession();
            transaction = session.beginTransaction();
            session.save(nve);
            transaction.commit();
        } 
        catch (HibernateException e) 
        {
            if (transaction != null)
            {
                transaction.rollback();
            }

            throw new APIException("Insert failed: " + e.getMessage());
        } 
        finally 
        {
        	IOUtil.close(session);
        }

        return nve;
    }

    @Override
    public boolean delete(NVEntity nve, boolean withReference)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        SharedUtil.checkIfNulls("NVEntity is null.", nve);

        Session session = null; 
        Transaction transaction = null;

        try 
        {
        	session = connect().openSession();
            transaction = session.beginTransaction();
            session.delete(nve);
            transaction.commit();
        }
        catch (HibernateException e) 
        {
            if (transaction != null)
            {
                transaction.rollback();
            }
            throw new APIException("Delete failed: " + e.getMessage());
        } 
        finally 
        {
        	IOUtil.close(session);
        }

        return true;
    }

    @Override
    public boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return false;
    }

    @Override
    public <V extends NVEntity> V update(V nve)
        throws NullPointerException, IllegalArgumentException, APIException
    {
        SharedUtil.checkIfNulls("NVEntity is null.", nve);

        Session session = null;
        Transaction transaction = null;

        try 
        {
        	session = connect().openSession();
            transaction = session.beginTransaction();
            session.update(nve);
            transaction.commit();
        } 
        catch (HibernateException e) 
        {
            if (transaction != null)
            {
                transaction.rollback();
            }
            throw new APIException("Updated failed: " + e.getMessage());
        }
        finally
        {
        	IOUtil.close(session);
        }

        return nve;
    }

    @Override
    public  <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateRefOnly, boolean includeParam, String... nvConfigNames)
        throws NullPointerException, IllegalArgumentException, APIException
    {
        return null;
    }

    @Override
    public long countMatch(NVConfigEntity nvce, QueryMarker... queryCriteria)
        throws NullPointerException, IllegalArgumentException, APIException
    {
        return 0;
    }

    @SuppressWarnings("unchecked")
	@Override
    public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId)
    {
        SharedUtil.checkIfNulls("Meta type name is null.", metaTypeName);
        SharedUtil.checkIfNulls("Reference ID is null.", objectId);

        if (!(objectId instanceof String))
        {
            throw new IllegalArgumentException("Invalid reference ID class type: " + objectId.getClass() + ", expected: " + String.class);
        }

        String referenceID = (String) objectId;

        Session session = null;

        Transaction transaction = null;
        NVEntity nve;

        try
        {
        	session = connect().openSession();
            session.setDefaultReadOnly(true);
            transaction = session.beginTransaction();
            nve = (NVEntity) session.get(metaTypeName, referenceID);
            transaction.commit();
        }
        catch (HibernateException e)
        {
//            if (transaction != null) {
//                transaction.rollback();
//            }
//            e.printStackTrace();
            throw new APIException("Lookup failed: " + e.getMessage());
        } 
        finally 
        {
        	IOUtil.close(session);
        }

        return (NT) nve;
    }

    @Override
    public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT objectId, NIT projection)
    {
        return null;
    }

    @Override
    public DynamicEnumMap insertDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
        throws NullPointerException, IllegalArgumentException, APIException
    {
        return null;
    }

    @Override
    public DynamicEnumMap updateDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
        throws NullPointerException, IllegalArgumentException, APIException
    {
        return null;
    }

    @Override
    public DynamicEnumMap searchDynamicEnumMapByName(String name)
        throws NullPointerException, IllegalArgumentException, APIException
    {
        return null;
    }

    @Override
    public void deleteDynamicEnumMap(String name)
        throws NullPointerException, IllegalArgumentException, APIException
    {

    }

    @Override
    public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
        throws NullPointerException, IllegalArgumentException, AccessException, APIException
    {
        return null;
    }

}