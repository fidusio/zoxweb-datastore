package org.zoxweb.server.ds.shiro;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.zoxweb.shared.security.shiro.ShiroAssociationRuleDAO;
import org.zoxweb.shared.security.shiro.ShiroPermissionDAO;
import org.zoxweb.shared.security.shiro.ShiroRoleDAO;
import org.zoxweb.shared.util.CRUD;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.SharedUtil;


/**
 * Fidus Store implementation of shiro AuthorizationInfo
 * @author mnael
 *
 */
@SuppressWarnings("serial")
public class DSAuthorizationInfo implements AuthorizationInfo
{
	
	protected HashMap<String, ShiroAssociationRuleDAO> rulesMap = new HashMap<String, ShiroAssociationRuleDAO>();
	protected Set<ShiroAssociationRuleDAO> dynamicSet = new HashSet<ShiroAssociationRuleDAO>();
	protected Set<String> stringPermissions = null;
	protected Set<String> roles = null;
	protected Set<Permission> objectPermissions = null;
	private boolean dirty = true;
	private ShiroDSRealm realm;
	private static final transient Logger log = Logger.getLogger(DSAuthorizationInfo.class.getName());
	
	
	
//	public static String getNVEReferenceIDFromForm(FidusStoreShiroRealm realm, String formReferenceID)
//	{
//		MongoDataStore mds = (MongoDataStore) realm.getDataStore();
//		BasicDBObject projection = new BasicDBObject();
//		projection = projection.append(FormInfoDAO.Params.FORM_REFERENCE.getNVConfig().getName(), true);
//		BasicDBObject result = mds.lookupByReferenceID(FormInfoDAO.NVC_FORM_INFO_DAO.getName(), new ObjectId(formReferenceID), projection);
//		if (result != null)
//		{
//			BasicDBObject form_reference = (BasicDBObject) result.get(FormInfoDAO.Params.FORM_REFERENCE.getNVConfig().getName());
//			if (form_reference != null)
//			{
//				ObjectId nveReferenceID = (ObjectId) form_reference.get(FormInfoDAO.NVC_REFERENCE_ID.getName());
//				if (nveReferenceID != null)
//				{
//					return nveReferenceID.toHexString();
//				}
//			}
//		}
//		
//		return null;
//	}
	
	

	
	
	
	
	
	
	
	public DSAuthorizationInfo(ShiroDSRealm realm)
	{
		this.realm = realm;
	}
	
	public synchronized void addShiroAssociationRule(ShiroAssociationRuleDAO sard)
	{
		SharedUtil.checkIfNulls("Null ShiroAssociationRuleDAO", sard);
		
		Date date = sard.getExpiration();
		
		if (date != null && date.getTime() < System.currentTimeMillis())
		{
			return;
		}
		
		rulesMap.put(sard.getReferenceID(), sard);
		dirty = true;
	}
	
	
	public synchronized void addDynamicShiroAssociationRule(ShiroAssociationRuleDAO sard)
	{
		SharedUtil.checkIfNulls("Null ShiroAssociationRuleDAO", sard);
		
		Date date = sard.getExpiration();
		
		if (date != null && date.getTime() < System.currentTimeMillis())
		{
			return;
		}
		
		rulesMap.put(sard.getPattern(), sard);
		dirty = true;
	}
	
	private synchronized void update()
	{
		log.info("START:" + rulesMap.size());
		if (dirty)
		{
			if (stringPermissions == null)
			{
				 stringPermissions = new HashSet<String>();
			}
			if (roles == null)
			{
				roles = new HashSet<String>();
			}
			
			if (objectPermissions == null)
			{
				objectPermissions = new HashSet<Permission>();
			}
			stringPermissions.clear();
			roles.clear();
			objectPermissions.clear();
			Iterator<ShiroAssociationRuleDAO> it = rulesMap.values().iterator();
			while(it.hasNext())
			{
				ShiroAssociationRuleDAO sard = it.next();
				switch(sard.getAssociationType())
				{
				case PERMISSION_TO_ROLE:
					break;
				case PERMISSION_TO_SUBJECT:	
					stringPermissions.add(sard.getPattern());
					// to avoid management permissions
					if (sard.getAssociate() != null)
					{
						stringPermissions.add(SharedUtil.toCanonicalID(':', sard.getName(), CRUD.MOVE, sard.getAssociate()).toLowerCase());
						try
						{						
									
							Set<String> toAdds = realm.getRecusiveNVEReferenceIDFromForm(sard.getAssociate());
							if (toAdds != null)
							{
								//System.out.println(toAdds);
								for (String toAdd : toAdds)
								{
									stringPermissions.add(SharedUtil.toCanonicalID(':', sard.getName(), sard.getCRUD(), toAdd).toLowerCase());
									// we will automatically grant MOVE permission if a permission exist
									stringPermissions.add(SharedUtil.toCanonicalID(':', sard.getName(), CRUD.MOVE, toAdd).toLowerCase());
								}
							}
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					}
					break;
				case ROLEGROUP_TO_SUBJECT:
					break;
				case ROLE_TO_ROLEGROUP:
					break;
				case ROLE_TO_SUBJECT:
					ShiroRoleDAO role = sard.getAssociation();
					roles.add(role.getSubjectID());
					for (NVEntity nve : role.getPermissions().values())
					{
						if (nve instanceof ShiroPermissionDAO)
						{
							ShiroPermissionDAO permission = (ShiroPermissionDAO) nve;
							if (permission.getPermissionPattern() != null)
							{
								log.info("Adding permission : " + permission.getPermissionPattern());
								stringPermissions.add(permission.getPermissionPattern());
							}
						}
					}
					
					break;	
				}
			}
			dirty = false;
		}
	}
	
	
	public synchronized void addShiroAssociationRule(List<ShiroAssociationRuleDAO> sards)
	{
		SharedUtil.checkIfNulls("Null ShiroAssociationRuleDAO", sards);
		
		for(ShiroAssociationRuleDAO sard : sards)
		{
			addShiroAssociationRule(sard);
		}
	}
	
	
	public synchronized void deleteShiroAssociationRule(ShiroAssociationRuleDAO sard)
	{
		
	}
	
	public synchronized void updateShiroAssciationRule(ShiroAssociationRuleDAO sard)
	{
		
	}

	@Override
	public synchronized  Collection<String> getRoles() 
	{
		if (dirty)
		{
			update();
		}
		
		return roles;
	}

	@Override
	public synchronized Collection<String> getStringPermissions()
	{
		if (dirty)
		{
			update();
		}
		
		return stringPermissions;
	}

	@Override
	public synchronized Collection<Permission> getObjectPermissions()
	{
		if (dirty)
		{
			update();
		}
		
		return objectPermissions;
	}

}
