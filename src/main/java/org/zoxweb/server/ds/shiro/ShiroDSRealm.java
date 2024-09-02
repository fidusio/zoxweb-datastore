package org.zoxweb.server.ds.shiro;


import com.mongodb.BasicDBObject;
import io.xlogistx.shiro.DomainPrincipalCollection;
import io.xlogistx.shiro.ResourcePrincipalCollection;
import io.xlogistx.shiro.ShiroBaseRealm;
import io.xlogistx.shiro.authz.ShiroAuthorizationInfo;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.subject.PrincipalCollection;
import org.bson.types.ObjectId;
import org.zoxweb.server.ds.mongo.QueryMatchObjectId;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.security.UserIDCredentialsDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.crypto.PasswordDAO;
import org.zoxweb.shared.data.DataConst.DataParam;
import org.zoxweb.shared.data.FormInfoDAO;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.security.model.SecurityModel.PermissionToken;
import org.zoxweb.shared.security.shiro.*;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.*;
import org.zoxweb.shared.util.ResourceManager.Resource;

import java.util.*;
@Deprecated
public class ShiroDSRealm
	extends ShiroBaseRealm
	implements SetName
{

	public static final LogWrapper log = new LogWrapper(ShiroDSRealm.class);

	private volatile Set<ShiroAssociationRule> cachedSARD = null;
	
	private String userDefaultRoles = null;
	
	private APIDataStore<?> dataStore;

	public ShiroDSRealm()
	{
		if(log.isEnabled()) log.getLogger().info("started");
	}
	
	protected Set<ShiroAssociationRule> getCachedSARDs()
	{
		if (cachedSARD == null)
		{
			synchronized(this)
			{
				if (cachedSARD == null)
				{
					cachedSARD = new LinkedHashSet<ShiroAssociationRule>();
					if(userDefaultRoles != null)
					{
						ShiroRole role = lookupRole(userDefaultRoles);
						if(log.isEnabled()) log.getLogger().info("role:" + role);
					
						if (role != null)
						{
							for (NVEntity nve : role.getPermissions().values())
							{
								if(log.isEnabled()) log.getLogger().info(""+nve);
							}
							ShiroAssociationRule toAdd = new ShiroAssociationRule();
							toAdd.setAssociation(role);
							toAdd.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
							toAdd.setReferenceID(UUID.randomUUID().toString());
							cachedSARD.add(toAdd);	
						}
					}
				}
				if(log.isEnabled()) log.getLogger().info("cachedSARD size:" + cachedSARD.size());
			}
		}
		return cachedSARD;
	}

	
	
	public APIDataStore<?> getAPIDataStore()
	{
		return dataStore != null ? dataStore : ResourceManager.lookupResource(Resource.DATA_STORE);
	}

	public void setDataStore(APIDataStore<?> dataStore)
	{
		this.dataStore = dataStore;
	}
	
	
	
	
	@Override
	protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) 
	{
		if(log.isEnabled()) log.getLogger().info("Getting: " + principals);
       //null usernames are invalid
       if (principals == null) 
       {
           throw new AuthorizationException("PrincipalCollection method argument cannot be null.");
       }
       
      
     

       if (principals instanceof DomainPrincipalCollection)
	   {
    	   
	        //String userName = (String) getAvailablePrincipal(principals);
	        String domainID  = ((DomainPrincipalCollection) principals).getDomainID();
	        String userID = ((DomainPrincipalCollection) principals).getUserID();
	       
	        ShiroAuthorizationInfo  info = new ShiroAuthorizationInfo(this);
	       
	        
	        if (isPermissionsLookupEnabled()) 
	        {
	        
	        	List<ShiroAssociationRule> rules = getUserShiroAssociationRule(domainID, userID);
	        	if(log.isEnabled()) log.getLogger().info("++-+-+-+-++-+-+++-+-+-rules:" + rules.size());
//	        	for(ShiroAssociationRule rule : rules)
//	        	{
//	        		if(log.isEnabled()) log.getLogger().info("" + rule.getAssociationType());
//	        	}
	        	info.addShiroAssociationRule(rules);
	        	info.addShiroAssociationRule(getCachedSARDs(), 
	        								 new NVPair(SecurityModel.TOK_USER_ID, ((DomainPrincipalCollection) principals).getUserID()),
	        								 new NVPair(SecurityModel.TOK_RESOURCE_ID, "*"));
	        	
	        	
	        }
	        return info;
	   }
       else if (principals instanceof ResourcePrincipalCollection)
       {
    	   String refID = (String) principals.getPrimaryPrincipal();
    	   ShiroAuthorizationInfo  info = new ShiroAuthorizationInfo(this);
    	   if (isPermissionsLookupEnabled()) 
	        {
	        	List<ShiroAssociationRule> rules = getUserShiroAssociationRule(null, refID);
	        	if(log.isEnabled()) log.getLogger().info("Resource rules:" + rules.size());
//	        	for(ShiroAssociationRule rule : rules)
//	        	{
//	        		if(log.isEnabled()) log.getLogger().info("" + rule.getAssociationType());
//	        	}
	        	info.addShiroAssociationRule(rules, new NVPair(PermissionToken.RESOURCE_ID.getValue(), refID));
	        }
	        return info;
    	   
       }
       
       throw new AuthorizationException("Not a domain info");
	}
	
	protected List<ShiroAssociationRule> getUserShiroAssociationRule(String domainID, String resourceID)
	{
		List<ShiroAssociationRule> sardList = search(new QueryMatchString(RelationalOperator.EQUAL, resourceID, ShiroAssociationRule.Param.ASSOCIATED_TO));
		return sardList;
	}
	
	
	
	/**
	 * @see org.apache.shiro.realm.AuthenticatingRealm#doGetAuthenticationInfo(org.apache.shiro.authc.AuthenticationToken)
	 */
//	@Override
//	protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token)
//			throws AuthenticationException
//	{
//		if(log.isEnabled()) log.getLogger().info("AuthenticationToken:" + token);
//		
//		if (token instanceof DomainUsernamePasswordToken)
//		{
//			if(log.isEnabled()) log.getLogger().info("DomainUsernamePasswordToken based authentication");
//			DomainUsernamePasswordToken upToken = (DomainUsernamePasswordToken) token;
//	        //String userName = upToken.getUsername();
//	        //String domainID = upToken.getDomainID();
//	        if (upToken.getUsername() == null)
//	        {
//	            throw new AccountException("Null usernames are not allowed by this realm.");
//	        }
//	        UserIDDAO userIDDAO = lookupUserID(upToken.getUsername(), "_id", "_user_id");
//	        if (userIDDAO == null)
//	        {
//	            throw new AccountException("Account not found usernames are not allowed by this realm.");
//	        }
//	        upToken.setUserID(userIDDAO.getUserID());
//	        // String userID = upToken.getUserID();
//	        if(log.isEnabled()) log.getLogger().info( upToken.getUsername() +":"+upToken.getUserID());
//	        // Null username is invalid
//	        
//	        PasswordDAO password = getUserPassword(null, upToken.getUsername());
//	        if (password == null)
//	        {
//	        	throw new UnknownAccountException("No account found for user [" + upToken.getUserID() + "]");
//	        }
//
//	        return new DomainAuthenticationInfo(upToken.getUsername(), upToken.getUserID(), password, getName(), upToken.getDomainID(), upToken.getAppID(), null);
//	    }
//		else if (token instanceof JWTAuthenticationToken)
//		{
//			if(log.isEnabled()) log.getLogger().info("JWTAuthenticationToken based authentication");
//			// lookup AppDeviceDAO or SubjectAPIKey
//			// in oder to do that we need to switch the user to SUPER_ADMIN or DAEMON user
//			JWTAuthenticationToken jwtAuthToken = (JWTAuthenticationToken) token;
//			SubjectSwap ss = null;
//			try
//			{
//				APISecurityManager<Subject> sm = ResourceManager.SINGLETON.lookup(Resource.API_SECURITY_MANAGER);
//				APIAppManager appManager =  ResourceManager.SINGLETON.lookup(Resource.API_APP_MANAGER);
//				
//				ss = new SubjectSwap(sm.getDaemonSubject());
//				SubjectAPIKey sak = appManager.lookupSubjectAPIKey(jwtAuthToken.getJWTSubjectID(), false);
//				if (sak == null)
//					throw new UnknownAccountException("No account found for user [" + jwtAuthToken.getJWTSubjectID() + "]");
//				UserIDDAO userIDDAO = lookupUserID(sak.getUserID(), "_id", "_user_id", "primary_email");
//			    if (userIDDAO == null)
//			    {
//			        throw new AccountException("Account not found usernames are not allowed by this realm.");
//			    }
//			    
//			    // set the actual user 
//			    jwtAuthToken.setSubjectID(userIDDAO.getSubjectID());
//			    
//			    String domainID = jwtAuthToken.getDomainID();
//			    String appID    = jwtAuthToken.getAppID();
//			    if (sak instanceof AppDeviceDAO)
//			    {
//			    	domainID = ((AppDeviceDAO) sak).getDomainID();
//				    appID    = ((AppDeviceDAO) sak).getAppID();
//			    }
//			    
//			    DomainAuthenticationInfo ret =  new DomainAuthenticationInfo(jwtAuthToken.getSubjectID(), sak.getUserID(), sak.getAPISecretAsBytes(), getName(), domainID, appID, jwtAuthToken.getJWTSubjectID());
//			    
//			    return ret;
//			}
//			catch(Exception e)
//			{
//				e.printStackTrace();
//			}
//			finally
//			{
//				IOUtil.close(ss);
//			}
//			
//			
//		}
//		 throw new AuthenticationException("Invalid Authentication Token");
//	}



	public void addShiroRule(ShiroAssociationRule sard)
	{
		SharedUtil.checkIfNulls("Association parameters can't be null", sard, sard.getName(), sard.getAssociationType(), sard.getAssociatedTo()/*, sard.getAssociate()*/);
		List<QueryMarker> queryCriteria = null;
		List<ShiroAssociationRule> list = null;
		switch(sard.getAssociationType())
		{
		case PERMISSION_TO_SUBJECT:
			queryCriteria = new ArrayList<QueryMarker>();
			if (sard.getAssociate() != null)
				queryCriteria.add(new QueryMatchObjectId(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRule.Param.ASSOCIATE));
			
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRule.Param.ASSOCIATED_TO));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getAssociationType(), ShiroAssociationRule.Param.ASSOCIATION_TYPE));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getCRUD(), ShiroAssociationRule.Param.ASSOCIATION_CRUD));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getName(), DataParam.NAME));
			
			//System.out.println(queryCriteria);
			list = search(queryCriteria.toArray(new QueryMarker[0]));
			if (list.size() == 0 && sard.getReferenceID() == null)
			{
				getAPIDataStore().insert(sard);
			}
			// if a user was granted access and he currently logged in we 
			// must update his permissions and roles
			break;
		case PERMISSION_TO_ROLE:
			throw new IllegalArgumentException(sard.getAssociationType() + " not supported yet");
			
		case ROLEGROUP_TO_SUBJECT:
			break;
			
		case ROLE_TO_ROLEGROUP:
			throw new IllegalArgumentException(sard.getAssociationType() + " not supported yet");
			
		case ROLE_TO_SUBJECT:
		case ROLE_TO_RESOURCE:
			// assign a role to a subject
			// associated_to must be user_id or user_info_dao referenceid
			// associate should be ShiroRole
			
			
//			queryCriteria = new ArrayList<QueryMarker>();
//			if (sard.getAssociate() != null)
//				queryCriteria.add(new QueryMatchObjectId(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRule.Param.ASSOCIATE));
//			
//			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRule.Param.ASSOCIATED_TO));
//			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getAssociationType(), ShiroAssociationRule.Param.ASSOCIATION_TYPE));
//			
//			
//			list = search(queryCriteria.toArray(new QueryMarker[0]));
//			if (list.size() == 0 && sard.getReferenceID() == null)
			{
				ShiroRole role = lookupRole(sard.getAssociate());
				if(log.isEnabled()) log.getLogger().info("Role:"+role);
				if (role != null)
				{
					// maybe check role permission
					sard.setAssociation(role);
					List<ShiroAssociationRule> roleSard = search(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRule.Param.ASSOCIATE),
						   new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRule.Param.ASSOCIATED_TO),
						   new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getAssociationType(), ShiroAssociationRule.Param.ASSOCIATION_TYPE));
					if(log.isEnabled()) log.getLogger().info("roleSard:" + roleSard);
					if (roleSard == null || roleSard.size() == 0)
					{
						getAPIDataStore().insert(sard);
					}
					
				}
			}
			// check if the role exist
			//getDataStore().search(ShiroRole.NVC_SHIRO_ROLE_DAO, null, queryCriteria);
			
			
			break;
		case PERMISSION_TO_RESOURCE:
			break;
		
		
		}
	}
	
	ShiroAssociationRule lookupSARD(ShiroAssociationRule sard)
	{
		List<ShiroAssociationRule> matches = null;
		ArrayList<QueryMarker> queryCriteria = null;
		switch(sard.getAssociationType())
		{
		case PERMISSION_TO_RESOURCE:
			queryCriteria = new ArrayList<QueryMarker>();
			if (sard.getAssociate() != null)
				queryCriteria.add(new QueryMatchObjectId(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRule.Param.ASSOCIATE));
			
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRule.Param.ASSOCIATED_TO));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getAssociationType(), ShiroAssociationRule.Param.ASSOCIATION_TYPE));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getCRUD(), ShiroAssociationRule.Param.ASSOCIATION_CRUD));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getName(), DataParam.NAME));
			matches = search(queryCriteria.toArray(new QueryMarker[0]));
			break;
		case PERMISSION_TO_ROLE:
			break;
		case PERMISSION_TO_SUBJECT:
			break;
		case ROLEGROUP_TO_SUBJECT:
			break;
		
		case ROLE_TO_ROLEGROUP:
			break;
		case ROLE_TO_SUBJECT:
		case ROLE_TO_RESOURCE:
			matches = search(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRule.Param.ASSOCIATE),
					   new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRule.Param.ASSOCIATED_TO),
					   new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getAssociationType(), ShiroAssociationRule.Param.ASSOCIATION_TYPE));
			break;
		default:
			break;
			
		}
		
		
		if (matches != null && matches.size() == 1)
		{
			return matches.get(0);
		}
		
		return null;
		
		
	}


	public void deleteShiroRule(ShiroAssociationRule sard)
	{
		if (sard.getReferenceID() != null)
		{
			getAPIDataStore().delete(sard, false);
		}
		else
		{
			ShiroAssociationRule match = lookupSARD(sard);
			if(log.isEnabled()) log.getLogger().info("Match:" + match);
			if (match != null)
			{
				getAPIDataStore().delete(match, false);
			}
				
		}
		
	}


	public void updateShiroRule(ShiroAssociationRule sard)
	{
		// TODO Auto-generated method stub
		
	}


	public List<ShiroAssociationRule> search(QueryMarker... queryCriteria)
	{
		if (queryCriteria == null || queryCriteria.length == 0)
		{
			throw new NullPointerException("null or empty search parameters");
			
		}
		
		return getAPIDataStore().search(ShiroAssociationRule.NVC_SHIRO_ASSOCIATION_RULE, null, queryCriteria);
	}
	
	public List<ShiroAssociationRule> search(Collection<QueryMarker> queryCriteria)
	{
		if (queryCriteria == null || queryCriteria.size() == 0)
		{
			throw new NullPointerException("null or empty search parameters");
		}
		
		return getAPIDataStore().search(ShiroAssociationRule.NVC_SHIRO_ASSOCIATION_RULE, null, queryCriteria.toArray(new QueryMarker[queryCriteria.size()]));
	}



	/**
	 * Add a subject
	 *
	 * @param subject
	 * @return ShiroSubjectDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public SubjectIdentifier addSubject(SubjectIdentifier subject) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Delete a subject
	 *
	 * @param subject
	 * @param withRoles
	 * @return ShiroSubjectDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public SubjectIdentifier deleteSubject(SubjectIdentifier subject, boolean withRoles) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Updates a subject, usually the password.
	 *
	 * @param subject
	 * @return ShiroSubjectDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public SubjectIdentifier updateSubject(SubjectIdentifier subject) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Add a role
	 *
	 * @param role
	 * @return ShiroRoleDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroRole addRole(ShiroRole role) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Delete a role.
	 *
	 * @param role
	 * @param withPermissions
	 * @return ShiroRoleDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroRole deleteRole(ShiroRole role, boolean withPermissions) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Updates a role
	 *
	 * @param role
	 * @return ShiroRoleDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroRole updateRole(ShiroRole role) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Delete a shiro role
	 *
	 * @param shiroRole to be deleted
	 * @return the deleted shiro role
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public ShiroRole deleteRole(ShiroRole shiroRole) throws AccessSecurityException {
		return null;
	}

	/**
	 * Adds a role group.
	 *
	 * @param rolegroup
	 * @return ShiroRoleGroupDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroRoleGroup addRoleGroup(ShiroRoleGroup rolegroup) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Delete a role group.
	 *
	 * @param rolegroup
	 * @return ShiroRoleGroupDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroRoleGroup deleteRoleGroup(ShiroRoleGroup rolegroup) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Add a shiro authorization info
	 *
	 * @param shiroAuthzInfo to added
	 * @return the added shiro authorization info
	 * @throws AccessSecurityException if no permitted
	 */
	@Override
	public ShiroAuthzInfo addShiroAuthzInfo(ShiroAuthzInfo shiroAuthzInfo) throws AccessSecurityException {
		return null;
	}

	@Override
	public Set<ShiroAuthzInfo> lookupSubjectAuthzInfo(String subjectIdentifier) throws AccessSecurityException {
		return null;
	}

	/**
	 * Update a shiro authorization info
	 *
	 * @param shiroAuthzInfo to updated
	 * @return the updated shiro authorization info
	 * @throws AccessSecurityException if no permitted
	 */
	@Override
	public ShiroAuthzInfo updateShiroAuthzInfo(ShiroAuthzInfo shiroAuthzInfo) throws AccessSecurityException {
		return null;
	}

	/**
	 * Delete a shiro authorization info
	 *
	 * @param shiroAuthzInfo to delted
	 * @return the deleted shiro authorization info
	 * @throws AccessSecurityException if no permitted
	 */
	@Override
	public ShiroAuthzInfo deleteShiroAuthzInfo(ShiroAuthzInfo shiroAuthzInfo) throws AccessSecurityException {
		return null;
	}

	/**
	 * @return the key maker associated with shiro realm controller
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public KeyMaker getKeyMaker() throws AccessSecurityException {
		return null;
	}

	/**
	 * @param keyMaker to be set for the shiro realm controller
	 * @throws AccessSecurityException if no permitted
	 */
	@Override
	public void setKeyMaker(KeyMaker keyMaker) throws AccessSecurityException {

	}

	/**
	 * Update a role group.
	 *
	 * @param rolegroup
	 * @return ShiroRoleGroupDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroRoleGroup updateRoleGroup(ShiroRoleGroup rolegroup) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Create a subject identifier
	 *
	 * @param subjectID   the email or uuid identifier of the subject
	 * @param subjectType the type of the subject
	 * @param credential  subject credentials
	 * @return the created subject identifier
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public SubjectIdentifier addSubjectIdentifier(String subjectID, BaseSubjectID.SubjectType subjectType, CredentialInfo credential) throws AccessSecurityException {
		return null;
	}

	/**
	 * Create a subject identifier
	 *
	 * @param subjectID   the email or uuid identifier of the subject
	 * @param subjectType the type of the subject
	 * @return the created subject identifier
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public SubjectIdentifier addSubjectIdentifier(String subjectID, BaseSubjectID.SubjectType subjectType) throws AccessSecurityException {
		return null;
	}

	/**
	 * Create a subject identifier
	 *
	 * @param subjectIdentifier the subject identifier
	 * @return the created subject identifier
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public SubjectIdentifier addSubjectIdentifier(SubjectIdentifier subjectIdentifier) throws AccessSecurityException {
		return null;
	}

	/**
	 * Delete a user identifier use with extreme care
	 *
	 * @param subjectID to be deleted
	 * @return the deleted subject identifier
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public SubjectIdentifier deleteSubjectIdentifier(String subjectID) throws AccessSecurityException {
		return null;
	}

	/**
	 * Lookup subject credential info
	 *
	 * @param subjectID      the subject identifier
	 * @param credentialType the
	 * @return the subject credential
	 */
	@Override
	public <C> C lookupCredential(String subjectID, CredentialInfo.CredentialType credentialType) {
		return null;
	}

	/**
	 * Add a credential object for the specified subject
	 *
	 * @param subjectID than owns the credentials
	 * @param ci        the credential info object ie: password, public key, token ...
	 * @return the validated credential info object
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public CredentialInfo addCredentialInfo(String subjectID, CredentialInfo ci) throws AccessSecurityException {
		return null;
	}

	/**
	 * Add a credential object for the specified subject
	 *
	 * @param subjectID than owns the credentials
	 * @param password  the credential info object ie: password, public key, token ...
	 * @return the validated credential info object
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public CredentialInfo addCredentialInfo(String subjectID, String password) throws AccessSecurityException {
		return null;
	}

	/**
	 * Add a credential object for the specified subject
	 *
	 * @param subjectID than owns the credentials
	 * @param password  the credential info object ie: password, public key, token ...
	 * @return the validated credential info object
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public CredentialInfo addCredentialInfo(String subjectID, byte[] password) throws AccessSecurityException {
		return null;
	}

	/**
	 * Delete a credential info
	 *
	 * @param ci to be deleted
	 * @return the deleted credential info
	 * @throws AccessSecurityException if not permitted
	 */
	@Override
	public CredentialInfo deleteCredentialInfo(CredentialInfo ci) throws AccessSecurityException {
		return null;
	}

	@Override
	public CredentialInfo updateCredentialInfo(CredentialInfo oldCI, CredentialInfo newCI) throws AccessSecurityException {
		return null;
	}

	/**
	 * Add a permission
	 *
	 * @param permission
	 * @return ShiroPermissionDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroPermission addPermission(ShiroPermission permission) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Delete a permission
	 *
	 * @param permission
	 * @return ShiroPermissionDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroPermission deletePermission(ShiroPermission permission) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}

	/**
	 * Updates a permission.
	 *
	 * @param permission
	 * @return ShiroPermissionDAO
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws AccessException
	 */
	@Override
	public ShiroPermission updatePermission(ShiroPermission permission) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}


	public PasswordDAO getSubjectPassword(String domainID, String userID)
	{
		UserIDCredentialsDAO uicd = lookupUserIDCredentials(userID);
		return uicd != null ? uicd.getPassword() : null;
	}


	public PasswordDAO setSubjectPassword(SubjectIdentifier subject, PasswordDAO passwd) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}


	public PasswordDAO setSubjectPassword(String subject, PasswordDAO passwd) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}


	public PasswordDAO setSubjectPassword(SubjectIdentifier subject, String passwd) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}


	public PasswordDAO setSubjectPassword(String subject, String passwd) throws NullPointerException, IllegalArgumentException, AccessException {
		return null;
	}


	public Set<String> getSubjectRoles(String domainID, String userID) {
		// TODO Auto-generated method stub
		return  new HashSet<String>();
	}

	@Override
	public Set<String> getSubjectPermissions(String domainID, String userID, Set<String> roleNames) {
		// TODO Auto-generated method stub
		return new HashSet<String>();
	}



//	public void createUser(UserIDDAO userID, UserStatus userIDstatus, String password)
//			throws NullPointerException, IllegalArgumentException, AccessException, APIException
//	{
//		SharedUtil.checkIfNulls("UserIDDAO object is null.", userID, userIDstatus);
//		password = FilterType.PASSWORD.validate(password);
//		
//		
//		if (lookupUserID(userID.getSubjectID()) != null)
//		{
//			throw new APIException("User already exist");
//		}
//			
//		if(log.isEnabled()) log.getLogger().info("User Name: " + userID.getPrimaryEmail());
//		if(log.isEnabled()) log.getLogger().info("First Name: " + userID.getUserInfo().getFirstName());
//		if(log.isEnabled()) log.getLogger().info("Middle Name: " + userID.getUserInfo().getMiddleName());
//		if(log.isEnabled()) log.getLogger().info("Last Name: " + userID.getUserInfo().getLastName());
//		if(log.isEnabled()) log.getLogger().info("Birthday: " + userID.getUserInfo().getDOB());
//		
//		userID.setReferenceID(null);
//		SharedUtil.validate(userID, true, true);
//		
//		
//		
//
//		
//			
//		
//		// special case to avoid chicken and egg situation
//		ObjectId objID = ObjectId.get();
//		String userIDRef = objID.toHexString();
//		apiSecurityManager.associateNVEntityToSubjectUserID(userID, userIDRef);
//		userID.setReferenceID(userIDRef);
//		userID.getUserInfo().setReferenceID(userIDRef);
//		////////////////////////
//		
//		try
//		{
//			// insert the user_info dao first
//			dataStore.insert(userID.getUserInfo());
//			
//			dataStore.insert(userID);
//			
//			UserIDCredentialsDAO userIDCredentials = new UserIDCredentialsDAO();
//			userIDCredentials.setReferenceID(userID.getReferenceID());
//			userIDCredentials.setUserID(userID.getReferenceID());
//			userIDCredentials.setLastStatusUpdateTimestamp(System.currentTimeMillis());
//			userIDCredentials.setUserStatus(userIDstatus);
//			PasswordDAO passwordDAO = CryptoUtil.hashedPassword(MDType.SHA_512, 0, 8196, password);
//			passwordDAO.setUserID(userID.getReferenceID());
//			userIDCredentials.setPassword(passwordDAO);
//			
//			
//			
//			switch(userIDstatus)
//			{
//			case ACTIVE:
//				break;
//			case DEACTIVATED:
//				break;
//			case INACTIVE:
//				break;
//			case PENDING_ACCOUNT_ACTIVATION:
//			case PENDING_RESET_PASSWORD:
//				userIDCredentials.setPendingToken(UUID.randomUUID().toString());
//				break;
//
//			
//			}
//			
//			
//			dataStore.insert(userIDCredentials);
//			userIDCredentials.getPassword().setReferenceID(userIDCredentials.getReferenceID());
//			dataStore.update(userIDCredentials);
//			// create the user master key
//			dataStore.insert(KeyMakerProvider.SINGLETON.createUserIDKey(userID, KeyMakerProvider.SINGLETON.getMasterKey()));
//			
//			// removed for now created during login
//			// MN 2014-12-23
//			// FidusStoreDataManager.SINGLETON.setUpUserAccount(userID, dataStore, (APIDocumentStore<?>) dataStore);
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//			throw new AccessException(e.getMessage());			
//		}
//	}
//	
//	
//	public void deleteUser(String subjectID)
//		throws NullPointerException, IllegalArgumentException, AccessException, APIException
//	{
//		
//		// crutial permission check
//		// of the super admin can delete user
//		
//		SharedUtil.checkIfNulls("subjectID null", subjectID);
//		UserIDDAO userID = lookupUserID(subjectID);
//		if (userID == null)
//		{
//			throw new APIException("subjectID " + subjectID + " not found.");
//		}
//		// delete a user requires the following
//		// delete the associated UserInfoDOA, UserIDCredentialsDAO and the encrypted key dao associated with the user id
//		dataStore.delete(userID, true);
//		dataStore.delete(UserIDCredentialsDAO.NVC_USER_ID_CREDENTIALS_DAO,  new QueryMatchObjectId(RelationalOperator.EQUAL, userID.getReferenceID(), MetaToken.REFERENCE_ID));
//		dataStore.delete(EncryptedKeyDAO.NVCE_ENCRYPTED_KEY_DAO,  new QueryMatchObjectId(RelationalOperator.EQUAL, userID.getReferenceID(), MetaToken.REFERENCE_ID));
//		
//		// TODO check if a user is logged in and invalidate his current session
//		
//		
//	}
	
	
	public Set<String> getRecursiveNVEReferenceIDFromForm(String formReferenceID)
	{
		HashSet<String> ret = null;
		
		BasicDBObject projection = new BasicDBObject();
		projection = projection.append(FormInfoDAO.Param.FORM_REFERENCE.getNVConfig().getName(), true);
		
		BasicDBObject result = getAPIDataStore().lookupByReferenceID(FormInfoDAO.NVC_FORM_INFO_DAO.getName(), new ObjectId(formReferenceID), projection);
		if (result != null)
		{
			ret = new HashSet<String>();
			BasicDBObject form_reference = (BasicDBObject) result.get(FormInfoDAO.Param.FORM_REFERENCE.getNVConfig().getName());
			if (form_reference != null)
			{
				

				
				
				form_reference = getNVERefFromReference(form_reference);
				
				recursiveSearch(form_reference, ret);
				

				
			}
		}
		
		
		return ret;
	}
	
	private  BasicDBObject getNVERefFromReference(BasicDBObject bsonObj)
	{
		ObjectId reference_id = (ObjectId) bsonObj.get(MetaToken.REFERENCE_ID.getName());
		ObjectId canonical_id  = (ObjectId) bsonObj.get(MetaToken.CANONICAL_ID.getName());
		if (reference_id != null && canonical_id != null)
		{
			BasicDBObject nvcEntry = getAPIDataStore().lookupByReferenceID("nv_config_entities", canonical_id);
			if (nvcEntry != null && canonical_id != null)
			{
				String nvcSubName = nvcEntry.getString(MetaToken.CANONICAL_ID.getName());
				if(nvcSubName != null)
				{
					return getAPIDataStore().lookupByReferenceID(nvcSubName, reference_id);
				}
			}
		}
		
		return null;
	}
	
	
	private Set<String> recursiveSearch(BasicDBObject bsonObj, Set<String>  result)
	{
		if (result == null)
		{
			result = new HashSet<String>();
		}
		
		if (bsonObj != null)
		{
			//System.out.println(bsonObj);
			
			ObjectId main = bsonObj.getObjectId("_id");
			if (main != null)
				result.add(main.toHexString());
			for (Object val : bsonObj.values())
			{
				if (val instanceof BasicDBObject)
				{
					recursiveSearch(getNVERefFromReference((BasicDBObject) val), result);
				}
				if (val instanceof List)
				{
					for (Object listObj : ((List<?>)val))
					{
						if (listObj instanceof BasicDBObject)
						{
							recursiveSearch(getNVERefFromReference((BasicDBObject) listObj), result);
						}
						else
						{
							break;
						}
					}
				}
			}
		}
		
		
		return result;
	}
	
	
	public  UserIDCredentialsDAO lookupUserIDCredentials(String user)
			throws NullPointerException, IllegalArgumentException, AccessException
	{
		UserIDDAO userIDDAO = lookupUserID( user, "_id");
		
		if (userIDDAO != null)
		{
			//APIDataStore<?> dataStore = FidusStoreAPIManager.SINGLETON.lookupAPIDataStore(FidusStoreAPIManager.FIDUS_STORE_NAME);
			List<UserIDCredentialsDAO> listOfUserIDCredentialsDAO = getAPIDataStore().searchByID(UserIDCredentialsDAO.NVC_USER_ID_CREDENTIALS_DAO, userIDDAO.getReferenceID());
			
			if (listOfUserIDCredentialsDAO != null && listOfUserIDCredentialsDAO.size() == 1)
			{
				return listOfUserIDCredentialsDAO.get(0);
			}
			
		}
		
		throw new AccessException("User credentials not found.");
	}



	public String getUserDefaultRoles() {
		return userDefaultRoles;
	}



	public void setUserDefaultRoles(String userDefaultRoles) {
		this.userDefaultRoles = userDefaultRoles;
	}



	
	
	

}
