package org.zoxweb.server.ds.shiro;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.logging.Logger;

import org.apache.shiro.authc.AccountException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.bson.types.ObjectId;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.server.ds.mongo.QueryMatchObjectId;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.UserIDCredentialsDAO;

import org.zoxweb.server.security.shiro.ShiroBaseRealm;
import org.zoxweb.server.security.shiro.SubjectSwap;
import org.zoxweb.server.security.shiro.authc.DomainAuthenticationInfo;
import org.zoxweb.server.security.shiro.authc.DomainPrincipalCollection;
import org.zoxweb.server.security.shiro.authc.DomainUsernamePasswordToken;
import org.zoxweb.server.security.shiro.authc.JWTAuthenticationToken;
import org.zoxweb.shared.api.APIAppManager;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APISecurityManager;

import org.zoxweb.shared.crypto.PasswordDAO;
import org.zoxweb.shared.data.AppDeviceDAO;
import org.zoxweb.shared.data.DataConst.DataParam;
import org.zoxweb.shared.data.FormInfoDAO;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.db.QueryMarker;

import org.zoxweb.shared.db.QueryMatchString;

import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.security.SubjectAPIKey;
import org.zoxweb.shared.security.shiro.ShiroAssociationRuleDAO;
import org.zoxweb.shared.security.shiro.ShiroRoleDAO;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.GetValue;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;
import org.zoxweb.shared.util.SetName;

import org.zoxweb.shared.util.SharedUtil;

import com.mongodb.BasicDBObject;

public class ShiroDSRealm
	extends ShiroBaseRealm
	implements SetName
{

	private static final transient Logger log = Logger.getLogger(ShiroDSRealm.class.getName());
	
	private APIDataStore<?> dataStore;
	private APISecurityManager<Subject> apiSecurityManager;
	
	
	public ShiroDSRealm()
	{
		
	}

	public APISecurityManager<Subject> getAPISecurityManager() {
		return apiSecurityManager != null ? apiSecurityManager :  ResourceManager.SINGLETON.lookup(Resource.API_SECURITY_MANAGER);
	}

	public void setAPISecurityManager(APISecurityManager<Subject> apiSecurityManager) {
		this.apiSecurityManager = apiSecurityManager;
	}
	
	public APIDataStore<?> getDataStore()
	{
		return dataStore != null ? dataStore : ResourceManager.SINGLETON.lookup(Resource.DATA_STORE);
	}

	public void setDataStore(APIDataStore<?> dataStore)
	{
		this.dataStore = dataStore;
	}
	
	
	
	
	@Override
	protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) 
	{
		
       //null usernames are invalid
       if (principals == null) 
       {
           throw new AuthorizationException("PrincipalCollection method argument cannot be null.");
       }
       
       log.info("PrincipalCollection class:" + principals.getClass());
     

       if (principals instanceof DomainPrincipalCollection )
	   {
	        //String userName = (String) getAvailablePrincipal(principals);
	        String domainID  = ((DomainPrincipalCollection) principals).getDomainID();
	        String userID = ((DomainPrincipalCollection) principals).getUserID();
	       
	        DSAuthorizationInfo  info = new DSAuthorizationInfo(this);
	        
	        if (isPermissionsLookupEnabled()) 
	        {
	        	List<ShiroAssociationRuleDAO> rules = getUserShiroAssociationRule(domainID, userID);
	        	log.info("rules:" + rules.size());
	        	info.addShiroAssociationRule(rules);
	        }
	        //System.out.println("PrincipalCollection:" +principals);
	        return info;
	   }
       
       throw new AuthorizationException("Not a domain info");
	}
	
	protected List<ShiroAssociationRuleDAO> getUserShiroAssociationRule(String domainID, String userID)
	{
		List<ShiroAssociationRuleDAO> sardList = search(new QueryMatchString(RelationalOperator.EQUAL, userID, ShiroAssociationRuleDAO.Param.ASSOCIATED_TO));
		return sardList;
	}
	
	/**
	 * @see org.apache.shiro.realm.AuthenticatingRealm#doGetAuthenticationInfo(org.apache.shiro.authc.AuthenticationToken)
	 */
	@Override
	protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token)
			throws AuthenticationException
	{
		log.info("AuthenticationToken:" + token);
		
		if (token instanceof DomainUsernamePasswordToken)
		{
			log.info("DomainUsernamePasswordToken based authentication");
			DomainUsernamePasswordToken upToken = (DomainUsernamePasswordToken) token;
	        String userName = upToken.getUsername();
	        String domainID = upToken.getDomainID();
	        if (userName == null)
	        {
	            throw new AccountException("Null usernames are not allowed by this realm.");
	        }
	        UserIDDAO userIDDAO = lookupUserID(userName, "_id", "_user_id");
	        if (userIDDAO == null)
	        {
	            throw new AccountException("Account not found usernames are not allowed by this realm.");
	        }
	        upToken.setUserID(userIDDAO.getUserID());
	        // String userID = upToken.getUserID();
	        log.info( domainID +":"+upToken.getUserID());
	        // Null username is invalid
	        
	        PasswordDAO password = getUserPassword(domainID, userName);
	        if (password == null)
	        {
	        	throw new UnknownAccountException("No account found for user [" + upToken.getUserID() + "]");
	        }

	        return new DomainAuthenticationInfo(userName, upToken.getUserID(), password, getName(), domainID, upToken.getAppID());
	    }
		else if (token instanceof JWTAuthenticationToken)
		{
			log.info("JWTAuthenticationToken based authentication");
			// lookup AppDeviceDAO or SubjectAPIKey
			// in oder to do that we need to switch the user to SUPER_ADMIN or DAEMON user
			JWTAuthenticationToken jwtAuthToken = (JWTAuthenticationToken) token;
			SubjectSwap ss = null;
			try
			{
				APISecurityManager<Subject> sm = ResourceManager.SINGLETON.lookup(Resource.API_SECURITY_MANAGER);
				APIAppManager appManager =  ResourceManager.SINGLETON.lookup(Resource.API_APP_MANAGER);
				
				ss = new SubjectSwap(sm.getDaemonSubject());
				SubjectAPIKey sak = appManager.lookupSubjectAPIKey(jwtAuthToken.getJWTSubjectID(), false);
				if (sak == null)
					throw new UnknownAccountException("No account found for user [" + jwtAuthToken.getJWTSubjectID() + "]");
				UserIDDAO userIDDAO = lookupUserID(sak.getUserID(), "_id", "_user_id", "primary_email");
			    if (userIDDAO == null)
			    {
			        throw new AccountException("Account not found usernames are not allowed by this realm.");
			    }
			    
			    // set the actual user 
			    jwtAuthToken.setSubjectID(userIDDAO.getSubjectID());
			    
			    String domainID = jwtAuthToken.getDomainID();
			    String appID    = jwtAuthToken.getAppID();
			    if (sak instanceof AppDeviceDAO)
			    {
			    	domainID = ((AppDeviceDAO) sak).getDomainID();
				    appID    = ((AppDeviceDAO) sak).getAppID();
			    }
			    
			    return new DomainAuthenticationInfo(jwtAuthToken.getSubjectID(), sak.getUserID(), sak.getAPISecretAsBytes(), getName(), domainID, appID);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
				IOUtil.close(ss);
			}
			
			
		}
		 throw new AuthenticationException("Invalid Authentication Token");
	}

	
	@Override
	public void addShiroRule(ShiroAssociationRuleDAO sard) 
	{
		SharedUtil.checkIfNulls("Association parameters can't be null", sard, sard.getName(), sard.getAssociationType(), sard.getAssociatedTo()/*, sard.getAssociate()*/);
		switch(sard.getAssociationType())
		{
		case PERMISSION_TO_SUBJECT:
			List<QueryMarker> queryCriteria = new ArrayList<QueryMarker>();
			if (sard.getAssociate() != null)
				queryCriteria.add(new QueryMatchObjectId(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRuleDAO.Param.ASSOCIATE));
			
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRuleDAO.Param.ASSOCIATED_TO));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getAssociationType(), ShiroAssociationRuleDAO.Param.ASSOCIATION_TYPE));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, ""+sard.getCRUD(), ShiroAssociationRuleDAO.Param.ASSOCIATION_CRUD));
			queryCriteria.add(new QueryMatchString(RelationalOperator.EQUAL, sard.getName(), DataParam.NAME));
			
			//System.out.println(queryCriteria);
			List<ShiroAssociationRuleDAO> list = search(queryCriteria.toArray(new QueryMarker[0]));
			if (list.size() == 0 && sard.getReferenceID() == null)
			{
				getDataStore().insert(sard);
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
			// assign a role to a subject
			// associated_to must be user_id or user_info_dao referenceid
			// associate should be ShiroRoleDAO
			
			
			ShiroRoleDAO role = lookupRole(sard.getAssociate());
			if (role != null)
			{
				// maybe check role permission
				sard.setAssociation(role);
				List<ShiroAssociationRuleDAO> roleSard = search(new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociate(), ShiroAssociationRuleDAO.Param.ASSOCIATE),
					   new QueryMatchString(RelationalOperator.EQUAL, sard.getAssociatedTo(), ShiroAssociationRuleDAO.Param.ASSOCIATED_TO));
				if (roleSard == null || roleSard.size() == 0)
				{
					getDataStore().insert(sard);
				}
				
			}
			// check if the role exist
			//getDataStore().search(ShiroRoleDAO.NVC_SHIRO_ROLE_DAO, null, queryCriteria);
			
			
			break;
		}
		
		
	
		
	}

	@Override
	public void deleteShiroRule(ShiroAssociationRuleDAO sard)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateShiroRule(ShiroAssociationRuleDAO sard)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<ShiroAssociationRuleDAO> search(QueryMarker... queryCriteria)
	{
		if (queryCriteria == null || queryCriteria.length == 0)
		{
			throw new NullPointerException("null or empty search parameters");
			
		}
		
		return getDataStore().search(ShiroAssociationRuleDAO.NVC_SHIRO_ASSOCIATION_RULE_DAO, null, queryCriteria);
	}

	@Override
	public PasswordDAO getUserPassword(String domainID, String userID)
	{
		UserIDCredentialsDAO uicd = lookupUserIDCredentials(userID);
		return uicd != null ? uicd.getPassword() : null;
	}

	@Override
	protected Set<String> getUserRoles(String domainID, String userID) {
		// TODO Auto-generated method stub
		return  new HashSet<String>();
	}

	@Override
	protected Set<String> getUserPermissions(String domainID, String userID, Set<String> roleNames) {
		// TODO Auto-generated method stub
		return new HashSet<String>();
	}
	
	
	
	public  UserIDDAO lookupUserID(String subjectID, String ...params)
			throws NullPointerException, IllegalArgumentException, AccessException, APIException
	{
		
		return APIAppManagerProvider.lookupUserID(getDataStore(), subjectID, params);
//		SharedUtil.checkIfNulls("subjectID null", subjectID);
//		QueryMatch<?> query = null;
//		if (FilterType.EMAIL.isValid(subjectID))
//		{
//			// if we have an email
//			query = new QueryMatch<String>(RelationalOperator.EQUAL, subjectID, UserIDDAO.Param.PRIMARY_EMAIL.getNVConfig());
//		}
//		else
//		{
//			query = new QueryMatchObjectId(RelationalOperator.EQUAL, subjectID, MetaToken.REFERENCE_ID);//"_id", new BasicDBObject("$in", listOfObjectID)
//		}
//	
//		ArrayList<String> listParams = null;
//		if (params != null && params.length > 0)
//		{
//			listParams = new ArrayList<String>();
//			for (String str : params)
//			{
//				if (!SharedStringUtil.isEmpty(str))
//				{
//					listParams.add(str);
//				}
//			}
//		}
//		
//		List<UserIDDAO> listOfUserIDDAO = getDataStore().search(UserIDDAO.NVC_USER_ID_DAO, listParams, query);
//		
//		if (listOfUserIDDAO == null || listOfUserIDDAO.size() != 1)
//		{
//			return null;
//		}
//		
//		return listOfUserIDDAO.get(0);

	}
	
	public  UserIDDAO lookupUserID(GetValue<String> subjectID, String ...params)
			throws NullPointerException, IllegalArgumentException, AccessException
	{
		SharedUtil.checkIfNulls("DB or user ID null", getDataStore(), subjectID);
		return lookupUserID(subjectID.getValue(), params);
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
//		log.info("User Name: " + userID.getPrimaryEmail());
//		log.info("First Name: " + userID.getUserInfo().getFirstName());
//		log.info("Middle Name: " + userID.getUserInfo().getMiddleName());
//		log.info("Last Name: " + userID.getUserInfo().getLastName());
//		log.info("Birthday: " + userID.getUserInfo().getDOB());
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
	
	
	public Set<String> getRecusiveNVEReferenceIDFromForm(String formReferenceID)
	{
		HashSet<String> ret = null;
		
		BasicDBObject projection = new BasicDBObject();
		projection = projection.append(FormInfoDAO.Param.FORM_REFERENCE.getNVConfig().getName(), true);
		
		BasicDBObject result = getDataStore().lookupByReferenceID(FormInfoDAO.NVC_FORM_INFO_DAO.getName(), new ObjectId(formReferenceID), projection);
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
			BasicDBObject nvcEntry = getDataStore().lookupByReferenceID("nv_config_entities", canonical_id);
			if (nvcEntry != null && canonical_id != null)
			{
				String nvcSubName = nvcEntry.getString(MetaToken.CANONICAL_ID.getName());
				if(nvcSubName != null)
				{
					return getDataStore().lookupByReferenceID(nvcSubName, reference_id);
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
			List<UserIDCredentialsDAO> listOfUserIDCredentialsDAO = getDataStore().searchByID(UserIDCredentialsDAO.NVC_USER_ID_CREDENTIALS_DAO, userIDDAO.getReferenceID());
			
			if (listOfUserIDCredentialsDAO != null && listOfUserIDCredentialsDAO.size() == 1)
			{
				return listOfUserIDCredentialsDAO.get(0);
			}
			
		}
		
		throw new AccessException("User credentials not found.");
	}

	

}
