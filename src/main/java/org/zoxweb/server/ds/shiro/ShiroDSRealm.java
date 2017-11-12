package org.zoxweb.server.ds.shiro;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.UserIDCredentialsDAO;
import org.zoxweb.server.security.UserIDCredentialsDAO.UserStatus;
import org.zoxweb.server.security.shiro.ShiroBaseRealm;
import org.zoxweb.server.security.shiro.authc.DomainAuthenticationInfo;
import org.zoxweb.server.security.shiro.authc.DomainPrincipalCollection;
import org.zoxweb.server.security.shiro.authc.DomainUsernamePasswordToken;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.crypto.CryptoConst.MDType;
import org.zoxweb.shared.crypto.PasswordDAO;
import org.zoxweb.shared.data.FormInfoDAO;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.filters.FilterType;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.security.shiro.ShiroAssociationRuleDAO;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.GetValue;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.SetName;
import org.zoxweb.shared.util.SharedStringUtil;
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
		return apiSecurityManager;
	}

	public void setAPISecurityManager(APISecurityManager<Subject> apiSecurityManager) {
		this.apiSecurityManager = apiSecurityManager;
	}
	
	public APIDataStore<?> getDataStore()
	{
		return dataStore;
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
	        String userID = ((DomainPrincipalCollection) principals).getSubjectID();
	       
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
			log.info("Domain based authentication");
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
		 throw new AuthenticationException("Not a domain info");
	}

	
	@Override
	public void addShiroRule(ShiroAssociationRuleDAO sard) 
	{
		// TODO Auto-generated method stub
		
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
	
	
	
	public  UserIDDAO lookupUserID(String user, String ...params)
			throws NullPointerException, IllegalArgumentException, AccessException
		{
			SharedUtil.checkIfNulls("DB or user ID null", dataStore, user);
			//APIDataStore<?> dataStore = FidusStoreAPIManager.SINGLETON.lookupAPIDataStore(FidusStoreAPIManager.FIDUS_STORE_NAME);
			QueryMatch<?> query = null;
			if (FilterType.EMAIL.isValid(user))
			{
				// if we have an email
				query = new QueryMatch<String>(RelationalOperator.EQUAL, user, UserIDDAO.Param.PRIMARY_EMAIL.getNVConfig());
			}
			else
			{
				query = new QueryMatch<String>(RelationalOperator.EQUAL, user, MetaToken.REFERENCE_ID);//"_id", new BasicDBObject("$in", listOfObjectID)
			}
		
			ArrayList<String> listParams = null;
			if (params != null && params.length > 0)
			{
				listParams = new ArrayList<String>();
				for (String str : params)
				{
					if (!SharedStringUtil.isEmpty(str))
					{
						listParams.add(str);
					}
				}
			}
			
			List<UserIDDAO> listOfUserIDDAO = dataStore.search(UserIDDAO.NVC_USER_ID_DAO, listParams, query);
			
			if (listOfUserIDDAO == null || listOfUserIDDAO.size() != 1)
			{
				return null;
			}
			
			return listOfUserIDDAO.get(0);

		}
	
	
	public void createUser(UserIDDAO userID, UserStatus userIDstatus, String password)
			throws NullPointerException, IllegalArgumentException, AccessException, APIException
	{
		SharedUtil.checkIfNulls("UserIDDAO object is null.", userID, userIDstatus);
		password = FilterType.PASSWORD.validate(password);
		
		
		if (lookupUserID(userID.getSubjectID()) != null)
		{
			throw new APIException("User already exist");
		}
			
		log.info("User Name: " + userID.getPrimaryEmail());
		log.info("First Name: " + userID.getUserInfo().getFirstName());
		log.info("Middle Name: " + userID.getUserInfo().getMiddleName());
		log.info("Last Name: " + userID.getUserInfo().getLastName());
		log.info("Birthday: " + userID.getUserInfo().getDOB());
		
		userID.setReferenceID(null);
		SharedUtil.validate(userID, true, true);
		
		
		
//		Calendar dob = new GregorianCalendar();
//		dob.setTime(new Date(userID.getUserInfo().getDOB()));
//		
//		Calendar yearToCheck = new GregorianCalendar();
//		
//		yearToCheck.set(Calendar.YEAR, yearToCheck.get(Calendar.YEAR) - 13);
//		
//		log.info("DOB: " + dob.getTime());
//		log.info("Year to check: " + yearToCheck.getTime());
//		
//		if (dob.after(yearToCheck))
//		{
//			throw new IllegalArgumentException("You must be at least 13 years old to register.");
//		}
		
			
		
		// special case to avoid chicken and egg situation
		ObjectId objID = ObjectId.get();
		String userIDRef = objID.toHexString();
		apiSecurityManager.associateNVEntityToSubjectUserID(userID, userIDRef);
		userID.setReferenceID(userIDRef);
		////////////////////////
		
		try
		{
			dataStore.insert(userID);
			
			UserIDCredentialsDAO userIDCredentials = new UserIDCredentialsDAO();
			userIDCredentials.setReferenceID(userID.getReferenceID());
			userIDCredentials.setUserID(userID.getReferenceID());
			userIDCredentials.setLastStatusUpdateTimestamp(System.currentTimeMillis());
			userIDCredentials.setUserStatus(userIDstatus);
			PasswordDAO passwordDAO = CryptoUtil.hashedPassword(MDType.SHA_512, 0, 8196, password);
			passwordDAO.setUserID(userID.getReferenceID());
			userIDCredentials.setPassword(passwordDAO);
			
			
			
			switch(userIDstatus)
			{
			case ACTIVE:
				break;
			case DEACTIVATED:
				break;
			case INACTIVE:
				break;
			case PENDING_ACCOUNT_ACTIVATION:
			case PENDING_RESET_PASSWORD:
				userIDCredentials.setPendingToken(UUID.randomUUID().toString());
				break;

			
			}
			
			
			dataStore.insert(userIDCredentials);
			userIDCredentials.getPassword().setReferenceID(userIDCredentials.getReferenceID());
			dataStore.update(userIDCredentials);
			// create the user master key
			dataStore.insert(KeyMakerProvider.SINGLETON.createUserIDKey(userID, KeyMakerProvider.SINGLETON.getMasterKey()));
			
			// removed for now created during login
			// MN 2014-12-23
			// FidusStoreDataManager.SINGLETON.setUpUserAccount(userID, dataStore, (APIDocumentStore<?>) dataStore);
			
//			APINotificationMessage message = new APINotificationMessageDAO();
//			message.setMessageType(APIServiceType.EMAIL_NOTIFICATION);
//			message.setSenderID("do-not-reply@zoxweb.com");
//			String [] recipients = {userID.getPrimaryEmail()};
//			message.setRecipientIDs(recipients);
//			
//			//	TBD: mn find solution for static NVConfig creation for classes.
//			//MessageTemplateDAO mtd = new MessageTemplateDAO();
//			
//			List<MessageTemplateDAO> messageTemplateList = dataStore.search(MessageTemplateDAO.NVC_MESSAGE_TEMPLATE_DAO, null, 
//					new QueryMatch<String>(DataParam.NAME.getNVConfig().getName(), "fidus_store_registration", RelationalOperator.EQUAL)
//					);
//			
//			MessageTemplateDAO messageTemplate = null;
//			
//			if (messageTemplateList.size() == 0)
//			{
//				throw new APIException("Template missing.");
//			}
//			
//			messageTemplate = messageTemplateList.get(0);
//			message.setTitle(messageTemplate.getTitle());
//			message.setPreTag(messageTemplate.getPreTag());
//			message.setPostTag(messageTemplate.getPostTag());
//			//List<NVPair> tagList = messageTemplate.getBodyTags();
//			//HttpServletResponse resp = getThreadLocalResponse();	
//			String baseUrl = ApplicationConfigManager.SINGLETON.loadDefault().lookupValue(ApplicationConfigDAO.ApplicationDefaultParam.APPLICATION_URL) + 
//							 ApplicationConfigManager.SINGLETON.loadDefault().lookupValue("application_start_uri") + "/activation";
//			
//			//System.out.println("baseurl:" + baseUrl);
//			log.info("URL: " + baseUrl);
//			
//			for (NVPair nvp : messageTemplate.getBodyTags().values())
//			{
//				switch(nvp.getName())
//				{
//				case "name":
//					nvp.setValue(userID.getUserInfo().getFirstName());
//					break;
//				case "last_name":
//					nvp.setValue(userID.getUserInfo().getLastName());
//					break;
//				case "token":
//					nvp.setValue(userIDCredentials.getPendingToken());
//					break;
//				case "activation_url":
//					nvp.setValue(baseUrl);
//				}
//			}
//			
//			message.setBodyTags(messageTemplate.getBodyTags());
//			message.setBodyTemplate(messageTemplate.getBodyContent());
//			
//			message.getExtraAttribues().add(new NVPair(SMTPMessageParam.SENDER_ID_NAME.getValue(), "Fidus Store"));
//
//			SMTPProvider smtpProvider = (SMTPProvider) FidusStoreAPIManager.SINGLETON.lookupAPINotification(FidusStoreAPIManager.FIDUS_STORE_EMAIL);
//			smtpProvider.sendAPIMessage(message, APINotificationDelivery.QUEUED);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new AccessException(e.getMessage());			
		}
	}
	
	public Set<String> getRecusiveNVEReferenceIDFromForm(String formReferenceID)
	{
		HashSet<String> ret = null;
		
		BasicDBObject projection = new BasicDBObject();
		projection = projection.append(FormInfoDAO.Param.FORM_REFERENCE.getNVConfig().getName(), true);
		
		BasicDBObject result = dataStore.lookupByReferenceID(FormInfoDAO.NVC_FORM_INFO_DAO.getName(), new ObjectId(formReferenceID), projection);
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
			BasicDBObject nvcEntry = dataStore.lookupByReferenceID("nv_config_entities", canonical_id);
			if (nvcEntry != null && canonical_id != null)
			{
				String nvcSubName = nvcEntry.getString(MetaToken.CANONICAL_ID.getName());
				if(nvcSubName != null)
				{
					return dataStore.lookupByReferenceID(nvcSubName, reference_id);
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
				List<UserIDCredentialsDAO> listOfUserIDCredentialsDAO = dataStore.searchByID(UserIDCredentialsDAO.NVC_USER_ID_CREDENTIALS_DAO, userIDDAO.getReferenceID());
				
				if (listOfUserIDCredentialsDAO != null && listOfUserIDCredentialsDAO.size() == 1)
				{
					return listOfUserIDCredentialsDAO.get(0);
				}
				
			}
			
			throw new AccessException("User credentials not found.");
		}
	
	public  UserIDDAO lookupUserID(GetValue<String> userValue, String ...params)
			throws NullPointerException, IllegalArgumentException, AccessException
		{
			SharedUtil.checkIfNulls("DB or user ID null", dataStore, userValue);
			//APIDataStore<?> dataStore = FidusStoreAPIManager.SINGLETON.lookupAPIDataStore(FidusStoreAPIManager.FIDUS_STORE_NAME);
			String user = userValue.getValue();
			QueryMatch<?> query = null;
			if (FilterType.EMAIL.isValid(user))
			{
				// if we have an email
				query = new QueryMatch<String>(RelationalOperator.EQUAL, user, UserIDDAO.Param.PRIMARY_EMAIL.getNVConfig());
			}
			else
			{
				query = new QueryMatch<String>(RelationalOperator.EQUAL, user, MetaToken.REFERENCE_ID);//"_id", new BasicDBObject("$in", listOfObjectID)
			}
		
			ArrayList<String> listParams = null;
			if (params != null && params.length > 0)
			{
				listParams = new ArrayList<String>();
				for (String str : params)
				{
					if (!SharedStringUtil.isEmpty(str))
					{
						listParams.add(str);
					}
				}
			}
			
			List<UserIDDAO> listOfUserIDDAO = dataStore.search(UserIDDAO.NVC_USER_ID_DAO, listParams, query);
			
			if (listOfUserIDDAO == null || listOfUserIDDAO.size() != 1)
			{
				return null;
			}
			
			return listOfUserIDDAO.get(0);

		}

}
