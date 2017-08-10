package org.zoxweb.server.ds.mongo;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;

import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APICredentialsDAO;
import org.zoxweb.shared.filters.ValueFilter;
import org.zoxweb.shared.util.NVEntity;

@SuppressWarnings("serial")
public class UpdateFilterClass
    implements ValueFilter<Class<?>, Boolean>
{
	public static final UpdateFilterClass SINGLETON = new UpdateFilterClass();
	private static final Class<?> array[] =
	{
		APIConfigInfoDAO.class,
		//	MN
		//	This is required to prevent API tokens from updating this class during token generation and update.
		APICredentialsDAO.class,
		//UserIDDAO.class
	};

	@Override
	public String toCanonicalID() 
	{
		return null;
	}

	@Override
	public Boolean validate(Class<?> in) 
			throws NullPointerException, IllegalArgumentException 
	{
		return isValid(in);
	}

	@Override
	public boolean isValid(Class<?> in)
	{
		try
		{
			//	Mother of all hacks, must be removed once proper solution is found.
			// fix create a permission or role for management to access the class type 
			// assign update permission to management
			// and check permission
			Subject subject = SecurityUtils.getSubject();
			
			if (subject.isAuthenticated())
			{
				if (subject.getPrincipal().equals("management@zoxweb.com"))
				{
					return true;
				}
			}
		}
		catch (Exception e)
		{
			
		}
		
		for (Class<?> c : array)
		{
			if (c.equals(in))
			{
				return false;
			}
		}
		
		return true;
	}
	
	public boolean isValid(NVEntity in)
	{
		if (in != null)
		{
			return isValid(in.getClass());
		}
	
		
		return false;
	}

}