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