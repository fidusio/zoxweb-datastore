package org.zoxweb.server.ds.shiro;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import org.junit.Test;
import org.zoxweb.server.ds.SetupTool;

public class ShiroPermissionsTest 
{
	SetupTool setupTool = getSetupTool();
	private static SetupTool getSetupTool()
	{
		try {
			return SetupTool.initApp();
		} catch (NullPointerException | KeyStoreException | NoSuchAlgorithmException | CertificateException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	@Test
	public void testPemissions()
	{
		
	}
}
