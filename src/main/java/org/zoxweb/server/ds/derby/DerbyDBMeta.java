package org.zoxweb.server.ds.derby;

import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.io.UByteArrayOutputStream;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.util.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DerbyDBMeta {
    private DerbyDBMeta(){}

    public static  List<NVEntityRefMeta> toNVEntityRefMetaList(NVStringList list)
    {
        List<NVEntityRefMeta> ret = new ArrayList<NVEntityRefMeta>();
        for(String str : list.getValues())
        {

            ret.add(toNVEntityRefMeta(str));
        }
        return ret;
    }

    public static NVEntityRefMeta toNVEntityRefMeta(String token)
    {
        String[] tokens = token.split(":");
        return new NVEntityRefMeta(tokens[0], tokens[1]);
    }

    public static String toNVEntityDBToken(NVEntity nve)
    {
        return nve.getNVConfig().getMetaType().getName() + ":" + nve.getGlobalID();
    }


    public static void toDerbyValue(StringBuilder sb, NVBase<?> nvb, boolean addName) throws IOException {
        Object value = nvb.getValue();
        if (addName)
        {
            sb.append(nvb.getName());
            sb.append("=?");
            return;
        }
        if (value == null)
        {
            sb.append("NULL");
        }
        else if (value instanceof String)
        {
            sb.append("'" + value + "'");
        }
        else if (value instanceof Enum)
        {
            sb.append("'" +  ((Enum<?>) value).name()+ "'");
        }
        else if (value instanceof Boolean)
        {
            sb.append(value);
        }
        else if(value instanceof Number)
        {
            sb.append(value);
        }
        else if (value instanceof NVGenericMap)
        {
            String json = GSONUtil.toJSONGenericMap((NVGenericMap) value, false, false, true);
            sb.append("'");
            sb.append(json);
            sb.append("'");
        }
        else if (nvb instanceof NVStringList)
        {
            NVGenericMap nvgm = new NVGenericMap();
            nvgm.add((GetNameValue<?>) nvb);
            sb.append("'" + GSONUtil.toJSONGenericMap(nvgm, false, false, false) + "'");
        }
        else if (value instanceof NVEntity)
        {
            sb.append("'" + ((NVEntity) value).getNVConfig().getName() + ":" + ((NVEntity) value).getGlobalID());
        }
    }


    public static void toDerbyValue(PreparedStatement ps, int index, NVBase<?> nvb) throws IOException, SQLException {
        Object value = nvb.getValue();
        if (nvb.getValue() == null)
        {
            ps.setObject(index, null);
        }
        if (nvb instanceof NVBlob)
        {
            ps.setBinaryStream(index, new ByteArrayInputStream(((NVBlob)nvb).getValue()));
        }
        else if (nvb.getValue() instanceof String)
        {
            ps.setString(index, (String) nvb.getValue());
        }
        else if (nvb instanceof NVEnum)
        {
            ps.setString(index, ((Enum<?>) nvb.getValue()).name());
        }
        else if (nvb instanceof NVBoolean)
        {
            ps.setBoolean(index, (Boolean) nvb.getValue());
        }
//    else if(nvc.getMetaType() == Date.class)
//    {
//      ps.setLong(index, (Long)value);
//    }
        else if(value instanceof Number)
        {
            ps.setObject(index, value, DerbyDT.javaClassToSQLType(value.getClass()));
        }
        else if (nvb instanceof NVGenericMap)
        {
            String json = GSONUtil.toJSONGenericMap((NVGenericMap) nvb, false, false, true);
            ps.setString(index, json);
        }
        else if (MetaToken.isPrimitiveArray(nvb))
        {
            NVGenericMap nvgm = new NVGenericMap();
            nvgm.add((GetNameValue<?>) nvb);
            String json = GSONUtil.toJSONGenericMap(nvgm, false, false, false);
            ps.setString(index, json);
        }
        else if (MetaToken.isNVEntityArray(nvb))
        {
            NVGenericMap nvgm = new NVGenericMap();
//            nvgm.add((GetNameValue<?>) nvb);
//            String json = GSONUtil.toJSONGenericMap(nvgm, false, false, false);
//            ps.setString(index, json);
        }
        else if (nvb instanceof NVEntityReference)
        {
            ps.setString(index, toNVEntityDBToken(((NVEntityReference) nvb).getValue()));
        }

    }






    @SuppressWarnings("unchecked")
    public static void mapValue(ResultSet rs, NVConfig nvc, NVBase<?> nvb) throws SQLException, IOException {
        //Object value = rs.getObject(nvb.getName());
        if (nvb instanceof NVGenericMap)
        {
            NVGenericMap nvgm = GSONUtil.fromJSONGenericMap(rs.getString(nvb.getName()), null, null);
            ((NVGenericMap)nvb).setValue(nvgm.getValue());
        }
        else if (MetaToken.isPrimitiveArray(nvb))
        {
            String strValue = rs.getString(nvb.getName());
            NVGenericMap nvgm = GSONUtil.fromJSONGenericMap(strValue, null, null);

            if (nvgm.size() == 1) {

                if (nvb instanceof NVEnumList)
                {
                    NVStringList tempList = (NVStringList)nvgm.values()[0];
                    for(String enumName : tempList.getValues())
                    {
                        ((NVEnumList)nvb).getValue().add(SharedUtil.enumValue(nvc.getMetaTypeBase(), enumName));
                    }
                }
                else {
                    ((NVBase<Object>) nvb).setValue(nvgm.values()[0].getValue());
                }
            }
            else
            {
                System.out.println("ERROR !!! : " + nvgm + " " + strValue);
            }
        }
        else if (nvb instanceof NVEnum)
        {
            ((NVEnum)nvb).setValue(SharedUtil.enumValue(nvc.getMetaType(), rs.getString(nvb.getName())));
        }
        else if(nvb instanceof NVBlob)
        {
            Blob b = rs.getBlob(nvb.getName());
            InputStream is = b.getBinaryStream();
            UByteArrayOutputStream baos = new UByteArrayOutputStream();
            IOUtil.relayStreams(is, baos, true);
            ((NVBlob)nvb).setValue(baos.toByteArray());
        }
        else if (nvb instanceof NVFloat)
        {
            ((NVFloat)nvb).setValue(rs.getFloat(nvb.getName()));
        }
        else if (nvb instanceof NVDouble)
        {
            ((NVDouble)nvb).setValue(rs.getDouble(nvb.getName()));
        }
        else if (nvb instanceof NVLong)
        {
            ((NVLong)nvb).setValue(rs.getLong(nvb.getName()));
        }
        else if (nvb instanceof NVInt)
        {
            ((NVInt)nvb).setValue(rs.getInt(nvb.getName()));
        }
        else if (nvb instanceof NVBoolean)
        {
            ((NVBoolean)nvb).setValue(rs.getBoolean(nvb.getName()));
        }
        else
            ((NVBase<Object>)nvb).setValue(rs.getObject(nvb.getName()));
    }

    public static DerbyDT metaToDerbyDT(NVConfig nvc)
    {
        Class<?> nvcJavaClass = nvc.getMetaType();
        DerbyDT ddt = null;
        if (nvcJavaClass == Boolean.class)
        {
            ddt = DerbyDT.BOOLEAN;
        }
        else if (nvcJavaClass == byte[].class)
        {
            ddt = DerbyDT.BLOB;
            return ddt;
        }
        else if (nvcJavaClass == String.class)
        {
            if (nvc.getName().equals(MetaToken.GLOBAL_ID.getName()))
            {
                ddt = DerbyDT.GLOBAL_ID;
            }
            else if (nvc.getName().equals(MetaToken.USER_ID.getName()))
            {
                ddt = DerbyDT.USER_ID;
            }
            else if (nvc.getName().equals(MetaToken.ACCOUNT_ID.getName()))
            {
                ddt = DerbyDT.ACCOUNT_ID;
            }
            else
            {
                ddt = DerbyDT.LONG_VARCHAR;
            }
        }
        else if (nvcJavaClass == Integer.class)
        {
            ddt = DerbyDT.INTEGER;
        }
        else if (nvcJavaClass == Long.class)
        {
            ddt = DerbyDT.BIGINT;
        }
        else if (nvcJavaClass == Float.class)
        {
            ddt = DerbyDT.FLOAT;
        }
        else if (nvcJavaClass == Double.class)
        {
            ddt = DerbyDT.DOUBLE;
        }
        else if (nvcJavaClass == Date.class)
        {
            ddt = DerbyDT.TIMESTAMP;
        }
        else if (nvcJavaClass == NVStringList.class)
        {
            ddt = DerbyDT.STRING_LIST;
        }
        else if (Enum.class.isAssignableFrom(nvcJavaClass))
        {
            ddt = DerbyDT.ENUM;
        }
        else if (ArrayValues.class.isAssignableFrom(nvcJavaClass))
        {
            ddt = DerbyDT.INNER_ARRAY;
        }
        else if (NVEntity.class.isAssignableFrom(nvcJavaClass))
        {
            ddt = DerbyDT.REMOTE_REFERENCE;
        }
        else if (nvc.isArray())
        {
            return DerbyDT.INNER_ARRAY;
        }



        if (ddt != null)
        {
            if (nvc.isArray())
                return DerbyDT.INNER_ARRAY;
            else
                return ddt;
        }
        throw new IllegalArgumentException("Type not found " + nvc);
    }
}
