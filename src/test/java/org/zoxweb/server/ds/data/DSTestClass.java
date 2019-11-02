package org.zoxweb.server.ds.data;


import org.zoxweb.server.ds.derby.DerbyDBMeta;
import org.zoxweb.server.util.MetaUtil;
import org.zoxweb.shared.data.SetNameDescriptionDAO;
import org.zoxweb.shared.util.*;

import java.security.SecureRandom;
import java.util.Date;
import java.util.UUID;


public class DSTestClass {

    static SecureRandom sr =  new SecureRandom();

    @SuppressWarnings("serial")
    public static class AllTypes
        extends SetNameDescriptionDAO
    {
        public enum Param
                implements GetNVConfig
        {

            BOOL_VAL(NVConfigManager.createNVConfig("boolean_val", "Bool Value", "BoolVal", false, true, boolean.class)),
            BYTES_VAL(NVConfigManager.createNVConfig("bytes_val", "Byte array", "BytesVal", false, true, byte[].class)),
            INT_VAL(NVConfigManager.createNVConfig("int_val", "Integer Value", "IntVal", false, true, int.class)),
            UNIQUE_INT_VAL(NVConfigManager.createNVConfig("unique_int_val", "Integer Value", "IntVal", false, true, true, int.class, null)),
            LONG_VAL(NVConfigManager.createNVConfig("long_val", "Long Value", "LongVal", false, true, long.class)),
            FLOAT_VAL(NVConfigManager.createNVConfig("float_val", "Float Value", "FloatVal", false, true, float.class)),
            DOUBLE_VAL(NVConfigManager.createNVConfig("double_val", "Double Value", "DoubleVal", false, true, double.class)),
            ENUM_VAL(NVConfigManager.createNVConfig("enum_val", "Enum Value", "EnumVal", false, true, Const.Status.class)),
            STRING_ARRAY(NVConfigManager.createNVConfig("string_list", "String array as string list", "StringList", false, true, NVStringList.class)),
            DATE_VAL(NVConfigManager.createNVConfig("date_val", "Date", "Date", false, true, Date.class)),
            ;

            private NVConfig nvc;

            Param(NVConfig nvc)
            {
                this.nvc = nvc;
            }

            public String toString()
            {
                return getNVConfig().getName();
            }

            /* (non-Javadoc)
             * @see org.zoxweb.shared.util.GetNVConfig#getNVConfig()
             */
            @Override
            public NVConfig getNVConfig()
            {
                return nvc;
            }
        }
        public static final NVConfigEntity NVC_ALLTYPES_DOA = new NVConfigEntityLocal(
                "all_types",
                null ,
                "AllTypes",
                true,
                false,
                false,
                false,
                AllTypes.class,
                SharedUtil.extractNVConfigs(AllTypes.Param.values()),
                null,
                false,
                SetNameDescriptionDAO.NVC_NAME_DESCRIPTION_DAO
        );

        public AllTypes()
        {
            super(NVC_ALLTYPES_DOA);
        }

        public boolean getBoolean()
        {
            return lookupValue(Param.BOOL_VAL);
        }

        public void setBoolean(boolean val)
        {
            setValue(Param.BOOL_VAL, val);
        }

        public byte[] getBytes()
        {
            return lookupValue(Param.BYTES_VAL);
        }

        public void setBytes(byte[] val)
        {
            setValue(Param.BYTES_VAL, val);
        }

        public int getInt()
        {
            return lookupValue(Param.INT_VAL);
        }

        public void setInt(int val)
        {
            setValue(Param.INT_VAL, val);
        }

        public int getUniqueInt()
        {
            return lookupValue(Param.UNIQUE_INT_VAL);
        }

        public void setUniqueInt(int val)
        {
            setValue(Param.UNIQUE_INT_VAL, val);
        }

        public long getLong()
        {
            return lookupValue(Param.LONG_VAL);
        }

        public void setLong(long val)
        {
            setValue(Param.LONG_VAL, val);
        }

        public float getFloat()
        {
            return lookupValue(Param.FLOAT_VAL);
        }

        public void setFloat(float val)
        {
            setValue(Param.FLOAT_VAL, val);
        }

        public double getDouble()
        {
            return lookupValue(Param.DOUBLE_VAL);
        }

        public void setDouble(double val)
        {
            setValue(Param.DOUBLE_VAL, val);
        }

        public Const.Status getStatus()
        {
            return lookupValue(Param.ENUM_VAL);
        }

        public void setStatus(Const.Status val)
        {
            setValue(Param.ENUM_VAL, val);
        }
        public String[] getStringArray()
        {
            return ((NVStringList)lookup(Param.STRING_ARRAY)).getValues();
        }

        public void setStringArray(String ...vals)
        {
            ((NVStringList)lookup(Param.STRING_ARRAY)).setValues(vals);
        }

        public void setDate(Date date)
        {
            setValue(Param.DATE_VAL, date.getTime());
        }

        public Date getDate()
        {
            return new Date((long)lookupValue(Param.DOUBLE_VAL));
        }



        @SuppressWarnings("unused")
        public static void testValues(AllTypes at)
        {
            boolean bool = at.getBoolean();
            int integer = at.getInt();
            byte[] bytes = at.getBytes();
            long longVal = at.getLong();
            float floatVal = at.getFloat();
            double doubleVal = at.getDouble();
            String str = at.getName();
            Const.Status status = at.getStatus();

        }

        public static AllTypes autoBuilder()
        {
            AllTypes ret = new AllTypes();
            try {

                ret.setName("AllTypes-" + Math.abs(sr.nextInt()));
                ret.setDescription("Auto generated.");
                ret.setBoolean(sr.nextBoolean());
                ret.setInt(sr.nextInt());
                ret.setUniqueInt(sr.nextInt());
                ret.setLong(sr.nextLong());
                ret.setDouble(sr.nextDouble());
                ret.setFloat(sr.nextFloat());
                ret.setStatus(Const.Status.values()[sr.nextInt(Const.Status.values().length)]);
                ret.setBytes(sr.generateSeed(64));
                ret.setStringArray("toto", "titi", "tata");
                ret.setDate(new Date());

            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            return ret;
        }

    }



    @SuppressWarnings("serial")
    public static class ComplexTypes
            extends SetNameDescriptionDAO {
        public enum Param
                implements GetNVConfig {

            ALL_TYPES(NVConfigManager.createNVConfigEntity("all_types", "All Types", "AllTypes", false, true, AllTypes.NVC_ALLTYPES_DOA, NVConfigEntity.ArrayType.NOT_ARRAY)),
            ALL_TYPES_ARRAY(NVConfigManager.createNVConfigEntity("all_types_array", "All Types array", "AllTypesArray", false, true, AllTypes.NVC_ALLTYPES_DOA, NVConfigEntity.ArrayType.LIST)),
            INT_ARRAY(NVConfigManager.createNVConfig("int_array", "Integer Value", "IntVal", false, true, int[].class)),
            LONG_ARRAY(NVConfigManager.createNVConfig("long_array", "Long Value", "LongVal", false, true, long[].class)),
            FLOAT_ARRAY(NVConfigManager.createNVConfig("float_array", "Float Value", "FloatVal", false, true, float[].class)),
            DOUBLE_ARRAY(NVConfigManager.createNVConfig("double_array", "Double Value", "DoubleVal", false, true, double[].class)),
            ENUM_ARRAY(NVConfigManager.createNVConfig("enum_array", "Enum Value", "EnumVal", false, true, Const.Status[].class)),
            ;

            private NVConfig nvc;

            Param(NVConfig nvc) {
                this.nvc = nvc;
            }

            public String toString() {
                return getNVConfig().getName();
            }

            /* (non-Javadoc)
             * @see org.zoxweb.shared.util.GetNVConfig#getNVConfig()
             */
            @Override
            public NVConfig getNVConfig() {
                return nvc;
            }
        }

        public static final NVConfigEntity NVC_NVETYPES_DOA = new NVConfigEntityLocal(
                "complex_types",
                null,
                "ComplexTypes",
                true,
                false,
                false,
                false,
                ComplexTypes.class,
                SharedUtil.extractNVConfigs(Param.values()),
                null,
                false,
                SetNameDescriptionDAO.NVC_NAME_DESCRIPTION_DAO
        );

        public ComplexTypes()
        {
            super(NVC_NVETYPES_DOA);
        }

        public AllTypes getAllTypes()
        {
            return lookupValue(Param.ALL_TYPES);
        }

        public void setAllTypes(AllTypes at)
        {
            setValue(Param.ALL_TYPES, at);
        }

        public static ComplexTypes buildComplex()
        {
            ComplexTypes ret = new ComplexTypes();

            ret.setName("AllTypes-" + Math.abs(sr.nextInt()));
            ret.setDescription("Auto generated.");
            for (int i = 0; i < 3; i++) {
                ((NVDoubleList) ret.attributes.get(Param.DOUBLE_ARRAY.getNVConfig().getName())).getValue().add(sr.nextDouble());
                ((NVFloatList) ret.attributes.get(Param.FLOAT_ARRAY.getNVConfig().getName())).getValue().add(sr.nextFloat());
                ((NVIntList) ret.attributes.get(Param.INT_ARRAY.getNVConfig().getName())).getValue().add(sr.nextInt());
                ((NVLongList) ret.attributes.get(Param.LONG_ARRAY.getNVConfig().getName())).getValue().add(sr.nextLong());
                ((NVEnumList) ret.attributes.get(Param.ENUM_ARRAY.getNVConfig().getName())).getValue().add(Const.Status.values()[sr.nextInt(Const.Status.values().length)]);
                ((NVEntityReferenceList) ret.attributes.get(Param.ALL_TYPES_ARRAY.getNVConfig().getName())).getValue().add(AllTypes.autoBuilder());
            }

            return ret;
        }


    }

    public static  <V extends NVEntity> V init(V nve)
    {
        int index = 0;
        MetaUtil.initTimeStamp(nve);
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        if (nve instanceof DeviceID)
        {
            ((DeviceID<String>) nve).setDeviceID(UUID.randomUUID().toString());
        }
        if (nve instanceof SubjectID)
        {
            ((SubjectID<String>) nve).getSubjectID();
        }
        for (NVBase<?> nvb : nve.getAttributes().values())
        {
            if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_UPDATE_EXCLUSION, nvb))
            {
                if(nvce.lookup(nvb.getName()).getMetaType() == Date.class) {
                }
                else if(nvb instanceof NVInt)
                {
                    ((NVInt)nvb).setValue(sr.nextInt());
                }
                else if(nvb instanceof NVLong)
                {
                    ((NVLong)nvb).setValue(sr.nextLong());
                }
                else if(nvb instanceof NVFloat)
                {
                    ((NVFloat)nvb).setValue(sr.nextFloat());
                }
                else if(nvb instanceof NVDouble)
                {
                    ((NVDouble)nvb).setValue(sr.nextDouble());
                }
                else if(nvb instanceof NVPair && nvb.getValue() == null)
                {
                    ((NVPair)nvb).setValue("str-" + ++index);
                }
            }
        }






        return nve;
    }


}
