package org.zoxweb.server.ds.mongo.sync;

import com.mongodb.client.MongoDatabase;
import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.types.Binary;
//import org.bson.types.ObjectId;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.crypto.EncryptedData;
import org.zoxweb.shared.util.*;

import java.math.BigDecimal;
import java.util.*;

public class MongoUtil {
    /**
     * Contains reference ID, account ID, and user ID.
     */
    public enum ReservedID
            implements GetNameValue<String> {
        REFERENCE_ID(MetaToken.REFERENCE_ID.getName(), "_id"),
        SUBJECT_GUID(MetaToken.SUBJECT_GUID),
        GUID(MetaToken.GUID);

        private final String name;
        private final String value;


        ReservedID(GetName gnv) {
            this(gnv.getName(), gnv.getName());
        }


        ReservedID(String name, String value) {
            this.name = name;
            this.value = value;
            MongoUtil.SINGLETON.reservedIDMap.put(name, this);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static ReservedID lookupByName(GetName gn) {
            return MongoUtil.SINGLETON.reservedIDMap.get(gn.getName());
        }

        public static ReservedID lookupByName(String name) {
            return MongoUtil.SINGLETON.reservedIDMap.get(name);
        }

        public static String map(NVConfig nvc, String name) {
            ReservedID resID = lookupByName(name);
            if (resID != null) {
                return resID.getValue();
            }

            if (name != null && nvc != null && nvc.isTypeReferenceID() && name.equals(nvc.getName())) {
                return "_" + nvc.getName();
            }

            return name;
        }
    }


//    protected final static Map<Class<?>, BiConsumer<?,?>> nvbToDocument = new LinkedHashMap<>();

//    public interface DataUpdater {
//        void update(SyncMongoDS mongoDS, DataUpdate dataUpdate)
//                throws InstantiationException, IllegalAccessException, APIException;
//    }


    public interface DataDeserializer {
        void deserialize(SyncMongoDS ds, String subjectGUID, MongoDatabase db, Document doc, NVEntity container, NVConfig nvc, NVBase<?> nvb)
                throws InstantiationException, IllegalAccessException, APIException;
    }


    public final static MongoUtil SINGLETON = new MongoUtil();

    private MongoUtil() {
        init();
    }


    //private final KVMapStore<Class<?>, DataUpdater> BSONToData = new KVMapStoreDefault<>(new LinkedHashMap<>());
    public final RegistrarMapDefault<Class<?>, DataDeserializer> BSONToDataDeserializer = new RegistrarMapDefault<>(c -> {
        if (c.isArray() && c.getComponentType().isEnum())
            return Enum[].class;

        return c.isEnum() ? Enum.class : c;
    }, null);

    private final HashMap<String, ReservedID> reservedIDMap = new LinkedHashMap<>();

    public BsonBinary bsonNVEGUID(NVEntity nve) {
        return new BsonBinary(UUID.fromString(nve.getGUID()));
    }

//    public UUID nveGUID(NVEntity nve) {
//        return UUID.fromString(nve.getGUID());
//    }


//    public <V> V guessID(String idToGuess) {
//        SUS.checkIfNulls("null idToGuess", idToGuess);
//        try {
//            return (V) new ObjectId(idToGuess);
//        } catch (Exception e) {
//            return (V) UUID.fromString(idToGuess);
//        }
//    }


    private void init() {

//        BSONToDataDeserializer.setKeyFilter(c -> {
//            if (c.isArray() && c.getComponentType().isEnum())
//                return Enum[].class;
//
//            return c.isEnum() ? Enum.class : c;
//        });

        BSONToDataDeserializer.map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                    Object tempValue = doc.get(nvc.getName());
                    if (tempValue instanceof Document) {
                        tempValue = mds.fromDB(subjectGUID, db, (Document) tempValue, EncryptedData.class);
                    }

                    if (mds.getAPIConfigInfo().getSecurityController() != null)
                        ((NVPair) nvb).setValue((String) mds.getAPIConfigInfo().getSecurityController().decryptValue(mds, container, nvb, tempValue, null));
                    else
                        ((NVPair) nvb).setValue((String) tempValue);
                }, String.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                    List<String> values = new ArrayList<String>();
                    List<Document> dbValues = (List<Document>) doc.get(nvc.getName());

                    for (Object val : dbValues) {
                        values.add((String) val);
                    }
                    ((NVStringList) nvb).setValue(values);
                }, NVStringList.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                    Set<String> values = new HashSet<String>();
                    List<Document> dbValues = (List<Document>) doc.get(nvc.getName());

                    for (Object val : dbValues) {
                        values.add((String) val);
                    }
                    ((NVStringSet) nvb).setValue(values);
                }, NVStringSet.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                    NVGenericMap nvgm = (NVGenericMap) nvb;
                    Document dbNVGM = (Document) doc.get(nvc != null ? nvc.getName() : nvb.getName());
                    mds.fromNVGenericMap(subjectGUID, nvgm, dbNVGM);
                }, NVGenericMap.class)

                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                    Binary mBinary = (Binary) doc.get(nvc.getName());
                    if (mBinary != null)
                        ((NVBlob) nvb).setValue(mBinary.getData());
                }, byte[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVBigDecimal) nvb).setValue(new BigDecimal(doc.getString(nvc.getName()))),
                        BigDecimal.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVBoolean) nvb).setValue(doc.getBoolean(nvc.getName())),
                        boolean.class, Boolean.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVDouble) nvb).setValue(doc.getDouble(nvc.getName())),
                        double.class, Double.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVFloat) nvb).setValue(doc.getDouble(nvc.getName()).floatValue()),
                        double.class, Double.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVLong) nvb).setValue(doc.getLong(nvc.getName()))
                        , long.class, Long.class, Date.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVInt) nvb).setValue(doc.getInteger(nvc.getName()))
                        , int.class, Integer.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVNumber) nvb).setValue((Number) doc.get(nvc.getName()))
                        , Number.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> ((NVEnum) nvb).setValue(SharedUtil.enumValue(nvc.getMetaType(), doc.getString(nvc.getName()))),
                        Enum.class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            boolean isFixed = doc.getBoolean(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()));
                            List<Document> docList = (List<Document>) doc.get(nvc.getName());

                            if (nvb instanceof NVGetNameValueList) {
                                if (docList != null) {
                                    for (int i = 0; i < docList.size(); i++) {
                                        ((NVGetNameValueList) nvb).add(mds.toNVPair(subjectGUID, container, docList.get(i)));
                                    }
                                }
                                ((NVGetNameValueList) nvb).setFixed(isFixed);
                            } else {
                                ArrayValues<NVPair> arrayValues = (ArrayValues<NVPair>) nvb;

                                if (docList != null) {
                                    for (Document docTem : docList) {
                                        arrayValues.add(mds.toNVPair(subjectGUID, container, docTem));
                                    }
                                }

                                if (nvb instanceof NVPairList)
                                    ((NVPairList) nvb).setFixed(isFixed);
                            }
                        },
                        String[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            List<Long> values = new ArrayList<Long>();
                            for (Object val : (List<String>) doc.get(nvc.getName())) {
                                values.add((Long) val);
                            }
                            ((NVLongList) nvb).setValue(values);
                        },
                        long[].class, Long[].class, Date[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            List<BigDecimal> ret = new ArrayList<BigDecimal>();
                            for (String val : (List<String>) doc.get(nvc.getName())) {
                                ret.add(new BigDecimal(val));
                            }
                            ((NVBigDecimalList) nvb).setValue(ret);
                        },
                        BigDecimal[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            List<Double> values = new ArrayList<Double>();
                            for (Object val : (List<Document>) doc.get(nvc.getName())) {
                                values.add((Double) val);
                            }
                            ((NVDoubleList) nvb).setValue(values);
                        },
                        double[].class, Double[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            List<Float> values = new ArrayList<Float>();
                            for (Object val : (List<Document>) doc.get(nvc.getName())) {
                                if (val instanceof Double)
                                    val = ((Double) val).floatValue();

                                values.add((Float) val);
                            }
                            ((NVFloatList) nvb).setValue(values);
                        },
                        float[].class, Float[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            List<Integer> values = new ArrayList<Integer>();
                            for (Object val : (List<Document>) doc.get(nvc.getName())) {
                                values.add((Integer) val);
                            }
                            ((NVIntList) nvb).setValue(values);
                        },
                        int[].class, Integer[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            List<String> listOfEnumValues = (List<String>) doc.get(nvc.getName());
                            List<Enum<?>> listOfEnums = new ArrayList<Enum<?>>();

                            for (String enumValue : listOfEnumValues) {
                                listOfEnums.add(SharedUtil.enumValue(nvc.getMetaTypeBase(), enumValue));
                            }

                            ((NVEnumList) nvb).setValue(listOfEnums);
                        },
                        Enum[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                            boolean isFixed = doc.getBoolean(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()));

                            List<Document> list = (List<Document>) doc.get(nvc.getName());

                            //List<NVPair> nvpl = new ArrayList<NVPair>();
                            if (nvb instanceof NVGetNameValueList) {
                                if (list != null) {
                                    for (int i = 0; i < list.size(); i++) {
                                        ((NVGetNameValueList) nvb).add(mds.toNVPair(subjectGUID, container, list.get(i)));
                                    }
                                }
                                ((NVGetNameValueList) nvb).setFixed(isFixed);
                            } else {
                                ArrayValues<NVPair> arrayValues = (ArrayValues<NVPair>) nvb;

                                if (list != null) {
                                    for (int i = 0; i < list.size(); i++) {
                                        arrayValues.add(mds.toNVPair(subjectGUID, container, list.get(i)));
                                    }
                                }

                                if (nvb instanceof NVPairList)
                                    ((NVPairList) nvb).setFixed(isFixed);
                            }
                        },
                        String[].class)
                .map((mds, subjectGUID, db, doc, container, nvc, nvb) -> {
                    NamedValue namedValue = (NamedValue<?>) nvb;
                    Document nvDoc = (Document) doc.get(nvc.getName());
                    namedValue.setValue(nvDoc.get(MetaToken.VALUE.getName()));
                    lookupDataDeserializer(NVGenericMap.class).deserialize(mds, subjectGUID, db, nvDoc, container, null, namedValue.getProperties());
                }, NamedValue.class)


        ;

    }


    public DataDeserializer lookupDataDeserializer(Class<?> clazz) {
        return BSONToDataDeserializer.get(clazz);
    }

//    public <V> GetNameValue<V> idToGNV(String idToGuess) {
//        SUS.checkIfNulls("null idToGuess", idToGuess);
//        return (GetNameValue<V>) GetNameValue.create(ReservedID.GUID.getValue(), UUID.fromString(idToGuess));

    ////        try {
    ////            return (GetNameValue<V>) GetNameValue.create(ReservedID.REFERENCE_ID.getValue(), new ObjectId(idToGuess));
    ////        } catch (Exception e) {
    ////            return (GetNameValue<V>) GetNameValue.create(ReservedID.GUID.getValue(), UUID.fromString(idToGuess));
    ////        }
//    }
    public Document idAsGUID(NVEntity nve) {
        return new Document("_id", IDGs.UUIDV4.decode(nve.getReferenceID()));
    }

    //    public Document idAsRefID(NVEntity nve) {
//        return new Document(ReservedID.REFERENCE_ID.getValue(), nveRefID(nve));
//    }
    public UUID getRefIDAsUUID(Document doc) {
        return doc.get("_id", UUID.class);
    }


//    public static void fillNVBDocs()
//    {
//        nvbToDocument.put(NVPairList.class, new BiConsumer<NVPairList, Document>() {
//            /**
//             * Performs this operation on the given arguments.
//             *
//             * @param nvPairList the first input argument
//             * @param document   the second input argument
//             */
//            @Override
//            public void accept(NVPairList nvPairList, Document document) {
//                if (nvPairList.isFixed())
//                    document.append(SharedUtil.toCanonicalID('_', nvPairList.getName(), MetaToken.IS_FIXED.getName()), nvPairList.isFixed());
//                document.append(nvPairList.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false));
//            }
//        });
//
//        if (nvb instanceof NVPairList) {
//            if (((NVPairList) nvb).isFixed())
//                doc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
//            doc.append(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false));
//        } else if (nvb instanceof NVGetNameValueList) {
//            if (((NVGetNameValueList) nvb).isFixed())
//                doc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVGetNameValueList) nvb).isFixed());
//            doc.append(nvc.getName(), mapArrayValuesNVGetNameValueString(nve, (ArrayValues<GetNameValue<String>>) nvb, false));
//        } else if (nvb instanceof NVPairGetNameMap) {
//            //if(log.isEnabled()) log.getLogger().info("WE have NVPairGetNameMap:" + nvb.getName() + ":" +nvc);
//            //doc.append(MetaToken.IS_FIXED.getName(), ((NVPairList) nvb).isFixed());
//            List<Document> vals = mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false);
//            doc.append(nvc.getName(), vals);
//            //if(log.isEnabled()) log.getLogger().info("vals:" + vals);
//        } else if (nvb instanceof NVEnum) {
//            doc.append(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
//        } else if (nvb instanceof NVEnumList) {
//            doc.append(nvc.getName(), mapEnumList((NVEnumList) nvb));
//        } else if (nvb instanceof NVStringList) {
//            doc.append(nvc.getName(), ((NVStringList) nvb).getValue());
//        } else if (nvb instanceof NVStringSet) {
//            doc.append(nvc.getName(), ((NVStringSet) nvb).getValue());
//        }
//    }
}
