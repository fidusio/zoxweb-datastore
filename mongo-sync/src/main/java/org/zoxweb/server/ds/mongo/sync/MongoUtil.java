package org.zoxweb.server.ds.mongo.sync;

import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.zoxweb.shared.util.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.UUID;

public class MongoUtil {
    private MongoUtil() {
    }

//    protected final static Map<Class<?>, BiConsumer<?,?>> nvbToDocument = new LinkedHashMap<>();

    private final static HashMap<String, ReservedID> reservedIDMap = new LinkedHashMap<>();

    public static BsonBinary bsonNVEGUID(NVEntity nve) {
        return new BsonBinary(UUID.fromString(nve.getGUID()));
    }

    public static UUID nveGUID(NVEntity nve) {
        return UUID.fromString(nve.getGUID());
    }

    public static ObjectId nveRefID(NVEntity nve) {
        return new ObjectId(nve.getReferenceID());
    }

    public static <V> V guessID(String idToGuess)
    {
        SUS.checkIfNulls("null idToGuess", idToGuess);
        try
        {
            return (V) new ObjectId(idToGuess);
        }
        catch (Exception e)
        {
            return (V) UUID.fromString(idToGuess);
        }
    }

    public static <V> GetNameValue<V> idToGNV(String idToGuess)
    {
        SUS.checkIfNulls("null idToGuess", idToGuess);
        try
        {
            return (GetNameValue<V>) GetNameValue.create(ReservedID.REFERENCE_ID.getValue(), new ObjectId(idToGuess));
        }
        catch (Exception e)
        {
            return (GetNameValue<V>) GetNameValue.create(ReservedID.GUID.getValue(), UUID.fromString(idToGuess));
        }
    }

    public static Document idAsGUID(NVEntity nve) {
        return new Document("_id", nveGUID(nve));
    }

    public static Document idAsRefID(NVEntity nve) {
        return new Document(ReservedID.REFERENCE_ID.getValue(), nveRefID(nve));
    }

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
            reservedIDMap.put(name, this);
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
            return reservedIDMap.get(gn.getName());
        }

        public static ReservedID lookupByName(String name) {
            return reservedIDMap.get(name);
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
