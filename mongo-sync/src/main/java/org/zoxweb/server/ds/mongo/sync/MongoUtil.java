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

    public static Document idAsGUID(NVEntity nve) {
        return new Document("_id", nveGUID(nve));
    }

    public static Document idAsRefID(NVEntity nve) {
        return new Document("_id", nveRefID(nve));
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
}
