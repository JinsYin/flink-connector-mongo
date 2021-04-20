package cn.guruguru.flink.connector.mongo.internal.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

/**
 * GeoJSON + BSON ?
 */
@Internal
public class MongoTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    // ~ instance fields ------------------------------------

    private ObjectIdInfo objectIdInfo;
    /** {@link LogicalType} is serializable, but {@link TableSchema} is not serializable */
    private LogicalType logicalType;

    // ~ getters ---------------------------------------------

    public LogicalType getLogicalType() {
        return logicalType;
    }

    public ObjectIdInfo getObjectIdInfo() {
        return objectIdInfo;
    }

    // ~ methods ---------------------------------------------

    /**
     * Construct a {@link MongoTableSchema} from a {@link TableSchema}.
     */
    public static MongoTableSchema fromTableSchema(TableSchema tableSchema) {
        MongoTableSchema mongoTableSchema = new MongoTableSchema();
        LogicalType logicalType = tableSchema.toPhysicalRowDataType().getLogicalType();
        //tableSchema.getPrimaryKey();

        mongoTableSchema.logicalType = logicalType;
        mongoTableSchema.objectIdInfo = ObjectIdInfo.builder().build();
        return mongoTableSchema;
    }

    // --------------------------------------------------------

    /**
     * MongoDB ObjectId's schema
     */
    public static final class ObjectIdInfo {
        public static final String DEFAULT_OID_NAME = "_id";
        public static final DataType DEFAULT_OID_TYPE = DataTypes.STRING();

        private final String oidName;
        private final DataType oidType;

        public ObjectIdInfo(String oidName, DataType oidType) {
            this.oidName = oidName;
            this.oidType = oidType;
        }

        public String getOidName() {
            return oidName;
        }

        public DataType getOidType() {
            return oidType;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String oidName = DEFAULT_OID_NAME;
            private DataType oidType= DEFAULT_OID_TYPE;

            private Builder setOidName(String oidName) {
                this.oidName = oidName;
                return this;
            }

            private Builder setOidType(DataType oidType) {
                this.oidType = oidType;
                return this;
            }

            public ObjectIdInfo build() {
                return new ObjectIdInfo(oidName, oidType);
            }
        }
    }
}
