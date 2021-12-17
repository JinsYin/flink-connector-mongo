package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.bson.*;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class for converting {@link BsonDocument} to {@link RowData}
 *
 * @author JinsYin
 */
public class MongoRowDataDeserializationConverter implements MgDeserializationConverter<RowData> {

    // ~ instance fields -------------------------------------------

    private final RowType rowType;
    private final MongoDeserializationSetter[] toInternalSetters; // a set of deserialization functions

    // ~ constructor ------------------------------------------------

    public MongoRowDataDeserializationConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.toInternalSetters = new MongoDeserializationSetter[rowType.getFieldCount()];

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalSetters[i] = createNullableInternalSetter(rowType.getTypeAt(i), fieldNames.get(i));
        }
    }

    // ~ methods -----------------------------------------------------

    @Override
    public RowData toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < bsonDocument.size(); i++) {
            toInternalSetters[i].set(genericRowData, i, bsonDocument);
        }
        return genericRowData;
    }

    @Override
    public RowData toInternal(BsonDocument bsonDocument, RowData rowdata) throws MongoTypeConversionException {
        for (int i = 0; i < bsonDocument.size(); i++) {
            toInternalSetters[i].set((GenericRowData) rowdata, i, bsonDocument);
        }
        return rowdata;
    }

    // --------------------------------------------------------------
    // Function interfaces
    // --------------------------------------------------------------

    @FunctionalInterface
    private interface MongoDeserializationSetter extends Serializable {
        void set(GenericRowData row, int pos, BsonDocument doc); // row.setField(pos, doc.get(fieldNames[pos]))
    }

    @FunctionalInterface
    private interface MongoDeserializationConverter extends Serializable {
        Object deserialize(BsonValue value); // Object: RowData, StringData ....
    }

    // --------------------------------------------------------------
    // Deserialization setters : row.setField(pos, doc.get(fieldNames[pos]))
    // --------------------------------------------------------------

    private MongoDeserializationSetter createNullableInternalSetter(LogicalType type, String fieldName) {
        return wrapIntoNullableInternalSetter(createNotNullInternalSetter(type, fieldName), type, fieldName);
    }

    private MongoDeserializationSetter wrapIntoNullableInternalSetter(
            MongoDeserializationSetter setter, LogicalType type, String fieldName) {
        return (row, pos, doc) -> {
            if (doc == null || doc.isNull(fieldName) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                row.setField(pos, new NullType());
            } else {
                setter.set(row, pos, doc);
            }
        };
    }

    private MongoDeserializationSetter createNotNullInternalSetter(LogicalType type, String fieldName) {
        return (row, pos, doc) -> {
            BsonValue value = doc.get(fieldName);
            row.setField(pos, createNullableInternalConverter(type).deserialize(value));
        };
    }

    // --------------------------------------------------------------
    // Deserialization converters (BsonValue -> Object(RowData/StringData) )
    // --------------------------------------------------------------

    private MongoDeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createNotNullInternalConverter(type));
    }

    private MongoDeserializationConverter wrapIntoNullableInternalConverter(
            MongoDeserializationConverter converter) {
        return bsonValue -> {
            if (bsonValue == null || BsonType.NULL.equals(bsonValue.getBsonType())) {
                return new NullType();
            }
            return converter.deserialize(bsonValue);
        };
    }

    /**
     * @see BsonType
     */
    private MongoDeserializationConverter createNotNullInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return value -> ((BsonBoolean) value).getValue(); // BsonBoolean -> boolean
            case DOUBLE:
                return value -> ((BsonDouble) value).getValue(); // BsonDouble -> double
            case BINARY:
            case VARBINARY:
                return value -> ((BsonBinary) value).getData(); // BsonBinary -> byte[]
            case TINYINT:
                return value -> ((BsonBinary) value).getData()[0]; // BsonBinary -> byte
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value -> ((BsonInt32) value).getValue(); // BsonInt32 -> int
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value -> ((BsonInt64) value).getValue(); // BsonInt64 -> long
            case DATE:
                return createInternalDateConverter((DateType) type);
            case TIME_WITHOUT_TIME_ZONE:
                return createInternalTimeConverter((TimeType) type);
            //case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createInternalTimestampConverter((TimestampType) type);
            case CHAR:
            case VARCHAR:
                return value -> {
                    StringData stringData = (StringData) value;
                    return new BsonString(stringData.toString());
                };
            case ROW:
                return createInternalRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createInternalMapConverter((MapType) type);
            case ARRAY:
                return createInternalArrayConverter((ArrayType) type);
            case DECIMAL:
                return createInternalDecimalConverter((DecimalType) type);
            case SMALLINT:
            case FLOAT:
            case SYMBOL: // BsonSymbol
            case RAW:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * see cn.guruguru.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter#createInternalConverter
     * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/types.html#date">Date</a>
     */
    private MongoDeserializationConverter createInternalDateConverter(DateType dateType) {
        return value -> {
            BsonDateTime bsonDateTime = (BsonDateTime) value;
            long milliseconds = bsonDateTime.getValue();

            // way 1: BsonDateTime -> int
            return (int) TimeUnit.MILLISECONDS.toDays(milliseconds); // Describes the number of days since epoch.

            // way 2: BsonDateTime -> LocalDate
            //return Instant.ofEpochMilli(milliseconds)
            //        .atZone(ZoneId.systemDefault())
            //        .toLocalDate(); // or: return (int) localDate.toEpochDay() // BsonDateTime -> int
        };
    }

    /**
     * see cn.guruguru.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter#createInternalConverter
     * @see <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/types.html#time">Time</a>
     */
    private MongoDeserializationConverter createInternalTimeConverter(TimeType timeType) {
        return value -> {
            BsonDateTime bsonDateTime = (BsonDateTime) value;
            long milliseconds = bsonDateTime.getValue();

            // way 1: BsonDateTime -> int
            return (int) milliseconds; // Describes the number of milliseconds of the day.

            // way 2: BsonDateTime -> LocalTime
            //return Instant.ofEpochMilli(milliseconds)
            //        .atZone(ZoneId.systemDefault())
            //        .toLocalTime(); // or: (int) localTime.toNanoOfDay() / 1_000_000L); // BsonDateTime -> int
        };
    }

    private MongoDeserializationConverter createInternalTimestampConverter(TimestampType tsType) {
        return null;
    }

    private MongoDeserializationConverter createInternalMapConverter(MapType mapType) {
        return null;
    }

    private MongoDeserializationConverter createInternalRowConverter(RowType rowType) {
        return null;
    }

    private MongoDeserializationConverter createInternalArrayConverter(ArrayType arrayType) {
        return null;
    }

    private MongoDeserializationConverter createInternalDecimalConverter(DecimalType decimalType) {
        return null;
    }

    private MongoDeserializationConverter createInternalDecimal128Converter() {
        return value -> {
            BsonDecimal128 bsonDecimal128 = ((BsonDecimal128) value);
            Decimal128 decimal128 = bsonDecimal128.decimal128Value();
            BigDecimal bigDecimal = decimal128.bigDecimalValue();
            int precision = bigDecimal.precision();
            int scale = bigDecimal.scale();
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }
}
