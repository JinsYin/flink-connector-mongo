package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.bson.*;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RowDataMongoSerializationConverter implements MongoDeserializationConverter<RowData> {

    // ~ instance fields -------------------------------------------

    private final RowType rowType;
    private final MongoSerializationSetter[] toExternalConverters;   // a set of serialization functions

    // ~ constructor ------------------------------------------------

    public RowDataMongoSerializationConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.toExternalConverters = new MongoSerializationSetter[rowType.getFieldCount()];

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalConverters[i] = createNullableExternalSetter(rowType.getTypeAt(i), fieldNames.get(i));
        }
    }

    @Override
    public RowData toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }

    // --------------------------------------------------------------
    // Function interfaces
    // --------------------------------------------------------------

    @FunctionalInterface
    private interface MongoSerializationSetter extends Serializable {
        void serialize(BsonDocument doc, int pos, RowData row);
    }

    @FunctionalInterface
    private interface MongoSerializationConverter extends Serializable {
        BsonValue serialize(Object value);
    }

    // --------------------------------------------------------------
    // Serialization setters
    // --------------------------------------------------------------

    private MongoSerializationSetter createNullableExternalSetter(LogicalType type, String fieldName) {
        return wrapIntoNullableExternalSetter(createNotNullExternalSetter(type, fieldName), type, fieldName);
    }

    private MongoSerializationSetter wrapIntoNullableExternalSetter(
            MongoSerializationSetter setter, LogicalType type, String fieldName) {
        return (doc, pos, row) -> {
            if (row == null || row.isNullAt(pos) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                doc.append(fieldName, new BsonNull());
            } else {
                setter.serialize(doc, pos, row);
            }
        };
    }

    private MongoSerializationSetter createNotNullExternalSetter(LogicalType type, String fieldName) {
        return (doc, pos, row) -> {
            Object value = ((GenericRowData) row).getField(pos);
            doc.append(fieldName, createNullableExternalConverter(type).serialize(value));
        };
    }

    // --------------------------------------------------------------
    // Serialization converters
    // --------------------------------------------------------------

    private MongoSerializationConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createNotNullExternalConverter(type), type);
    }

    private MongoSerializationConverter wrapIntoNullableExternalConverter(
            MongoSerializationConverter converter, LogicalType type) {
        return value -> {
            if (value == null || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                return new BsonNull();
            } else {
                return converter.serialize(value);
            }
        };
    }

    private MongoSerializationConverter createNotNullExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return value -> new BsonBoolean((boolean) value);
            case DOUBLE:
                return value -> new BsonDouble((double) value);
            case BINARY:
            case VARBINARY:
                return value -> new BsonBinary((byte[]) value);
            case TINYINT:
                return value -> new BsonBinary(new byte[]{(byte) value});
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value -> new BsonInt32((int) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value -> new BsonInt64((long) value);
            case DATE:
                return value -> {
                    // Describes the number of days since epoch.
                    int days = (int) value;
                    long milliseconds = TimeUnit.DAYS.toMillis(days);
                    return new BsonDateTime(milliseconds);
                };
            case TIME_WITHOUT_TIME_ZONE:
                return value -> {
                    // Describes the number of milliseconds of the day.
                    int milliseconds = (int) value;
                    return new BsonDateTime(milliseconds);
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value -> {
                    final int precision = ((TimestampType) type).getPrecision();
                    TimestampData tsData = (TimestampData) value;
                    long milliseconds = tsData.getMillisecond();
                    return new BsonTimestamp(milliseconds);
                };
            case CHAR:
            case VARCHAR:
                return value -> {
                    StringData stringData = (StringData) value;
                    return new BsonString(stringData.toString());
                };
            case ROW:
                return createExternalRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createExternalMapConverter((MapType) type);
            case ARRAY:
                return createExternalArrayConverter((ArrayType) type);
            case DECIMAL:
                return createExternalDecimalConverter((DecimalType) type);
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
     * value -> new BsonDecimal128(new Decimal128(BigDecimal))
     *
     * see org.apache.flink.table.data.DecimalDataTest
     */
    private MongoSerializationConverter createExternalDecimalConverter(DecimalType decimalType) {
        return (Object value) -> {
            final int precision = decimalType.getPrecision(); // schema precision
            final int scale = decimalType.getScale(); // schema scale

            DecimalData decimalData = value instanceof BigInteger ?
                    DecimalData.fromBigDecimal(new BigDecimal((BigInteger) value, 0), precision, scale) :
                    DecimalData.fromBigDecimal((BigDecimal) value, precision, scale);

            // precision and scale from data
            // decimalData.precision();
            // decimalData.scale();

            BigDecimal bigDecimal = decimalData.toBigDecimal();
            return new BsonDecimal128(new Decimal128(bigDecimal));
        };
    }

    /**
     * value -> new BsonDocument(List<BsonValue>)
     */
    private MongoSerializationConverter createExternalRowConverter(RowType rowType) {
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        final LogicalType[] fieldTypes = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);

        final List<MongoSerializationSetter> fieldSetters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldSetters.add(createNullableExternalSetter(fieldTypes[i], fieldNames[i]));
        }

        return (Object value) -> {
            RowData row = (RowData) value;
            BsonDocument doc = new BsonDocument();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                fieldSetters.get(i).serialize(doc, i, row);
            }
            return doc;
        };
    }

    /**
     * value -> new BsonArray(List<BsonValue>)
     */
    private MongoSerializationConverter createExternalArrayConverter(ArrayType arrayType) {
        final LogicalType elementType = arrayType.getElementType();
        final MongoSerializationConverter elementConverter = createNotNullExternalConverter(elementType);

        return (Object value) -> {
            ArrayData arrayData = (ArrayData) value;
            List<BsonValue> bsonValues = new ArrayList<>();
            for (int i = 0; i < arrayData.size(); i++) {
                Object element = ArrayData.createElementGetter(elementType).getElementOrNull(arrayData, i);
                BsonValue bsonValue = elementConverter.serialize(element);
                bsonValues.add(bsonValue);
            }
            return new BsonArray(bsonValues);
        };
    }

    /**
     * value -> new BsonDocument(List<BsonElement>)
     */
    private MongoSerializationConverter createExternalMapConverter(MapType mapType) {
        final LogicalType keyType = mapType.getKeyType();
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "MongoDB Connector doesn't support non-string as key type of map. " +
                            "The map type is: " + mapType.asSummaryString());
        }
        final LogicalType valType = mapType.getValueType();
        final MongoSerializationConverter valConverter = createNullableExternalConverter(valType);

        return (Object value) -> {
            MapData mapData = (MapData) value;

            ArrayData keyArray = mapData.keyArray();
            ArrayData valArray = mapData.valueArray();

            List<BsonElement> bsonElements = new ArrayList<>();
            for (int i = 0; i < mapData.size(); i++) {
                String key = keyArray.getString(i).toString(); // key must be string
                Object val = ArrayData.createElementGetter(valType).getElementOrNull(valArray, i);
                BsonElement bsonElement = new BsonElement(key, valConverter.serialize(val));
                bsonElements.add(bsonElement);
            }
            return new BsonDocument(bsonElements);
        };
    }
}
