package pgwire

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgproto3/v2"
)

// Standard PostgreSQL Data Type OIDs
const (
	BoolOID    uint32 = 16
	ByteaOID   uint32 = 17
	Int8OID    uint32 = 20
	Int2OID    uint32 = 21
	Int4OID    uint32 = 23
	TextOID    uint32 = 25
	Float4OID  uint32 = 700
	Float8OID  uint32 = 701
	VarcharOID uint32 = 1043
	DateOID    uint32 = 1082
	TimeOID    uint32 = 1083
	TimestampOID uint32 = 1114
	NumericOID uint32 = 1700
)

// SQLiteTypeToOID maps a SQLite column type to a PostgreSQL OID.
func SQLiteTypeToOID(sqlType string) uint32 {
	upper := strings.ToUpper(sqlType)
	switch {
	case strings.Contains(upper, "INT"):
		return Int8OID
	case strings.Contains(upper, "CHAR") || strings.Contains(upper, "TEXT") || strings.Contains(upper, "CLOB"):
		return TextOID
	case strings.Contains(upper, "BLOB"):
		return ByteaOID
	case strings.Contains(upper, "REAL") || strings.Contains(upper, "FLOA") || strings.Contains(upper, "DOUB"):
		return Float8OID
	case strings.Contains(upper, "BOOL"):
		return BoolOID
	case strings.Contains(upper, "NUM") || strings.Contains(upper, "DEC"):
		return NumericOID
	case strings.Contains(upper, "DATE") || strings.Contains(upper, "TIME"):
		return TextOID
	default:
		return TextOID
	}
}

// BuildRowDescription creates pgproto3.RowDescription from sql.ColumnTypes.
func BuildRowDescription(colTypes []*sql.ColumnType) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(colTypes))
	for i, ct := range colTypes {
		typeName := ct.DatabaseTypeName()
		oid := SQLiteTypeToOID(typeName)

		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(ct.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          oid,
			DataTypeSize:         -1, // variable length
			TypeModifier:         -1,
			Format:               0, // 0 = text format
		}
	}
	return &pgproto3.RowDescription{Fields: fields}
}

// FormatValue converts a scanned Go value into wire text bytes.
func FormatValue(val any) []byte {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case int64:
		return strconv.AppendInt(nil, v, 10)
	case int:
		return strconv.AppendInt(nil, int64(v), 10)
	case float64:
		return strconv.AppendFloat(nil, v, 'g', -1, 64)
	case bool:
		if v {
			return []byte("t")
		}
		return []byte("f")
	case time.Time:
		return v.AppendFormat(nil, "2006-01-02 15:04:05.999999-07")
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}
