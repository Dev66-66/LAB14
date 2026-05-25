package arrow_flight

import "github.com/apache/arrow/go/v15/arrow"

// EventSchema возвращает Arrow Schema для UserEvent (7 колонок).
func EventSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "user_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "session_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "page", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "duration_ms", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: false},
	}, nil)
}

// WindowSchema возвращает Arrow Schema для AggregatedWindow (5 колонок).
func WindowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "window_start", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "window_end", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "total_events", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "unique_users", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "avg_duration_ms", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, nil)
}
