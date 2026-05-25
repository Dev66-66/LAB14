package arrow_flight

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/Dev66-66/LAB14/collector/internal/domain"
	arrowpkg "github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const descriptorCmd = "aggregated_windows"

// FlightServer реализует Arrow Flight RPC сервер,
// раздающий накопленные AggregatedWindow Python-клиенту.
type FlightServer struct {
	flight.BaseFlightServer
	windows []domain.AggregatedWindow
	mu      sync.RWMutex
	alloc   *memory.GoAllocator
}

// NewFlightServer создаёт сервер.
func NewFlightServer() *FlightServer {
	return &FlightServer{
		windows: make([]domain.AggregatedWindow, 0, 64),
		alloc:   memory.NewGoAllocator(),
	}
}

// AddWindow добавляет агрегат в буфер сервера (вызывается агрегатором).
func (s *FlightServer) AddWindow(w domain.AggregatedWindow) {
	s.mu.Lock()
	s.windows = append(s.windows, w)
	s.mu.Unlock()
}

// GetFlightInfo реализует FlightServiceServer.
// Дескриптор "aggregated_windows" → возвращает FlightInfo с endpoint.
func (s *FlightServer) GetFlightInfo(ctx context.Context, req *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if req.Type != flight.DescriptorCMD || string(req.Cmd) != descriptorCmd {
		return nil, status.Errorf(codes.NotFound, "unknown flight descriptor: %q", string(req.Cmd))
	}

	schema := WindowSchema()

	s.mu.RLock()
	totalRecords := int64(len(s.windows))
	s.mu.RUnlock()

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.alloc),
		FlightDescriptor: req,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: req.Cmd}},
		},
		TotalRecords: totalRecords,
		TotalBytes:   -1,
	}, nil
}

// DoGet реализует FlightServiceServer.
// Сериализует накопленные окна в Arrow RecordBatch, стримит клиенту
// и очищает буфер после успешной отдачи.
func (s *FlightServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// Атомарно забираем и очищаем буфер.
	s.mu.Lock()
	windows := make([]domain.AggregatedWindow, len(s.windows))
	copy(windows, s.windows)
	s.windows = s.windows[:0]
	s.mu.Unlock()

	schema := WindowSchema()

	bldr := array.NewRecordBuilder(s.alloc, schema)
	defer bldr.Release()

	wsBldr := bldr.Field(0).(*array.TimestampBuilder)
	weBldr := bldr.Field(1).(*array.TimestampBuilder)
	teBldr := bldr.Field(2).(*array.Int64Builder)
	uuBldr := bldr.Field(3).(*array.Int64Builder)
	adBldr := bldr.Field(4).(*array.Float64Builder)

	for _, w := range windows {
		wsBldr.Append(arrowpkg.Timestamp(w.WindowStart.UnixMilli()))
		weBldr.Append(arrowpkg.Timestamp(w.WindowEnd.UnixMilli()))
		teBldr.Append(w.TotalEvents)
		uuBldr.Append(w.UniqueUsers)
		adBldr.Append(w.AvgDuration)
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer w.Close()

	return w.Write(rec)
}

// Serve запускает gRPC сервер на ARROW_FLIGHT_HOST:ARROW_FLIGHT_PORT.
// Останавливается при отмене ctx.
func (s *FlightServer) Serve(ctx context.Context) error {
	host := getEnv("ARROW_FLIGHT_HOST", "0.0.0.0")
	port := getEnv("ARROW_FLIGHT_PORT", "8815")
	addr := fmt.Sprintf("%s:%s", host, port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("arrow flight: listen %s: %w", addr, err)
	}

	grpcSrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcSrv, s)

	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()

	return grpcSrv.Serve(ln)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
