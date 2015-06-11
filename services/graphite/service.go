package graphite

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	udpBufferSize = 65536
)

type Service struct {
	bindAddress      string
	database         string
	protocol         string
	batchSize        int
	batchTimeout     time.Duration
	consistencyLevel cluster.ConsistencyLevel

	parser *Parser

	logger *log.Logger

	ln   net.Listener
	addr net.Addr

	wg   sync.WaitGroup
	done chan struct{}

	PointsWriter interface {
		WritePoints(p *cluster.WritePointsRequest) error
	}
	MetaStore interface {
		CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error)
	}
}

// NewService returns an instance of the Graphite service.
func NewService(c Config) (*Service, error) {
	// Use defaults where necessary.
	d := c.WithDefaults()

	s := Service{
		bindAddress:  d.BindAddress,
		database:     d.Database,
		protocol:     d.Protocol,
		batchSize:    d.BatchSize,
		batchTimeout: time.Duration(d.BatchTimeout),
		logger:       log.New(os.Stderr, "[graphite] ", log.LstdFlags),
		done:         make(chan struct{}),
	}

	consistencyLevel, err := cluster.ParseConsistencyLevel(d.ConsistencyLevel)
	if err != nil {
		return nil, err
	}
	s.consistencyLevel = consistencyLevel

	parser := NewParser(d.NameSeparator, d.NameSchema)
	s.parser = parser

	return &s, nil
}

// Open starts the Graphite input processing data.
func (s *Service) Open() error {
	var err error

	if _, err := s.MetaStore.CreateDatabaseIfNotExists(s.database); err != nil {
		s.logger.Printf("failed to ensure target database %s exists: %s", s.database, err.Error())
		return err
	}
	s.logger.Printf("ensured target database %s exists", s.database)

	if strings.ToLower(s.protocol) == "tcp" {
		s.addr, err = s.openTCPServer()
	} else if strings.ToLower(s.protocol) == "udp" {
		s.addr, err = s.openUDPServer()
	} else {
		return fmt.Errorf("unrecognized Graphite input protocol %s", s.protocol)
	}
	if err != nil {
		return err
	}

	s.logger.Printf("%s Graphite input opened on %s", s.protocol, s.addr.String())
	return nil
}

// Close stops all data processing on the Graphite input.
func (s *Service) Close() error {
	close(s.done)
	s.wg.Wait()
	s.done = nil
	if s.ln != nil {
		s.ln.Close()
	}
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.logger = l
}

func (s *Service) Addr() net.Addr {
	return s.addr
}

// openTCPServer opens the Graphite input in TCP mode and starts processing data.
func (s *Service) openTCPServer() (net.Addr, error) {
	ln, err := net.Listen("tcp", s.bindAddress)
	if err != nil {
		return nil, err
	}
	s.ln = ln

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.ln.Accept()
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				s.logger.Println("graphite TCP listener closed")
				return
			}
			if err != nil {
				s.logger.Println("error accepting TCP connection", err.Error())
				continue
			}

			s.wg.Add(1)
			go s.handleTCPConnection(conn)
		}
	}()
	return ln.Addr(), nil
}

// handleTCPConnection services an individual TCP connection for the Graphite input.
func (s *Service) handleTCPConnection(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()

	batcher := tsdb.NewPointBatcher(s.batchSize, s.batchTimeout)
	batcher.Start()
	reader := bufio.NewReader(conn)

	// Start processing batches.
	s.wg.Add(1)
	go s.processBatches(batcher)

	for {
		// Read up to the next newline.
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			batcher.Flush()
			return
		}

		// Trim the buffer, even though there should be no padding
		line := strings.TrimSpace(string(buf))

		// Parse it.
		point, err := s.parser.Parse(line)
		if err != nil {
			s.logger.Printf("unable to parse data: %s", err)
			continue
		}
		batcher.In() <- point
	}
}

// openUDPServer opens the Graphite input in UDP mode and starts processing incoming data.
func (s *Service) openUDPServer() (net.Addr, error) {
	addr, err := net.ResolveUDPAddr("udp", s.bindAddress)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	batcher := tsdb.NewPointBatcher(s.batchSize, s.batchTimeout)
	batcher.Start()

	// Start processing batches.
	s.wg.Add(1)
	go s.processBatches(batcher)

	buf := make([]byte, udpBufferSize)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				batcher.Flush()
				conn.Close()
				return
			}
			for _, line := range strings.Split(string(buf[:n]), "\n") {
				point, err := s.parser.Parse(line)
				if err != nil {
					continue
				}
				batcher.In() <- point
			}
		}
	}()
	return conn.LocalAddr(), nil
}

// processBatches continually drains the given batcher and writes the batches to the database.
func (s *Service) processBatches(batcher *tsdb.PointBatcher) {
	defer s.wg.Done()
	for {
		select {
		case batch := <-batcher.Out():
			if err := s.PointsWriter.WritePoints(&cluster.WritePointsRequest{
				Database:         s.database,
				RetentionPolicy:  "",
				ConsistencyLevel: s.consistencyLevel,
				Points:           batch,
			}); err != nil {
				s.logger.Printf("failed to write point batch to database %q: %s", s.database, err)
			}
		case <-s.done:
			return
		}
	}
}

// Parser encapulates a Graphite Parser.
type Parser struct {
	separator  string
	fieldNames []string
}

// NewParser returns a GraphiteParser instance.
func NewParser(separator, schema string) *Parser {
	p := Parser{
		separator:  separator,
		fieldNames: strings.Split(schema, separator),
	}
	if len(p.fieldNames) == 1 && p.fieldNames[0] == "" {
		p.fieldNames = nil
	}
	return &p
}

// Parse performs Graphite parsing of a single line.
func (p *Parser) Parse(line string) (tsdb.Point, error) {
	// Break into 3 fields (name, value, timestamp).
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return nil, fmt.Errorf("received %q which doesn't have three fields", line)
	}

	// decode the name and tags
	name, tags, err := p.DecodeNameAndTags(fields[0])
	if err != nil {
		return nil, err
	}

	// Parse value.
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, fmt.Errorf("field \"%s\" value: %s", fields[0], err)
	}

	fieldValues := make(map[string]interface{})
	fieldValues["value"] = v

	// Parse timestamp.
	unixTime, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return nil, fmt.Errorf("field \"%s\" time: %s", fields[0], err)
	}

	// Check if we have fractional seconds
	timestamp := time.Unix(int64(unixTime), int64((unixTime-math.Floor(unixTime))*float64(time.Second)))

	point := tsdb.NewPoint(name, tags, fieldValues, timestamp)

	return point, nil
}

// DecodeNameAndTags parses the name and tags of a single field of a Graphite datum.
func (p *Parser) DecodeNameAndTags(metric string) (string, map[string]string, error) {
	// No schema? Just return the metric name as the measurement.
	if len(p.fieldNames) == 0 {
		return metric, nil, nil
	}

	// decode the name and tags
	parts := strings.Split(metric, p.separator)
	tags := make(map[string]string)
	var measurement string

	for i := range parts {
		if i >= len(p.fieldNames) {
			break
		}

		if p.fieldNames[i] == "" {
			continue
		} else if p.fieldNames[i] == "measurement" {
			measurement = parts[i]
		} else {
			tags[p.fieldNames[i]] = parts[i]
		}
	}

	if measurement == "" {
		return measurement, tags, fmt.Errorf("no measurement specified for metric. %q", metric)
	}
	return measurement, tags, nil
}
