package log

import (
	"fmt"
	api "github.com/rcrupp/proglog/api/v1"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log represents a structured log that stores records in segments.
// The Log type handles operations such as appending records,
// reading records, managing segments, and removing segments.
type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// NewLog creates a new Log instance with the specified directory and configuration.
// If the segment's maxStoreBytes is not set, it defaults to 1024.
// If the segment's maxIndexBytes is not set, it defaults to 1024.
// It returns the created Log instance and an error if any.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// setup initializes the Log instance by reading the files in the directory specified in the Log's Dir field.
// It retrieves the baseOffsets from the filenames of the files, sorts them in ascending order, and creates new segments for each baseOffset.
// If there are no segments, it creates a new segment with the initial offset specified in the Log's Config field.
// It returns an error if any.
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return nil
		}
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

// Append appends a new record to the active segment of the Log.
// It takes a pointer to an api.Record as input and returns the offset of the appended record and an error.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read retrieves the record at the specified offset in the log.
// It acquires a read lock, iterates over the segments in the log, and finds the segment that contains the requested offset.
// If the segment is found, it calls the Read method of the segment to retrieve the record at the specified offset.
// If the segment is not found or the offset is out of range, it returns an error.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

// Close closes the Log instance by locking the mutex to ensure thread safety, and then iterates over each segment in the Log's segments field.
// It calls the Close method on each segment and returns any error that occurs.
// Once all segments are closed, it releases the lock on the mutex and returns nil.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes the Log and removes all files in the Log's directory.
// It returns an error if any.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes the log directory and re-initializes the Log instance by calling the Remove() method and then the setup() method.
// It returns an error if any occurs during the removal or setup process.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset returns the lowest base offset in the Log's segments. It acquires a read lock to ensure concurrent access safety.
// If an error occurs during the operation, it returns zero and the error.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the highest offset in the Log.
// It acquires a read lock on the Log's mutex, retrieves the next offset of the last segment,
// subtracts 1 from it and returns the result.
// If the next offset is 0, indicating an empty log, it returns 0.
// It releases the read lock before returning.
// Error is always nil.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes segments with nextOffset less than or equal to lowest+1 from the Log's segments. If there is any error occurred in the Remove method called on the segment, it returns
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// Reader returns a new io.Reader that reads from the Log's segments.
// It acquires a read lock to ensure concurrent safety and releases it before returning.
// It creates a slice of io.Reader instances, each initialized with an originReader that reads from a segment's store starting at offset 0.
// Finally, it returns an io.MultiReader that reads from all the readers in the slice, in order.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

// originReader represents a reader that reads data from a store starting at a specific offset.
// The store is embedded in the originReader, allowing it to perform read operations using the store's methods.
type originReader struct {
	*store
	off int64
}

// Read reads up to len(p) bytes from the originReader's underlying store at the current offset, starting at o.off.
// It updates o.off by the number of bytes read and returns the number of bytes read and any error encountered.
// This method is implemented by calling the ReadAt method of the store, passing the provided buffer and the current offset.
// The store's ReadAt method reads the bytes at the specified offset from the store's file and returns the number of bytes read and any error encountered.
// If an error occurs during the reading or if the end of the file is reached, the error is propagated.
// The number of bytes read may be less than len(p) if the end of the file is reached before reading the requested number of bytes.
func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// newSegment creates a new segment with the provided offset and configuration.
// It initializes a new store and index for the segment using the directory, baseOffset,
// and Config fields of the Log instance. The store and index files are opened using the
// baseOffset and file extensions, and the segment's nextOffset is calculated based on
// the existing index entries. The segment is added to the Log segments slice, and becomes
// the activeSegment.
// It returns an error if any.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
