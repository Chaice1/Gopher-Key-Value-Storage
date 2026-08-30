package wal

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/storage"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte)
	Delete(string) error
}

type Wal struct {
	fileLog      *os.File
	fileMetaData *os.File
	mu           *sync.Mutex
	logger       *slog.Logger
	s            Storage
}

func NewWal(pathLog, pathMetaData string, logger *slog.Logger, s Storage) (*Wal, error) {

	fLog, err := os.OpenFile(pathLog, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fMetaData, err := os.OpenFile(pathMetaData, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{
		fileLog:      fLog,
		fileMetaData: fMetaData,
		mu:           &sync.Mutex{},
		logger:       logger,
		s:            s,
	}, nil
}

func (w *Wal) Write(lastLogIdx int64, term uint64, op byte, key string, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keyLen := uint32(len(key))
	valLen := uint32(len(val))

	bufLen := 8 + 8 + 1 + 4 + 4 + len(key) + len(val)

	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(lastLogIdx))
	binary.LittleEndian.PutUint64(buf[8:16], term)
	buf[16] = op
	binary.LittleEndian.PutUint32(buf[17:21], keyLen)
	binary.LittleEndian.PutUint32(buf[21:25], valLen)

	copy(buf[25:25+keyLen], storage.FromStringToBytes(key))
	copy(buf[25+keyLen:], val)

	if _, err := w.fileLog.Write(buf); err != nil {
		return err
	}

	return w.fileLog.Sync()

}

func (w *Wal) RecoverLogHistory() ([]cluster.LogEntry, error) {

	if _, err := w.fileLog.Seek(0, 0); err != nil {
		return nil, err
	}
	log := make([]cluster.LogEntry, 0)
	header := make([]byte, 25)
	for {

		_, err := io.ReadFull(w.fileLog, header)

		if err == io.EOF {
			break
		}

		if err != nil {
			w.logger.Error("failed to read bytes from file", "error", err)
			return nil, err
		}
		logIdx := int64(binary.LittleEndian.Uint64(header[0:8]))
		term := binary.LittleEndian.Uint64(header[8:16])
		op := header[16]
		keyLen := binary.LittleEndian.Uint32(header[17:21])
		valLen := binary.LittleEndian.Uint32(header[21:25])

		BufKey := make([]byte, keyLen)
		BufVal := make([]byte, valLen)

		if _, err := io.ReadFull(w.fileLog, BufKey); err != nil {
			w.logger.Error("failed to read bytes from file", "error", err)
			return nil, err
		}
		if _, err := io.ReadFull(w.fileLog, BufVal); err != nil {
			w.logger.Error("failed to read bytes from file", "error", err)
			return nil, err
		}

		key := string(BufKey)
		logEntry := cluster.LogEntry{
			Term:    term,
			Command: op,
			Key:     key,
			Value:   BufVal,
		}
		if logIdx >= int64(len(log)) {
			log = append(log, logEntry)
		} else {
			log = append(log[:logIdx], logEntry)
		}
	}

	_, err := w.fileLog.Seek(0, 2)
	return log, err
}

func (w *Wal) WriteMetaData(term uint64, votedFor string, lastCommitedIdx int64) error {

	if _, err := w.fileMetaData.Seek(0, 0); err != nil {
		return err
	}

	if err := w.fileMetaData.Truncate(0); err != nil {
		return err
	}
	buf := make([]byte, 20+len(votedFor))
	binary.LittleEndian.PutUint64(buf[0:8], term)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(votedFor)))
	binary.LittleEndian.PutUint64(buf[12:20], uint64(lastCommitedIdx))
	copy(buf[20:], storage.FromStringToBytes(votedFor))

	if _, err := w.fileMetaData.Write(buf); err != nil {
		return err
	}

	return w.fileMetaData.Sync()

}

func (w *Wal) RecoverMetaData() (uint64, string, int64, error) {
	if _, err := w.fileMetaData.Seek(0, 0); err != nil {
		return 0, "", -1, err
	}

	var term uint64
	var votedFor string
	header := make([]byte, 20)
	_, err := io.ReadFull(w.fileMetaData, header)
	if err == io.EOF {
		return 0, "", -1, nil
	}
	if err != nil {
		w.logger.Error("failed to read bytes from file", "error", err)
		return 0, "", -1, err
	}

	term = binary.LittleEndian.Uint64(header[0:8])
	votedForLen := binary.LittleEndian.Uint32(header[8:12])
	lastCommitedIdx := binary.LittleEndian.Uint64(header[12:])

	bufVotedFor := make([]byte, votedForLen)

	if _, err := io.ReadFull(w.fileMetaData, bufVotedFor); err != nil {
		w.logger.Error("failed to read bytes from file", "error", err)
		return 0, "", -1, err
	}

	votedFor = string(bufVotedFor)
	_, err = w.fileMetaData.Seek(0, 2)
	return term, votedFor, int64(lastCommitedIdx), err
}

func (w *Wal) Close() error {
	w.fileLog.Close()
	w.fileMetaData.Close()
	return nil
}
