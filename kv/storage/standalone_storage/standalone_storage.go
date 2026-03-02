package standalone_storage

import (
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/metrics"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		engine: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Engine is already started in NewStandAloneStorage
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engine.NewTransaction(false)
	return &StandAloneStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	start := time.Now()
	defer func() {
		// Record write duration
		metrics.StorageWriteDuration.Observe(time.Since(start).Seconds())
	}()

	wb := new(engine_util.WriteBatch)
	totalBytes := 0
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
			totalBytes += len(data.Key) + len(data.Value)
			metrics.StorageWriteOps.Inc()
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
			totalBytes += len(data.Key)
			metrics.StorageWriteOps.Inc()
		}
	}

	// Record bytes written
	metrics.StorageWriteBytes.Add(float64(totalBytes))

	return wb.WriteToDB(s.engine)
}

// StandAloneStorageReader implements storage.StorageReader interface
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		// Record read duration
		metrics.StorageReadDuration.Observe(time.Since(start).Seconds())
		metrics.StorageReadOps.Inc()
	}()

	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	// Record bytes read
	if val != nil {
		metrics.StorageReadBytes.Add(float64(len(key) + len(val)))
	}

	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
