package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{txn: txn, iter: iter}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for scan.iter.Valid() {
		item := scan.iter.Item()
		userKey := DecodeUserKey(item.Key())
		commitTS := decodeTimestamp(item.Key())

		// If commitTS > txn.StartTS, this write is not visible. Seek to a visible version.
		if commitTS > scan.txn.StartTS {
			scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
			continue
		}

		value, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, nil, err
		}

		switch write.Kind {
		case WriteKindPut:
			val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			// Advance past all versions of this key.
			scan.iter.Seek(EncodeKey(userKey, 0))
			return userKey, val, nil
		case WriteKindDelete:
			// Skip this key entirely.
			scan.iter.Seek(EncodeKey(userKey, 0))
			continue
		case WriteKindRollback:
			// Skip this version, look at earlier versions of the same key.
			scan.iter.Next()
			continue
		}
	}
	return nil, nil, nil
}
