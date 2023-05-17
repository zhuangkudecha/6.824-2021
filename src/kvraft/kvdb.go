package kvraft

type KvDb struct {
	KV map[string]string
}

func MakeDb() *KvDb {
	return &KvDb{KV: make(map[string]string)}
}

func (db *KvDb) Get(key string) (string, Err) {
	if value, ok := db.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (db *KvDb) Put(key, value string) Err {
	db.KV[key] = value
	return OK
}

func (db *KvDb) Append(key, value string) Err {
	db.KV[key] += value
	return OK
}
