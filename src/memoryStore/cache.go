package memorystore

import (
	"RapidStore/memoryStore/internal"
	"RapidStore/memoryStore/internal/DS"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	neverExpires = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
)

type basicSet struct {
	key   string
	value GeneralValue
}

type LimitedStorage interface {
	CurrentSize() uint64
	Evict()
	Keys() []string // list all keys in the store
}

type KeyInterface interface {
	SetKey(key string, value any)
	GetKey(key string) any
	DeleteKey(key string)
	ExpireKey(key string, duration time.Time) bool
	TTLKey(key string) (time.Duration, error)
	ExistsKey(key string) bool
	WatchKey(key string) <-chan bool // notify when key is modified or deleted
	Increment(key string) (int64, error)
	Decrement(key string) (int64, error)
	Append(key string, suffix string) error
	MSet(pairs ...basicSet) bool
	LimitedStorage
}

type HashTableManager interface {
	HSet(key, field string, value any, duration time.Duration) bool // set a field ex: HSET user:!23 name "alice" (bool indicates wheater a new hashset was created)
	HGet(key, field string) (any, error)                            // get a field ex: HGET user:!23 name -> "alice"
	HGetAll(key string) map[string]any                              // HGETALL user:!23 -> map[name:alice age:30]
	HDel(key, field string)
	HExists(key, field string) bool
	LimitedStorage
}

// List (ordered) no ttl can be provided ; error really only occures if the key doesnt exist
type ListManager interface {
	LPush(key string, value any) error // push start;
	RPush(key string, value any) error // push end;
	LPop(key string) (any, error)      // pop left; if key doesnt exist
	RPop(key string) (any, error)      // pop right
	LRange(key string, start, stop int) ([]any, error)
	Size(key string) uint
	LimitedStorage
}

// Set (unordered, unique)
type SetManager interface {
	SAdd(key string, member any)           // add element to set
	SMembers(key string) []any             // list of elements
	SRem(key string, member any) bool      // remove element
	SIsMember(key string, member any) bool // check membership
	SCard(key string) uint                 // size
	LimitedStorage
}

// Sorted Set (ordered, unique with score)
// each member is unique & associated with a score, stored and ordered by score
type SortedSetManager interface {
	ZAdd(key string, score float64, member any) bool              // add member
	ZRange(key string, start, stop int, withScores bool) []any    // members ordred by score (lowest -> highest)
	ZRevRange(key string, start, stop int, withScores bool) []any // same as Zrange but in reverse (highest -> lowest)
	ZRank(key string, member any) (int, bool)                     // rank of member (ascending)  -> low score first
	ZRevRank(key string, member any)                              // rank of member (descending) -> high score first
	ZScore(key string, member any) (float64, bool)                // score of member
	LimitedStorage
}

type UtilityManager interface {
	Info() map[string]string // server metadata (uptime,timestamp,total keys, memory usage, Write/Read ops, size of Write ahead log, active connections, total request, cpu load, last command)
	Monitor() <-chan string  // chan of all the commands processed by the server (commands and their args)
	FlushDB() bool           // clear current db
	Ping() bool              // check if server is alive
}

type Cache interface {
	UtilityManager
	KeyInterface
	HashTableManager
	ListManager
	SetManager
	SortedSetManager
	initStore(policy internal.EvictionPolicy, maxSize uint64)
}

func NewCache() Cache {
	return nil
}

/* Implements the KeyInterface */
type keyStore struct {
	internalData map[string]GeneralValue
	length       uint64
	policy       internal.EvictionPolicy
}
type GeneralValue struct {
	Value any
	TTL   time.Time
}

func (k *keyStore) validKey(key string, valuePair GeneralValue) bool {
	// If TTL is set to neverExpires, the key is always valid
	if valuePair.TTL.Equal(neverExpires) {
		return true
	}
	// Check if the key has expired
	if time.Now().After(valuePair.TTL) {
		delete(k.internalData, key)
		return false
	}
	return true
}
func (k *keyStore) SetKey(key string, value any) {
	v := GeneralValue{
		Value: value,
		TTL:   neverExpires,
	}
	k.internalData[key] = v
}

func (k *keyStore) GetKey(key string) any {
	v, ok := k.internalData[key]
	if !ok || !k.validKey(key, v) {
		return ""
	}
	return v.Value
}
func (k *keyStore) DeleteKey(key string) {
	delete(k.internalData, key)
}
func (k *keyStore) ExpireKey(key string, duration time.Time) bool {
	v, ok := k.internalData[key]
	if !ok || !k.validKey(key, v) {
		return false
	}
	v.TTL = duration
	k.internalData[key] = v
	return true
}
func (k *keyStore) TTLKey(key string) (time.Duration, error) {
	v, ok := k.internalData[key]
	if !ok || !k.validKey(key, v) {
		return time.Since(time.Now()), errors.New("key does not exist")
	}
	return time.Until(v.TTL), nil
}

func (k *keyStore) ExistsKey(key string) bool {
	_, ok := k.internalData[key]

	return ok && k.validKey(key, k.internalData[key])
}
func (k *keyStore) WatchKey(key string) <-chan bool {
	// TODO: needs other things to be implemented first
	return nil
}

func (k *keyStore) Increment(key string) (int64, error) {
	v, exists := k.internalData[key]
	if !exists || !k.validKey(key, v) {
		// Initialize with 1 if key doesn't exist or is expired
		newVal := GeneralValue{Value: int64(1), TTL: neverExpires}
		k.internalData[key] = newVal
		return 1, nil
	}

	val := reflect.ValueOf(v.Value)
	if !val.Type().ConvertibleTo(reflect.TypeOf(int64(0))) {
		return 0, errors.New("value is not a numeric type")
	}

	// Convert to int64, increment, and store back
	numVal := val.Convert(reflect.TypeOf(int64(0))).Int()
	numVal++
	v.Value = numVal
	k.internalData[key] = v

	return numVal, nil
}
func (k *keyStore) Decrement(key string) (int64, error) {
	v, exists := k.internalData[key]
	if !exists || !k.validKey(key, v) {
		// Initialize with -1 if key doesn't exist or is expired
		newVal := GeneralValue{Value: int64(-1), TTL: neverExpires}
		k.internalData[key] = newVal
		return -1, nil
	}

	val := reflect.ValueOf(v.Value)
	if !val.Type().ConvertibleTo(reflect.TypeOf(int64(0))) {
		return 0, fmt.Errorf("invalid value type for key %s", key) // or handle error appropriately
	}

	// Convert to int64, decrement, and store back
	numVal := val.Convert(reflect.TypeOf(int64(0))).Int()
	numVal--
	v.Value = numVal
	k.internalData[key] = v

	return numVal, nil
}
func (k *keyStore) Append(key string, suffix string) error {
	v, exists := k.internalData[key]
	if !exists || !k.validKey(key, v) {
		// If key doesn't exist or is expired, set it to the new value
		newVal := GeneralValue{Value: suffix, TTL: neverExpires}
		k.internalData[key] = newVal
		return nil
	}

	strVal, ok := v.Value.(string)
	if !ok {
		return errors.New("key is of invalid type") // or handle error appropriately
	}

	strVal += suffix
	v.Value = strVal
	k.internalData[key] = v

	return nil
}
func (k *keyStore) MSet(pairs ...basicSet) bool {
	for _, pair := range pairs {
		k.SetKey(pair.key, pair.value)
	}
	return true
}
func (k *keyStore) CurrentSize() uint64 {
	return uint64(len(k.internalData))
}
func (k *keyStore) Evict() {
	switch k.policy {
	case internal.NoEviction:
		// Do nothing, new values aren't saved when memory limit is reached
	case internal.LRU:
		// Implement LRU eviction logic here
	case internal.LFU:
		// Implement LFU eviction logic here
	case internal.RANDOM:
		// Implement RANDOM eviction logic here
	case internal.VolatileLRU:
		// Implement Volatile LRU eviction logic here
	case internal.VolatileLFU:
		// Implement Volatile LFU eviction logic here
	case internal.VolatileRandom:
		// Implement Volatile RANDOM eviction logic here
	case internal.VolatileTTL:
		// Implement Volatile TTL eviction logic here
	default:
		panic("unknown eviction policy")
		// Unknown policy, handle error or default behavior
	}
}

// TODO: implement eviction policies
func (k *keyStore) Policy(policy internal.EvictionPolicy) {}
func (k *keyStore) Keys() []string { // list all keys in the store
	var res []string
	for key, value := range k.internalData {
		if k.validKey(key, value) {
			res = append(res, key)
		}
	}
	return res
}
func NewKeyStore(maxSize uint64, policy string) KeyInterface {
	return &keyStore{
		internalData: make(map[string]GeneralValue),
		length:       maxSize,
		policy:       internal.EvictionPolicy(topolicy(policy)),
	}
}

type FieldStore struct {
	FieldData map[string]map[string]GeneralValue //key : {user:(value,ttl)} , name: alice , age: 30
	length    uint64
	policy    internal.EvictionPolicy
}

func (f *FieldStore) validKey(key string, field string, valuePair GeneralValue) bool {
	// If TTL is set to neverExpires, the key is always valid
	if valuePair.TTL.Equal(neverExpires) {
		return true
	}
	// Check if the key has expired
	if time.Now().After(valuePair.TTL) {
		delete(f.FieldData[key], field)
		return false
	}
	return true
}

func (f *FieldStore) HSet(key, field string, value any, TTL time.Duration) bool {
	v, ok := f.FieldData[key]
	if !ok {
		// if it doesnt exist allocate new map and set the field to the value
		f.FieldData[key] = make(map[string]GeneralValue)
		f.FieldData[key][field] = GeneralValue{Value: value, TTL: time.Now().Add(TTL)}
		return false
	}
	/// if it exist use the existing hashtable and assign the field to the value
	v[field] = GeneralValue{Value: value, TTL: time.Now().Add(TTL)}
	return true
}

func (f *FieldStore) HGet(key, field string) (any, error) { // get a field
	v, ok := f.FieldData[key]
	if !ok {
		return nil, errors.New("key doesnt exist")
	}
	x, exist := v[field]
	if !exist || !f.validKey(key, field, x) {
		return nil, fmt.Errorf("field %s of key %s doesnt exist", field, key)
	}
	return x, nil

}

func (f *FieldStore) HGetAll(key string) map[string]any { // HGETALL
	var returnValues = make(map[string]any)
	for k, v := range f.FieldData[key] {
		if !f.validKey(key, k, v) {
			continue
		}
		returnValues[k] = v
	}
	return returnValues
}

func (f *FieldStore) HDel(key, field string) {
	v, ok := f.FieldData[key]
	if !ok || !f.validKey(key, field, v[field]) {
		fmt.Printf("(HDEL):: key %s doesnt exist\n", key)
		return
	}
	delete(v, field)
}
func (f *FieldStore) HExists(key, field string) bool {
	v, ok := f.FieldData[key]
	if !ok || !f.validKey(key, field, v[field]) {
		return false
	}
	_, ok = v[field]
	return ok

}
func (f *FieldStore) CurrentSize() uint64 {
	return uint64(len(f.FieldData))
}
func (f *FieldStore) Evict() {

	switch f.policy {
	case internal.NoEviction:
		// Do nothing, new values aren't saved when memory limit is reached
	case internal.LRU:
		// Implement LRU eviction logic here
	case internal.LFU:
		// Implement LFU eviction logic here
	case internal.RANDOM:
		// Implement RANDOM eviction logic here
	case internal.VolatileLRU:
		// Implement Volatile LRU eviction logic here
	case internal.VolatileLFU:
		// Implement Volatile LFU eviction logic here
	case internal.VolatileRandom:
		// Implement Volatile RANDOM eviction logic here
	case internal.VolatileTTL:
		// Implement Volatile TTL eviction logic here
	default:
		panic("unknown eviction policy")
		// Unknown policy, handle error or default behavior
	}
}
func (f *FieldStore) Keys() []string { // list all keys in the store
	var res []string
	for key := range f.FieldData {
		if !f.validKey(key, "", GeneralValue{TTL: neverExpires}) {
			continue
		}
		res = append(res, key)
	}
	return res
}

// NewFieldStore creates a new instance of FieldStore with the specified maximum size and eviction policy.
func NewFieldStore(maxSize uint64, policy string) HashTableManager {
	return &FieldStore{
		FieldData: make(map[string]map[string]GeneralValue),
		length:    maxSize,
		policy:    internal.EvictionPolicy(topolicy(policy)),
	}
}

type OrderedListStore struct {
	internalManager map[string]DS.SequenceStorage
	policy          internal.EvictionPolicy
	maxSize         uint64
}

func NewOrderedListStore(maxSize uint64, policy string) *OrderedListStore {
	return &OrderedListStore{
		internalManager: make(map[string]DS.SequenceStorage),
		policy:          internal.EvictionPolicy(topolicy(policy)),
		maxSize:         maxSize,
	}
}

func (o *OrderedListStore) LPush(key string, value any) error {
	if o.Size(key) >= uint(o.maxSize) {
		return errors.New("list has reached its maximum size")
	}
	v, ok := o.internalManager[key]
	if !ok {
		t := DS.NewSequenceStorage()
		t.AddFirst(value)
		o.internalManager[key] = t
		return nil
	}
	v.AddFirst(value)
	return nil
}
func (o *OrderedListStore) RPush(key string, value any) error {
	if o.Size(key) >= uint(o.maxSize) {
		return errors.New("list has reached its maximum size")
	}
	v, ok := o.internalManager[key]
	if !ok {
		t := DS.NewSequenceStorage()
		t.AddLast(value)
		o.internalManager[key] = t
		return nil
	}
	v.AddLast(value)
	return nil
}
func (o *OrderedListStore) LPop(key string) (any, error) {
	v, ok := o.internalManager[key]
	if !ok {
		return nil, errors.New("key does not exist")
	}
	size := v.Size()
	if size <= 0 {
		return nil, errors.New("list is empty")
	}
	nodeVal := v.PopHead().(*DS.Node)
	return nodeVal.Value, nil
}

func (o *OrderedListStore) RPop(key string) (any, error) {
	v, ok := o.internalManager[key]
	if !ok {
		return nil, errors.New("key does not exist")
	}
	size := v.Size()
	if size <= 0 {
		return nil, errors.New("list is empty")
	}
	nodeVal := v.PopTail().(*DS.Node)
	return nodeVal.Value, nil
}
func (o *OrderedListStore) LRange(key string, start, stop int) ([]any, error) {
	v, ok := o.internalManager[key]
	if !ok {
		return nil, errors.New("key does not exist")
	}
	size := v.Size()
	if size <= 0 {
		return nil, errors.New("list is empty")
	}
	if stop == -1 {
		stop = int(size) - 1
	}
	rangeValues := v.Range(start, stop)
	fmt.Printf("raw range values: %v\n", rangeValues)
	var res []any
	for i := range rangeValues {
		nn := rangeValues[i].(*DS.Node)
		res = append(res, nn.Value)
	}
	return res, nil
}
func (o *OrderedListStore) Size(key string) uint {
	v, ok := o.internalManager[key]
	if !ok {
		return 0
	}
	return v.Size()
}
func (o *OrderedListStore) CurrentSize() uint64 {
	return uint64(len(o.internalManager))
}
func (o *OrderedListStore) Evict() {
	panic("Do not implement me")
}
func (o *OrderedListStore) Keys() []string {
	var res []string
	for key := range o.internalManager {
		res = append(res, key)
	}
	return res
}

func NewListManager(size uint64) ListManager {
	return NewOrderedListStore(size, "no eviction")
}

type UniqueSetStore struct {
	internalManager map[string]map[interface{}]struct{}
	policy          internal.EvictionPolicy
	maxSize         uint64
}

func NewUniqueSetStore(maxSize uint64, policy string) *UniqueSetStore {
	return &UniqueSetStore{
		internalManager: make(map[string]map[interface{}]struct{}),
		policy:          internal.EvictionPolicy(topolicy(policy)),
		maxSize:         maxSize,
	}
}

func (u *UniqueSetStore) SAdd(key string, member any) {
	if uint(u.CurrentSize()) >= uint(u.maxSize) {
		u.Evict()
	}
	if _, ok := u.internalManager[key]; !ok {
		u.internalManager[key] = make(map[interface{}]struct{})
	}
	u.internalManager[key][member] = struct{}{}
}
func (u *UniqueSetStore) SMembers(key string) []any {
	if _, ok := u.internalManager[key]; !ok {
		return nil
	}
	var res []any
	for member := range u.internalManager[key] {
		res = append(res, member)
	}
	return res
}
func (u *UniqueSetStore) SRem(key string, member any) bool {
	if _, ok := u.internalManager[key]; !ok {
		return false
	}
	if _, exists := u.internalManager[key][member]; !exists {
		return false
	}
	delete(u.internalManager[key], member)
	return true
}
func (u *UniqueSetStore) SIsMember(key string, member any) bool {
	if _, ok := u.internalManager[key]; !ok {
		return false
	}
	_, exists := u.internalManager[key][member]
	return exists
}
func (u *UniqueSetStore) SCard(key string) uint {
	return uint(len(u.internalManager[key]))
}
func (u *UniqueSetStore) CurrentSize() uint64 { return uint64(len(u.internalManager)) }
func (u *UniqueSetStore) Evict() {
	switch u.policy {
	case internal.NoEviction:
		// Do nothing, new values aren't saved when memory limit is reached
	case internal.LRU:
		// Implement LRU eviction logic here
	case internal.LFU:
		// Implement LFU eviction logic here
	case internal.RANDOM:
		// Implement RANDOM eviction logic here
	case internal.VolatileLRU:
		// Implement Volatile LRU eviction logic here
	case internal.VolatileLFU:
		// Implement Volatile LFU eviction logic here
	case internal.VolatileRandom:
		// Implement Volatile RANDOM eviction logic here
	case internal.VolatileTTL:
		// Implement Volatile TTL eviction logic here
	default:
		panic("unknown eviction policy")
		// Unknown policy, handle error or default behavior
	}
}
func (u *UniqueSetStore) Keys() []string { // list all keys in the store
	var res []string
	for key := range u.internalManager {
		res = append(res, key)
	}
	return res
}
func NewSetManager(size uint64, policy string) SetManager {
	return NewUniqueSetStore(size, policy)
}

func topolicy(s string) int {
	switch s {
	case "no eviction":
		return int(internal.NoEviction)
	case "lru":
		return int(internal.LRU)
	case "lfu":
		return int(internal.LFU)
	case "random":
		return int(internal.RANDOM)
	case "volatile lru":
		return int(internal.VolatileLRU)
	case "volatile lfu":
		return int(internal.VolatileLFU)
	case "volatile random":
		return int(internal.VolatileRandom)
	case "volatile ttl":
		return int(internal.VolatileTTL)
	default:
		return int(internal.NoEviction)
	}
}
