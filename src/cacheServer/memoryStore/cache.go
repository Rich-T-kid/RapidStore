package memorystore

import (
	"RapidStore/memoryStore/internal"
	"RapidStore/memoryStore/internal/DS"

	//"RapidStore/memoryStore/internal/server"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	neverExpires = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
)
var (
	// errors
	ErrKeyNotFound     = errors.New("key not found")
	ErrKeyDoesNotExist = errors.New("key does not exist")
	ErrValueWrongType  = func(excp, got any) error {
		return fmt.Errorf("expected value of type %T but got %T", excp, got)
	}
	ErrMaxSizeReached = func(max uint64) error {
		return fmt.Errorf("maximum size of %d reached, cannot add more items", max)
	}
	ErrStructureEmpty = errors.New("data structure is empty")
)

type basicSet struct {
	key   string
	value GeneralValue
}

// LimitedStorage defines methods for managing storage with size limits and eviction policies
// this is mainly for internal use to manage memory and key limits
type LimitedStorage interface {
	CurrentSize() uint64
	Evict()
	Keys() []string // list all keys in the store
}

// KeyInterface defines methods for managing key-value pairs in the cache
type KeyInterface interface {
	SetKey(key string, value any, ttl ...time.Duration)
	GetKey(key string) any
	DeleteKey(key string)
	ExpireKey(key string, duration time.Duration) bool
	TTLKey(key string) (time.Duration, error)
	ExistsKey(key string) bool
	Type(key string) string
	Increment(key string) (int64, error)
	Decrement(key string) (int64, error)
	Append(key string, suffix string) error
	MSet(pairs map[string]any) bool
	LimitedStorage
}

// HashTableManager defines methods for managing hash tables in the cache
type HashTableManager interface {
	HSet(key, field string, value any, duration ...time.Duration) bool // set a field ex: HSET user:!23 name "alice" (bool indicates wheater a new hashset was created)
	HGet(key, field string) (any, error)                               // get a field ex: HGET user:!23 name -> "alice"
	HGetAll(key string) map[string]any                                 // HGETALL user:!23 -> map[name:alice age:30]
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

// Sorted Set (ordered, unique with score) || overwrite duplicates
// each member is unique & associated with a score, stored and ordered by score
type SortedSetManager interface {
	ZAdd(key string, score float64, member string) error       // add member
	ZRemove(key string, member string) error                   // remove member
	ZRange(key string, start, stop int, withScores bool) []any // members ordred by score (lowest -> highest)
	ZRank(key string, member string) (int, bool)               // rank of member (ascending)  -> low score first
	ZRevRank(key string, member string) (int, bool)            // rank of member (descending) -> high score first
	ZScore(key string, member string) (float64, bool)          // score of member
	LimitedStorage
}

type Cache interface {
	//UtilityManager
	KeyInterface
	HashTableManager
	ListManager
	SetManager
	SortedSetManager
	AdaptableState
	//initStore(policy internal.EvictionPolicy, maxSize uint64)
}

func NewCache(option ...func(*RapidStoreServer)) Cache {
	return NewRapidStore(option...)
}

// Sole implementation of the Cache interface
// ---------------- RapidStoreServer ----------------
type RapidStoreServer struct {
	// Keep your managers as named fields for explicit access
	KeyManger       KeyInterface
	HashManager     HashTableManager
	SequenceManager ListManager
	SetM            SetManager
	SortSetM        SortedSetManager
	// Config Info
	MaxMemory      uint64 // Maximum memory usage in bytes
	MaxKeys        uint64 // Maximum number of keys
	EvictionPolicy string // "lru", "lfu", "random", "volatile-lru", etc.

	// Memory monitoring
	MemoryWarningThreshold float64       // Warn at 80% memory usage
	MemoryCheckInterval    time.Duration // How often to check memory usage
}

func NewRapidStore(option ...func(*RapidStoreServer)) *RapidStoreServer {
	var size uint64 = 500
	var policy = "lru"
	t := RapidStoreServer{}
	for _, o := range option {
		o(&t)
	}
	r := &RapidStoreServer{
		KeyManger:       NewKeyStore(size, policy),
		HashManager:     NewFieldStore(size, policy),
		SequenceManager: NewOrderedListStore(size, policy),
		SetM:            NewSetManager(size, policy),
		SortSetM:        newSortedSetStore(size, policy),
		//Utility:                NewCacheUtility(),
		MaxMemory:              t.MaxMemory,
		MaxKeys:                t.MaxKeys,
		EvictionPolicy:         t.EvictionPolicy,
		MemoryWarningThreshold: t.MemoryWarningThreshold,
		MemoryCheckInterval:    t.MemoryCheckInterval,
	}
	return r
}

func WithMaxMemory(max uint64) func(*RapidStoreServer) {
	return func(c *RapidStoreServer) {
		c.MaxMemory = max
	}
}
func WithMaxKeys(max uint64) func(*RapidStoreServer) {
	return func(c *RapidStoreServer) {
		c.MaxKeys = max
	}
}
func WithEvictionPolicy(policy string) func(*RapidStoreServer) {
	return func(c *RapidStoreServer) {
		c.EvictionPolicy = policy
	}
}
func WithMemoryWarningThreashold(cap float64) func(*RapidStoreServer) {
	return func(c *RapidStoreServer) {
		c.MemoryWarningThreshold = cap
	}
}

func WithMemoryCheckInternal(dur time.Duration) func(*RapidStoreServer) {
	return func(c *RapidStoreServer) {
		c.MemoryCheckInterval = dur
	}
}

func (r *RapidStoreServer) SetKey(key string, value any, ttl ...time.Duration) {
	r.KeyManger.SetKey(key, value, ttl...)
}
func (r *RapidStoreServer) GetKey(key string) any { return r.KeyManger.GetKey(key) }
func (r *RapidStoreServer) DeleteKey(key string)  { r.KeyManger.DeleteKey(key) }
func (r *RapidStoreServer) ExpireKey(key string, duration time.Duration) bool {
	return r.KeyManger.ExpireKey(key, duration)
}
func (r *RapidStoreServer) TTLKey(key string) (time.Duration, error) { return r.KeyManger.TTLKey(key) }
func (r *RapidStoreServer) Type(key string) string                   { return r.KeyManger.Type(key) }
func (r *RapidStoreServer) ExistsKey(key string) bool                { return r.KeyManger.ExistsKey(key) }
func (r *RapidStoreServer) Increment(key string) (int64, error)      { return r.KeyManger.Increment(key) }
func (r *RapidStoreServer) Decrement(key string) (int64, error)      { return r.KeyManger.Decrement(key) }
func (r *RapidStoreServer) Append(key string, suffix string) error {
	return r.KeyManger.Append(key, suffix)
}
func (r *RapidStoreServer) MSet(pairs map[string]any) bool { return r.KeyManger.MSet(pairs) }

// HashTableManager methods
func (r *RapidStoreServer) HSet(key, field string, value any, duration ...time.Duration) bool {
	return r.HashManager.HSet(key, field, value, duration...)
}
func (r *RapidStoreServer) HGet(key, field string) (any, error) {
	v, err := r.HashManager.HGet(key, field)
	if err != nil {
		return nil, err
	}
	realVal := v.(GeneralValue)
	return realVal.Value, nil
}
func (r *RapidStoreServer) HGetAll(key string) map[string]any {
	table := r.HashManager.HGetAll(key)
	var res = make(map[string]any)
	for k, v := range table {
		realv := v.(GeneralValue)
		res[k] = realv.Value
	}
	return res
}
func (r *RapidStoreServer) HDel(key, field string)         { r.HashManager.HDel(key, field) }
func (r *RapidStoreServer) HExists(key, field string) bool { return r.HashManager.HExists(key, field) }

// ListManager methods
func (r *RapidStoreServer) LPush(key string, value any) error {
	return r.SequenceManager.LPush(key, value)
}
func (r *RapidStoreServer) RPush(key string, value any) error {
	return r.SequenceManager.RPush(key, value)
}
func (r *RapidStoreServer) LPop(key string) (any, error) { return r.SequenceManager.LPop(key) }
func (r *RapidStoreServer) RPop(key string) (any, error) { return r.SequenceManager.RPop(key) }
func (r *RapidStoreServer) LRange(key string, start, stop int) ([]any, error) {
	return r.SequenceManager.LRange(key, start, stop)
}
func (r *RapidStoreServer) Size(key string) uint { return r.SequenceManager.Size(key) }

// SetManager methods
func (r *RapidStoreServer) SAdd(key string, member any)      { r.SetM.SAdd(key, member) }
func (r *RapidStoreServer) SMembers(key string) []any        { return r.SetM.SMembers(key) }
func (r *RapidStoreServer) SRem(key string, member any) bool { return r.SetM.SRem(key, member) }
func (r *RapidStoreServer) SIsMember(key string, member any) bool {
	return r.SetM.SIsMember(key, member)
}
func (r *RapidStoreServer) SCard(key string) uint { return r.SetM.SCard(key) }

// SortedSetManager methods
func (r *RapidStoreServer) ZAdd(key string, score float64, member string) error {
	return r.SortSetM.ZAdd(key, score, member)
}
func (r *RapidStoreServer) ZRemove(key string, member string) error {
	return r.SortSetM.ZRemove(key, member)
}
func (r *RapidStoreServer) ZRange(key string, start, stop int, withScores bool) []any {
	return r.SortSetM.ZRange(key, start, stop, withScores)
}
func (r *RapidStoreServer) ZRank(key string, member string) (int, bool) {
	return r.SortSetM.ZRank(key, member)
}
func (r *RapidStoreServer) ZRevRank(key string, member string) (int, bool) {
	return r.SortSetM.ZRevRank(key, member)
}
func (r *RapidStoreServer) ZScore(key string, member string) (float64, bool) {
	return r.SortSetM.ZScore(key, member)
}

// UtilityManager methods

// LimitedStorage methods - route to KeyManager by default
func (r *RapidStoreServer) CurrentSize() uint64 { return r.KeyManger.CurrentSize() }
func (r *RapidStoreServer) Evict()              { r.KeyManger.Evict() }
func (r *RapidStoreServer) Keys() []string {
	var allKeys []string

	// Get keys from KeyManager (key-value store)
	allKeys = append(allKeys, r.KeyManger.Keys()...)

	// Get keys from HashManager (field store)
	allKeys = append(allKeys, r.HashManager.Keys()...)

	// Get keys from SequenceManager (list store)
	if orderedListStore, ok := r.SequenceManager.(*OrderedListStore); ok {
		allKeys = append(allKeys, orderedListStore.Keys()...)
	}

	// Get keys from SetManager (unique set store)
	if uniqueSetStore, ok := r.SetM.(*UniqueSetStore); ok {
		allKeys = append(allKeys, uniqueSetStore.Keys()...)
	}

	// Get keys from SortedSetManager (sorted set store)
	if sortedSetStore, ok := r.SortSetM.(*SortedSetStore); ok {
		allKeys = append(allKeys, sortedSetStore.Keys()...)
	}

	return allKeys
}

/* ---------------------- Implements the KeyInterface --------------------- */
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
func (k *keyStore) SetKey(key string, value any, ttl ...time.Duration) {
	var v GeneralValue
	if len(ttl) > 0 {
		v = GeneralValue{
			Value: value,
			TTL:   time.Now().Add(ttl[0])}
	} else {
		v = GeneralValue{
			Value: value,
			TTL:   neverExpires,
		}

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
func (k *keyStore) ExpireKey(key string, duration time.Duration) bool {
	v, ok := k.internalData[key]
	if !ok || !k.validKey(key, v) {
		return false
	}
	v.TTL = time.Now().Add(duration)
	k.internalData[key] = v
	return true
}
func (k *keyStore) TTLKey(key string) (time.Duration, error) {
	v, ok := k.internalData[key]
	if !ok || !k.validKey(key, v) {
		return 0, ErrKeyDoesNotExist
	}

	// Return the actual duration until expiry
	remaining := time.Until(v.TTL)

	// If already expired, return 0
	if remaining < 0 {
		return 0, nil
	}

	return remaining, nil
}

func (k *keyStore) ExistsKey(key string) bool {
	_, ok := k.internalData[key]

	return ok && k.validKey(key, k.internalData[key])
}
func (k *keyStore) Type(key string) string {
	v, ok := k.internalData[key]
	if !ok || !k.validKey(key, v) {
		return "none"
	}
	switch v.Value.(type) {
	case string:
		return "string"
	case int, int8, int16, int32, int64:
		return "integer"
	case float32, float64:
		return "float"
	case bool:
		return "boolean"
	case []any:
		return "list"
	case map[string]any:
		return "hash"
	default:
		var raw = reflect.ValueOf(v.Value)
		return raw.String()
	}
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
		return 0, ErrValueWrongType(int64(0), v.Value) // or handle error appropriately
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
		return 0, ErrValueWrongType(int64(0), val.Type()) // or handle error appropriately
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
		fmt.Printf("Key %s does not exist or is expired. Initializing with suffix: %s\n", key, suffix)
		// If key doesn't exist or is expired, set it to the new value
		newVal := GeneralValue{Value: suffix, TTL: neverExpires}
		k.internalData[key] = newVal
		return nil
	}

	strVal, ok := v.Value.(string)
	if !ok {
		return ErrValueWrongType("string", v.Value) // or handle error appropriately
	}

	v.Value = strVal + suffix
	fmt.Printf("After appending, key: %s has value: %s\n", key, strVal)
	k.internalData[key] = v

	return nil
}
func (k *keyStore) MSet(pairs map[string]any) bool {
	for k1, v := range pairs {
		k.SetKey(k1, v)
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

/* ---------------------- Implements the HashTableManager Interface --------------------- */
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

func (f *FieldStore) HSet(key, field string, value any, TTL ...time.Duration) bool {
	realTTl := time.Duration(0)
	if len(TTL) == 0 {
		realTTl = time.Until(neverExpires)
	} else {
		realTTl = TTL[0]
	}
	v, ok := f.FieldData[key]
	if !ok {
		// if it doesnt exist allocate new map and set the field to the value
		f.FieldData[key] = make(map[string]GeneralValue)
		f.FieldData[key][field] = GeneralValue{Value: value, TTL: time.Now().Add(realTTl)}
		return false
	}
	/// if it exist use the existing hashtable and assign the field to the value
	v[field] = GeneralValue{Value: value, TTL: time.Now().Add(realTTl)}
	return true
}

func (f *FieldStore) HGet(key, field string) (any, error) { // get a field
	v, ok := f.FieldData[key]
	if !ok {
		return nil, ErrKeyDoesNotExist
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

/* ---------------------- Implements the ListManager Interface --------------------- */
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
		return ErrMaxSizeReached(o.maxSize)
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
		return ErrMaxSizeReached(o.maxSize)
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
		return nil, ErrKeyDoesNotExist
	}
	size := v.Size()
	if size <= 0 {
		return nil, ErrStructureEmpty
	}
	nodeVal := v.PopHead().(*DS.Node)
	return nodeVal.Value, nil
}

func (o *OrderedListStore) RPop(key string) (any, error) {
	v, ok := o.internalManager[key]
	if !ok {
		return nil, ErrKeyDoesNotExist
	}
	size := v.Size()
	if size <= 0 {
		return nil, ErrStructureEmpty
	}
	nodeVal := v.PopTail().(*DS.Node)
	return nodeVal.Value, nil
}
func (o *OrderedListStore) LRange(key string, start, stop int) ([]any, error) {
	v, ok := o.internalManager[key]
	if !ok {
		return nil, ErrKeyDoesNotExist
	}
	size := v.Size()
	if size <= 0 {
		return nil, ErrStructureEmpty
	}
	if stop == -1 {
		stop = int(size) - 1
	}
	rangeValues := v.Range(start, stop)
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

/* ---------------------- Implements the SetManager Interface --------------------- */
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

/* ---------------------- Implements the SortedSetManager Interface --------------------- */
type SortedSetStore struct {
	internalStore map[string]DS.BinaryTree[float64]
	mbtoval       map[string]float64
	policy        internal.EvictionPolicy
	maxSize       uint64
}

func newSortedSetStore(size uint64, policy string) *SortedSetStore {
	return &SortedSetStore{
		policy:        internal.EvictionPolicy(topolicy(policy)),
		maxSize:       size,
		internalStore: make(map[string]DS.BinaryTree[float64]),
		mbtoval:       make(map[string]float64),
	}
}

// score is the key and members are the value for the BST
func (s *SortedSetStore) ZAdd(key string, score float64, member string) error {
	BST, ok := s.internalStore[key]
	if !ok {
		t := DS.NewBinaryTree[float64]()
		t.Insert(score, member)
		s.internalStore[key] = t
		s.mbtoval[member] = score
		return nil
	}

	// If member already exists, remove old score
	if oldScore, exists := s.mbtoval[member]; exists {
		BST.Delete(oldScore)
	}

	// Insert new score
	BST.Insert(score, member)
	s.mbtoval[member] = score // always update mapping

	return nil
}

func (s *SortedSetStore) ZRemove(key string, member string) error {
	BST, ok := s.internalStore[key]
	if !ok {
		return fmt.Errorf("key %s does not exist", key)
	}
	score, exists := s.mbtoval[member]
	if !exists {
		return fmt.Errorf("member %s does not exist in key %s", member, key)
	}
	BST.Delete(score)
	delete(s.mbtoval, member)
	return nil
}
func (s *SortedSetStore) ZRange(key string, start, stop int, withScores bool) []any {
	BST, ok := s.internalStore[key]
	if !ok {
		return []any{}
	}

	// Get all nodes in ascending order
	nodes := BST.RangeSearch(-1e18, 1e18) // effectively full range; can adjust depending on type
	if len(nodes) == 0 {
		return []any{}
	}

	// Normalize indices: handle negative start/stop like Redis
	n := len(nodes)
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []any{}
	}

	nodes = nodes[start : stop+1]

	// Build result slice
	var result []any
	for _, node := range nodes {
		if withScores {
			result = append(result, []any{node.Key, node.Value})
		} else {
			result = append(result, node.Value)
		}
	}
	return result
}

func (s *SortedSetStore) ZRank(key string, member string) (int, bool) {
	BST, ok := s.internalStore[key]
	if !ok {
		return -1, false
	}
	score, exists := s.mbtoval[member]
	if !exists {
		return -1, false
	}
	val, exst := BST.Rank(score)
	if !exst {
		return -1, false
	}
	return val, true
}
func (s *SortedSetStore) ZRevRank(key string, member string) (int, bool) {
	BST, ok := s.internalStore[key]
	if !ok {
		return -1, false
	}
	score, exists := s.mbtoval[member]
	if !exists {
		return -1, false
	}
	val, found := BST.RevRank(score)
	if !found {
		return -1, false
	}
	return val, true
}

func (s *SortedSetStore) ZScore(key string, member string) (float64, bool) {
	score, exists := s.mbtoval[member]
	if !exists {
		return 0, false
	}
	return score, true
}
func (s *SortedSetStore) CurrentSize() uint64 { return uint64(len(s.internalStore)) }
func (s *SortedSetStore) Evict()              {}
func (s *SortedSetStore) Keys() []string {
	var res []string
	for key := range s.internalStore {
		res = append(res, key)
	}
	return res
}

func NewSortedSetManager(size uint64, policy string) SortedSetManager {
	return newSortedSetStore(size, policy)
}

// ----------------------- Helper Functions -----------------------
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
