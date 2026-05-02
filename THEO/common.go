package ethstore

import (
	"errors"
	"sync"
	"unsafe"

	datatypepkg "theo.local/THEO/datatype"
	"theo.local/THEO/pebblestore"
)

type DataType = datatypepkg.DataType

const (
	UnknownTypeDataType                = datatypepkg.UnknownTypeDataType
	ChtRootDataType                    = datatypepkg.ChtRootDataType
	ChtIndexTableDataType              = datatypepkg.ChtIndexTableDataType
	ChtTableDataType                   = datatypepkg.ChtTableDataType
	BloomTrieRootDataType              = datatypepkg.BloomTrieRootDataType
	BloomTrieIndexDataType             = datatypepkg.BloomTrieIndexDataType
	BloomTrieTableDataType             = datatypepkg.BloomTrieTableDataType
	PreimageDataType                   = datatypepkg.PreimageDataType
	ConfigDataType                     = datatypepkg.ConfigDataType
	GenesisStateDataType               = datatypepkg.GenesisStateDataType
	CliqueSnapshotDataType             = datatypepkg.CliqueSnapshotDataType
	BloomBitsIndexDataType             = datatypepkg.BloomBitsIndexDataType
	HeaderDataType                     = datatypepkg.HeaderDataType
	HeaderNumberDataType               = datatypepkg.HeaderNumberDataType
	BlockBodyDataType                  = datatypepkg.BlockBodyDataType
	BlockReceiptsDataType              = datatypepkg.BlockReceiptsDataType
	TransactionLookupMetadataDataType  = datatypepkg.TransactionLookupMetadataDataType
	BloomBitsDataType                  = datatypepkg.BloomBitsDataType
	HeaderTotalDifficultyDataType      = datatypepkg.HeaderTotalDifficultyDataType
	HeaderNumberHashMappingDataType    = datatypepkg.HeaderNumberHashMappingDataType
	SnapshotAccountDataType            = datatypepkg.SnapshotAccountDataType
	SnapshotStorageDataType            = datatypepkg.SnapshotStorageDataType
	CodeDataType                       = datatypepkg.CodeDataType
	SkeletonHeaderDataType             = datatypepkg.SkeletonHeaderDataType
	TrieNodeAccountDataType            = datatypepkg.TrieNodeAccountDataType
	TrieNodeStorageDataType            = datatypepkg.TrieNodeStorageDataType
	StateIDLookupDataType              = datatypepkg.StateIDLookupDataType
	VerkleTrieDataType                 = datatypepkg.VerkleTrieDataType
	DatabaseVersionDataType            = datatypepkg.DatabaseVersionDataType
	HeadHeaderDataType                 = datatypepkg.HeadHeaderDataType
	HeadBlockDataType                  = datatypepkg.HeadBlockDataType
	HeadFastBlockDataType              = datatypepkg.HeadFastBlockDataType
	HeadFinalizedBlockDataType         = datatypepkg.HeadFinalizedBlockDataType
	PersistentStateIDDataType          = datatypepkg.PersistentStateIDDataType
	LastPivotDataType                  = datatypepkg.LastPivotDataType
	FastTrieProgressDataType           = datatypepkg.FastTrieProgressDataType
	SnapshotDisabledDataType           = datatypepkg.SnapshotDisabledDataType
	SnapshotRootDataType               = datatypepkg.SnapshotRootDataType
	SnapshotJournalDataType            = datatypepkg.SnapshotJournalDataType
	SnapshotGeneratorDataType          = datatypepkg.SnapshotGeneratorDataType
	SnapshotRecoveryDataType           = datatypepkg.SnapshotRecoveryDataType
	SnapshotSyncStatusDataType         = datatypepkg.SnapshotSyncStatusDataType
	SkeletonSyncStatusDataType         = datatypepkg.SkeletonSyncStatusDataType
	TrieJournalDataType                = datatypepkg.TrieJournalDataType
	TransactionIndexTailDataType       = datatypepkg.TransactionIndexTailDataType
	FastTransactionLookupLimitDataType = datatypepkg.FastTransactionLookupLimitDataType
	BadBlockDataType                   = datatypepkg.BadBlockDataType
	UncleanShutdownDataType            = datatypepkg.UncleanShutdownDataType
	TransitionStatusDataType           = datatypepkg.TransitionStatusDataType
	SnapSyncStatusFlagDataType         = datatypepkg.SnapSyncStatusFlagDataType
	LightClientUpdateDataType          = datatypepkg.LightClientUpdateDataType
	FixedCommitteeRootDataType         = datatypepkg.FixedCommitteeRootDataType
	SyncCommitteeDataType              = datatypepkg.SyncCommitteeDataType
)

var DataTypeStrings = datatypepkg.DataTypeStrings

// The fields below define the low level database schema prefixing.
var (
	// databaseVersionKey tracks the current database version.
	databaseVersionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// headFinalizedBlockKey tracks the latest known finalized block hash.
	headFinalizedBlockKey = []byte("LastFinalized")

	// persistentStateIDKey tracks the id of latest stored state(for path-based only).
	persistentStateIDKey = []byte("LastStateID")

	// lastPivotKey tracks the last pivot block used by fast sync (to reenable on sethead).
	lastPivotKey = []byte("LastPivot")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// snapshotDisabledKey flags that the snapshot should not be maintained due to initial sync.
	snapshotDisabledKey = []byte("SnapshotDisabled")

	// SnapshotRootKey tracks the hash of the last snapshot.
	SnapshotRootKey = []byte("SnapshotRoot")

	// snapshotJournalKey tracks the in-memory diff layers across restarts.
	snapshotJournalKey = []byte("SnapshotJournal")

	// snapshotGeneratorKey tracks the snapshot generation marker across restarts.
	snapshotGeneratorKey = []byte("SnapshotGenerator")

	// snapshotRecoveryKey tracks the snapshot recovery marker across restarts.
	snapshotRecoveryKey = []byte("SnapshotRecovery")

	// snapshotSyncStatusKey tracks the snapshot sync status across restarts.
	snapshotSyncStatusKey = []byte("SnapshotSyncStatus")

	// skeletonSyncStatusKey tracks the skeleton sync status across restarts.
	skeletonSyncStatusKey = []byte("SkeletonSyncStatus")

	// trieJournalKey tracks the in-memory trie node layers across restarts.
	trieJournalKey = []byte("TrieJournal")

	// txIndexTailKey tracks the oldest block whose transactions have been indexed.
	txIndexTailKey = []byte("TransactionIndexTail")

	// fastTxLookupLimitKey tracks the transaction lookup limit during fast sync.
	// This flag is deprecated, it's kept to avoid reporting errors when inspect
	// database.
	fastTxLookupLimitKey = []byte("FastTransactionLookupLimit")

	// badBlockKey tracks the list of bad blocks seen by local
	badBlockKey = []byte("InvalidBlock")

	// uncleanShutdownKey tracks the list of local crashes
	uncleanShutdownKey = []byte("unclean-shutdown") // config prefix for the db

	// transitionStatusKey tracks the eth2 transition status.
	transitionStatusKey = []byte("eth2-transition")

	// snapSyncStatusFlagKey flags that status of snap sync.
	snapSyncStatusFlagKey = []byte("SnapSyncStatus")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix        = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix       = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits
	SnapshotAccountPrefix = []byte("a") // SnapshotAccountPrefix + account hash -> account trie value
	SnapshotStoragePrefix = []byte("o") // SnapshotStoragePrefix + account hash + storage hash -> storage trie value
	CodePrefix            = []byte("c") // CodePrefix + code hash -> account code
	skeletonHeaderPrefix  = []byte("S") // skeletonHeaderPrefix + num (uint64 big endian) -> header

	// Path-based storage scheme of merkle patricia trie.
	TrieNodeAccountPrefix = []byte("A") // TrieNodeAccountPrefix + hexPath -> trie node
	TrieNodeStoragePrefix = []byte("O") // TrieNodeStoragePrefix + accountHash + hexPath -> trie node
	stateIDPrefix         = []byte("L") // stateIDPrefix + state root -> state id

	// VerklePrefix is the database prefix for Verkle trie data, which includes:
	// (a) Trie nodes
	// (b) In-memory trie node journal
	// (c) Persistent state ID
	// (d) State ID lookups, etc.
	VerklePrefix = []byte("v")

	PreimagePrefix = []byte("secure-key-")       // PreimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-")  // config prefix for the db
	genesisPrefix  = []byte("ethereum-genesis-") // genesis state prefix for the db

	// BloomBitsIndexPrefix is the data table of a chain indexer to track its progress
	BloomBitsIndexPrefix = []byte("iB")

	ChtPrefix           = []byte("chtRootV2-") // ChtPrefix + chtNum (uint64 big endian) -> trie root hash
	ChtTablePrefix      = []byte("cht-")
	ChtIndexTablePrefix = []byte("chtIndexV2-")

	BloomTriePrefix      = []byte("bltRoot-") // BloomTriePrefix + bloomTrieNum (uint64 big endian) -> trie root hash
	BloomTrieTablePrefix = []byte("blt-")
	BloomTrieIndexPrefix = []byte("bltIndex-")

	CliqueSnapshotPrefix = []byte("clique-")

	BestUpdateKey         = []byte("update-")    // bigEndian64(syncPeriod) -> RLP(types.LightClientUpdate)  (nextCommittee only referenced by root hash)
	FixedCommitteeRootKey = []byte("fixedRoot-") // bigEndian64(syncPeriod) -> committee root hash
	SyncCommitteeKey      = []byte("committee-") // bigEndian64(syncPeriod) -> serialized committee
)

// KeyPrefixInfo stores a prefix and its associated data type.
type KeyPrefixInfo struct {
	Prefix   []byte
	DataType DataType // Changed from string to DataType
}

// keyPrefixes defines the known key prefixes and their data types.
// Order matters for overlapping prefixes: longer/more specific ones should come first.
var keyPrefixes = []KeyPrefixInfo{
	// Specific multi-character prefixes (longer ones first in case of overlap)
	{ChtPrefix, ChtRootDataType},                 // "chtRootV2-"
	{ChtIndexTablePrefix, ChtIndexTableDataType}, // "chtIndexV2-"
	{ChtTablePrefix, ChtTableDataType},           // "cht-"

	{BloomTriePrefix, BloomTrieRootDataType},       // "bltRoot-"
	{BloomTrieIndexPrefix, BloomTrieIndexDataType}, // "bltIndex-"
	{BloomTrieTablePrefix, BloomTrieTableDataType}, // "blt-"

	{PreimagePrefix, PreimageDataType},             // "secure-key-"
	{configPrefix, ConfigDataType},                 // "ethereum-config-"
	{genesisPrefix, GenesisStateDataType},          // "ethereum-genesis-"
	{CliqueSnapshotPrefix, CliqueSnapshotDataType}, // "clique-"
	{BloomBitsIndexPrefix, BloomBitsIndexDataType}, // "iB"

	// Single-character data item prefixes
	{headerPrefix, HeaderDataType},                      // "h"
	{headerNumberPrefix, HeaderNumberDataType},          // "H"
	{blockBodyPrefix, BlockBodyDataType},                // "b"
	{blockReceiptsPrefix, BlockReceiptsDataType},        // "r"
	{txLookupPrefix, TransactionLookupMetadataDataType}, // "l"
	{bloomBitsPrefix, BloomBitsDataType},                // "B"
	{headerTDSuffix, HeaderTotalDifficultyDataType},     // "t" (headerPrefix + num + hash + t -> td)
	{headerHashSuffix, HeaderNumberHashMappingDataType}, // "n" (headerPrefix + num + n -> hash)
	{SnapshotAccountPrefix, SnapshotAccountDataType},    // "a"
	{SnapshotStoragePrefix, SnapshotStorageDataType},    // "o"
	{CodePrefix, CodeDataType},                          // "c"
	{skeletonHeaderPrefix, SkeletonHeaderDataType},      // "S"
	{TrieNodeAccountPrefix, TrieNodeAccountDataType},    // "A"
	{TrieNodeStoragePrefix, TrieNodeStorageDataType},    // "O"
	{stateIDPrefix, StateIDLookupDataType},              // "L"
	{VerklePrefix, VerkleTrieDataType},                  // "v"

	// Full key variables (act as prefixes for themselves in HasPrefix check)
	{databaseVersionKey, DatabaseVersionDataType},
	{headHeaderKey, HeadHeaderDataType},
	{headBlockKey, HeadBlockDataType},
	{headFastBlockKey, HeadFastBlockDataType},
	{headFinalizedBlockKey, HeadFinalizedBlockDataType},
	{persistentStateIDKey, PersistentStateIDDataType},
	{lastPivotKey, LastPivotDataType},
	{fastTrieProgressKey, FastTrieProgressDataType},
	{snapshotDisabledKey, SnapshotDisabledDataType},
	{SnapshotRootKey, SnapshotRootDataType},
	{snapshotJournalKey, SnapshotJournalDataType},
	{snapshotGeneratorKey, SnapshotGeneratorDataType},
	{snapshotRecoveryKey, SnapshotRecoveryDataType},
	{snapshotSyncStatusKey, SnapshotSyncStatusDataType},
	{skeletonSyncStatusKey, SkeletonSyncStatusDataType},
	{trieJournalKey, TrieJournalDataType},
	{txIndexTailKey, TransactionIndexTailDataType},
	{fastTxLookupLimitKey, FastTransactionLookupLimitDataType}, // Deprecated
	{badBlockKey, BadBlockDataType},
	{uncleanShutdownKey, UncleanShutdownDataType},
	{transitionStatusKey, TransitionStatusDataType},
	{snapSyncStatusFlagKey, SnapSyncStatusFlagDataType},
	{BestUpdateKey, LightClientUpdateDataType},          // "update-"
	{FixedCommitteeRootKey, FixedCommitteeRootDataType}, // "fixedRoot-"
	{SyncCommitteeKey, SyncCommitteeDataType},           // "committee-"
}

// TrieNode represents a node in the prefix Trie.
type TrieNode struct {
	Children map[byte]*TrieNode
	DataType DataType // Stores DataType if this node represents the end of a known prefix. Changed from string.
}

// NewTrieNode creates a new TrieNode.
// The DataType will be its zero value (UnknownTypeDataType if it's 0).
func NewTrieNode() *TrieNode {
	return &TrieNode{Children: make(map[byte]*TrieNode), DataType: UnknownTypeDataType}
}

// Insert adds a prefix and its associated DataType to the Trie.
func (t *TrieNode) Insert(prefix []byte, dataType DataType) { // Changed dataType from string
	node := t
	for _, b := range prefix {
		if _, ok := node.Children[b]; !ok {
			node.Children[b] = NewTrieNode()
		}
		node = node.Children[b]
	}
	node.DataType = dataType // Mark the end of the prefix with its DataType
}

// KeyDataTypeMatcher encapsulates a Trie for matching key prefixes to data types.
//
// GetDataType determines the data type of a given key by matching known prefixes
// using the encapsulated Trie.
// It finds the longest prefix of the input key that is registered in the Trie.
type KeyDataTypeMatcher struct {
	prefixTrie *TrieNode
}

// NewKeyDataTypeMatcher creates and initializes a new KeyDataTypeMatcher
// using the global keyPrefixes list.
func NewKeyDataTypeMatcher() *KeyDataTypeMatcher {
	trie := NewTrieNode()
	for _, kpi := range keyPrefixes {
		if len(kpi.Prefix) > 0 {
			trie.Insert(kpi.Prefix, kpi.DataType) // kpi.DataType is now DataType (int)
		}
	}
	return &KeyDataTypeMatcher{prefixTrie: trie}
}

// GetDataType determines the data type of a given key by matching known prefixes
// using the encapsulated Trie.
// It finds the longest prefix of the input key that is registered in the Trie.
func (m *KeyDataTypeMatcher) GetDataType(key []byte) DataType { // Return type changed to DataType
	if m == nil || m.prefixTrie == nil {
		return UnknownTypeDataType // Return UnknownTypeDataType
	}
	node := m.prefixTrie
	longestMatchType := UnknownTypeDataType // Default if no prefix matches at all. Changed from "UnknownType"

	for _, b := range key { // Iterate directly over key []byte
		child, ok := node.Children[b]
		if !ok {
			break
		}
		node = child
		if node.DataType != UnknownTypeDataType { // If this node marks the end of a known prefix (DataType is not its zero value)
			longestMatchType = node.DataType // This is a candidate match
		}
	}
	return longestMatchType
}

// Global instance of the matcher, initialized once.
var (
	defaultKeyMatcher      *KeyDataTypeMatcher
	initDefaultMatcherOnce sync.Once
)

// getDefaultKeyMatcher returns a shared instance of KeyDataTypeMatcher,
// initializing it on the first call.
func getDefaultKeyMatcher() *KeyDataTypeMatcher {
	initDefaultMatcherOnce.Do(func() {
		defaultKeyMatcher = NewKeyDataTypeMatcher()
	})
	return defaultKeyMatcher
}

// GetDataTypeFromKey remains the public API, using the default matcher.
// This maintains backward compatibility and ease of use if a single global matcher is sufficient.
func GetDataTypeFromKey(key []byte) DataType { // Return type changed to DataType
	matcher := getDefaultKeyMatcher()
	return matcher.GetDataType(key) // Call the method on the instance
}

// BytesToString converts a byte slice to a string without memory allocation.
// Note: The string refers to the same memory as the byte slice.
// The byte slice must not be modified while the string is in use.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts a string to a byte slice without memory allocation.
// Note: The byte slice refers to the same memory as the string.
// The byte slice must not be modified.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// ErrClosed and ErrNotFound are re-exported from pebblestore so that all
// packages can use a single sentinel error instance.
var (
	ErrClosed     = pebblestore.ErrClosed
	ErrNotFound   = pebblestore.ErrNotFound
	ErrCompaction = errors.New("compaction error") // Example, if you need more
)

// logEntry represents a single key-value pair within a block in the data log.
// Format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
type logEntry struct {
	BlockID uint64
	Key     string
	Value   string // Can be TombstoneMarker for deletion
	Offset  int64  // Offset in the data file where this entry starts
}

// blockIndexEntry stores the start and end offset for all entries belonging to a block.
// Format on disk: blockID (uint64) | startOffset (uint64) | endOffset (uint64)
type blockIndexEntry struct {
	BlockID     uint64
	StartOffset int64
	EndOffset   int64 // Offset *after* the last byte of the last entry for this block
}

// kvPointer stores the location of a specific key's value in the data log.
// Used as the value in the skiplist.
type kvPointer struct {
	Offset   int64 // Offset of the logEntry start
	ValueLen uint32
	BlockID  uint64 // The block ID this entry belongs to
	// Header keys can store value inline to avoid extra file reads on Get.
	InlineValue    []byte
	HasInlineValue bool
}
