package datatype

import "fmt"

// DataType defines the type of data identified by a key prefix.
type DataType int

func (dt DataType) String() string {
	if name, ok := DataTypeStrings[dt]; ok {
		return name
	}
	return fmt.Sprintf("DataType(%d)", dt)
}

const (
	UnknownTypeDataType DataType = iota
	ChtRootDataType
	ChtIndexTableDataType
	ChtTableDataType
	BloomTrieRootDataType
	BloomTrieIndexDataType
	BloomTrieTableDataType
	PreimageDataType
	ConfigDataType
	GenesisStateDataType
	CliqueSnapshotDataType
	BloomBitsIndexDataType
	HeaderDataType
	HeaderNumberDataType
	BlockBodyDataType
	BlockReceiptsDataType
	TransactionLookupMetadataDataType
	BloomBitsDataType
	HeaderTotalDifficultyDataType
	HeaderNumberHashMappingDataType
	SnapshotAccountDataType
	SnapshotStorageDataType
	CodeDataType
	SkeletonHeaderDataType
	TrieNodeAccountDataType
	TrieNodeStorageDataType
	StateIDLookupDataType
	VerkleTrieDataType
	DatabaseVersionDataType
	HeadHeaderDataType
	HeadBlockDataType
	HeadFastBlockDataType
	HeadFinalizedBlockDataType
	PersistentStateIDDataType
	LastPivotDataType
	FastTrieProgressDataType
	SnapshotDisabledDataType
	SnapshotRootDataType
	SnapshotJournalDataType
	SnapshotGeneratorDataType
	SnapshotRecoveryDataType
	SnapshotSyncStatusDataType
	SkeletonSyncStatusDataType
	TrieJournalDataType
	TransactionIndexTailDataType
	FastTransactionLookupLimitDataType
	BadBlockDataType
	UncleanShutdownDataType
	TransitionStatusDataType
	SnapSyncStatusFlagDataType
	LightClientUpdateDataType
	FixedCommitteeRootDataType
	SyncCommitteeDataType
)

var DataTypeStrings = map[DataType]string{
	UnknownTypeDataType:                "UnknownType",
	ChtRootDataType:                    "ChtRoot",
	ChtIndexTableDataType:              "ChtIndexTable",
	ChtTableDataType:                   "ChtTable",
	BloomTrieRootDataType:              "BloomTrieRoot",
	BloomTrieIndexDataType:             "BloomTrieIndex",
	BloomTrieTableDataType:             "BloomTrieTable",
	PreimageDataType:                   "Preimage",
	ConfigDataType:                     "Config",
	GenesisStateDataType:               "GenesisState",
	CliqueSnapshotDataType:             "CliqueSnapshot",
	BloomBitsIndexDataType:             "BloomBitsIndex",
	HeaderDataType:                     "Header",
	HeaderNumberDataType:               "HeaderNumber",
	BlockBodyDataType:                  "BlockBody",
	BlockReceiptsDataType:              "BlockReceipts",
	TransactionLookupMetadataDataType:  "TransactionLookupMetadata",
	BloomBitsDataType:                  "BloomBits",
	HeaderTotalDifficultyDataType:      "HeaderTotalDifficulty",
	HeaderNumberHashMappingDataType:    "HeaderNumberHashMapping",
	SnapshotAccountDataType:            "SnapshotAccountData",
	SnapshotStorageDataType:            "SnapshotStorageData",
	CodeDataType:                       "CodeData",
	SkeletonHeaderDataType:             "SkeletonHeaderData",
	TrieNodeAccountDataType:            "TrieNodeAccountData",
	TrieNodeStorageDataType:            "TrieNodeStorageData",
	StateIDLookupDataType:              "StateIDLookup",
	VerkleTrieDataType:                 "VerkleTrieData",
	DatabaseVersionDataType:            "DatabaseVersion",
	HeadHeaderDataType:                 "HeadHeader",
	HeadBlockDataType:                  "HeadBlock",
	HeadFastBlockDataType:              "HeadFastBlock",
	HeadFinalizedBlockDataType:         "HeadFinalizedBlock",
	PersistentStateIDDataType:          "PersistentStateID",
	LastPivotDataType:                  "LastPivot",
	FastTrieProgressDataType:           "FastTrieProgress",
	SnapshotDisabledDataType:           "SnapshotDisabled",
	SnapshotRootDataType:               "SnapshotRoot",
	SnapshotJournalDataType:            "SnapshotJournal",
	SnapshotGeneratorDataType:          "SnapshotGenerator",
	SnapshotRecoveryDataType:           "SnapshotRecovery",
	SnapshotSyncStatusDataType:         "SnapshotSyncStatus",
	SkeletonSyncStatusDataType:         "SkeletonSyncStatus",
	TrieJournalDataType:                "TrieJournal",
	TransactionIndexTailDataType:       "TransactionIndexTail",
	FastTransactionLookupLimitDataType: "FastTransactionLookupLimit",
	BadBlockDataType:                   "BadBlock",
	UncleanShutdownDataType:            "UncleanShutdown",
	TransitionStatusDataType:           "TransitionStatus",
	SnapSyncStatusFlagDataType:         "SnapSyncStatusFlag",
	LightClientUpdateDataType:          "LightClientUpdate",
	FixedCommitteeRootDataType:         "FixedCommitteeRoot",
	SyncCommitteeDataType:              "SyncCommittee",
}
