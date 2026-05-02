package prefixdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	compressedMetadataMagic   = 0x5a4d4431 // "ZMD1"
	compressedMetadataVersion = 1

	fileNodeVersionBase       = 2
	fileNodeVersionCompressed = 3

	fileNodeHeaderFlagSortedZstd = 1 << 0
)

var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			panic(err)
		}
		return enc
	},
}

var zstdDecoderPool = sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil)
		if err != nil {
			panic(err)
		}
		return dec
	},
}

func compressMetadataZstd(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)
	return enc.EncodeAll(src, make([]byte, 0, len(src))), nil
}

func decompressMetadataZstd(src []byte, expectedSize int) ([]byte, error) {
	if len(src) == 0 {
		if expectedSize == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("empty zstd payload")
	}
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)
	dst := make([]byte, 0, expectedSize)
	out, err := dec.DecodeAll(src, dst)
	if err != nil {
		return nil, err
	}
	if expectedSize >= 0 && len(out) != expectedSize {
		return nil, fmt.Errorf("unexpected decompressed size: got %d want %d", len(out), expectedSize)
	}
	return out, nil
}

func encodeCompressedMetadataBlock(raw []byte) ([]byte, error) {
	compressed, err := compressMetadataZstd(raw)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 12+len(compressed))
	var tmp32 [4]byte
	var tmp16 [2]byte
	binary.BigEndian.PutUint32(tmp32[:], compressedMetadataMagic)
	buf = append(buf, tmp32[:]...)
	binary.BigEndian.PutUint16(tmp16[:], compressedMetadataVersion)
	buf = append(buf, tmp16[:]...)
	buf = append(buf, 0, 0)
	binary.BigEndian.PutUint32(tmp32[:], uint32(len(raw)))
	buf = append(buf, tmp32[:]...)
	buf = append(buf, compressed...)
	return buf, nil
}

func maybeDecodeCompressedMetadataBlock(data []byte) ([]byte, bool, error) {
	if len(data) < 4 || binary.BigEndian.Uint32(data[:4]) != compressedMetadataMagic {
		return data, false, nil
	}
	if len(data) < 12 {
		return nil, true, fmt.Errorf("corrupted compressed metadata header")
	}
	version := binary.BigEndian.Uint16(data[4:6])
	if version != compressedMetadataVersion {
		return nil, true, fmt.Errorf("unsupported compressed metadata version %d", version)
	}
	originalSize := int(binary.BigEndian.Uint32(data[8:12]))
	raw, err := decompressMetadataZstd(data[12:], originalSize)
	if err != nil {
		return nil, true, err
	}
	return raw, true, nil
}

func (h FileNodeHeader) sortedCompressed() bool {
	return h.Version >= fileNodeVersionCompressed && h.Reserved[0]&fileNodeHeaderFlagSortedZstd != 0
}

func (h FileNodeHeader) sortedCompressedSize() int {
	if !h.sortedCompressed() {
		return int(h.SortedEntryCount) * NodeEntrySize
	}
	return int(binary.BigEndian.Uint32(h.Reserved[4:8]))
}

func (h *FileNodeHeader) setSortedCompression(compressed bool, compressedSize int) {
	for i := range h.Reserved {
		h.Reserved[i] = 0
	}
	if compressed {
		h.Version = fileNodeVersionCompressed
		h.Reserved[0] = fileNodeHeaderFlagSortedZstd
		binary.BigEndian.PutUint32(h.Reserved[4:8], uint32(compressedSize))
		return
	}
	if h.Version < fileNodeVersionBase || h.Version == fileNodeVersionCompressed {
		h.Version = fileNodeVersionBase
	}
}

func nodeFileStoredSortedPayloadSize(header FileNodeHeader) (int, error) {
	sortedStored := header.sortedCompressedSize()
	if sortedStored < 0 {
		return 0, fmt.Errorf("negative sorted node file payload size")
	}
	return sortedStored, nil
}

func nodeFileInferUnsortedEntryCount(header FileNodeHeader, payloadSize int) (uint32, error) {
	sortedStored, err := nodeFileStoredSortedPayloadSize(header)
	if err != nil {
		return 0, err
	}
	if payloadSize < sortedStored {
		return 0, fmt.Errorf("node file payload smaller than sorted portion: payload=%d sorted=%d", payloadSize, sortedStored)
	}
	unsortedStored := payloadSize - sortedStored
	if unsortedStored%NodeEntrySize != 0 {
		return 0, fmt.Errorf("node file unsorted payload size %d is not aligned to entry size %d", unsortedStored, NodeEntrySize)
	}
	return uint32(unsortedStored / NodeEntrySize), nil
}

func nodeFileStoredPayloadSize(header FileNodeHeader) (int, error) {
	sortedStored, err := nodeFileStoredSortedPayloadSize(header)
	if err != nil {
		return 0, err
	}
	unsortedStored := int(header.UnsortedEntryCount) * NodeEntrySize
	if unsortedStored < 0 {
		return 0, fmt.Errorf("negative node file payload size")
	}
	return sortedStored + unsortedStored, nil
}

func encodeNodeFilePayload(header *FileNodeHeader, sortedSlice, unsortedSlice []byte, compressSorted bool) ([]byte, error) {
	if header == nil {
		return nil, fmt.Errorf("nil node file header")
	}
	header.SortedEntryCount = uint32(len(sortedSlice) / NodeEntrySize)
	header.UnsortedEntryCount = uint32(len(unsortedSlice) / NodeEntrySize)
	if compressSorted && len(sortedSlice) > 0 {
		compressed, err := compressMetadataZstd(sortedSlice)
		if err != nil {
			return nil, err
		}
		header.setSortedCompression(true, len(compressed))
		payload := make([]byte, 0, len(compressed)+len(unsortedSlice))
		payload = append(payload, compressed...)
		payload = append(payload, unsortedSlice...)
		return payload, nil
	}
	header.setSortedCompression(false, 0)
	payload := make([]byte, 0, len(sortedSlice)+len(unsortedSlice))
	payload = append(payload, sortedSlice...)
	payload = append(payload, unsortedSlice...)
	return payload, nil
}

func decodeNodeFilePayload(header FileNodeHeader, payload []byte) ([]byte, []byte, []byte, error) {
	expectedSorted := int(header.SortedEntryCount) * NodeEntrySize
	if !header.sortedCompressed() {
		if len(payload) < expectedSorted {
			return nil, nil, nil, fmt.Errorf("unexpected node file payload size: got %d want at least %d", len(payload), expectedSorted)
		}
		unsortedStored := len(payload) - expectedSorted
		if unsortedStored%NodeEntrySize != 0 {
			return nil, nil, nil, fmt.Errorf("unexpected unsorted payload size: got %d not aligned to %d", unsortedStored, NodeEntrySize)
		}
		sortedSlice := payload[:expectedSorted]
		unsortedSlice := payload[expectedSorted:]
		return payload, sortedSlice, unsortedSlice, nil
	}
	sortedStored, err := nodeFileStoredSortedPayloadSize(header)
	if err != nil {
		return nil, nil, nil, err
	}
	if sortedStored > len(payload) {
		return nil, nil, nil, fmt.Errorf("short read got %d want at least %d: %w", len(payload), sortedStored, io.ErrUnexpectedEOF)
	}
	unsortedStored := len(payload) - sortedStored
	if unsortedStored%NodeEntrySize != 0 {
		return nil, nil, nil, fmt.Errorf("unexpected unsorted payload size: got %d not aligned to %d", unsortedStored, NodeEntrySize)
	}
	sortedSlice, err := decompressMetadataZstd(payload[:sortedStored], expectedSorted)
	if err != nil {
		return nil, nil, nil, err
	}
	combined := make([]byte, 0, len(sortedSlice)+unsortedStored)
	combined = append(combined, sortedSlice...)
	combined = append(combined, payload[sortedStored:]...)
	return combined, combined[:len(sortedSlice)], combined[len(sortedSlice):], nil
}
