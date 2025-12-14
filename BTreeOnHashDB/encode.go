package btreeonhashdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

func metaKey(treeID string) []byte {
	return []byte(fmt.Sprintf("btree:%s:meta", treeID))
}

func nodeKey(treeID string, id NodeID) []byte {
	return []byte(fmt.Sprintf("btree:%s:node:%d", treeID, id))
}

// encodeMeta serializes the tree metadata.
// Layout: [RootNodeID(8)][Height(2)][NextNodeID(8)] little-endian.
func encodeMeta(m *Meta) []byte {
	buf := make([]byte, 18)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(m.RootNodeID))
	binary.LittleEndian.PutUint16(buf[8:10], m.Height)
	binary.LittleEndian.PutUint64(buf[10:18], uint64(m.NextNodeID))
	return buf
}

func decodeMeta(data []byte) (*Meta, error) {
	if len(data) < 18 {
		return nil, fmt.Errorf("meta too short: %d", len(data))
	}
	return &Meta{
		RootNodeID: NodeID(binary.LittleEndian.Uint64(data[0:8])),
		Height:     binary.LittleEndian.Uint16(data[8:10]),
		NextNodeID: NodeID(binary.LittleEndian.Uint64(data[10:18])),
	}, nil
}

// encodeNode serializes a node.
// Header: [Type(1)][NumKeys(2)][NumChildren(2)][NextLeaf(8)][PrevLeaf(8)]
// Keys: [len(4)][bytes]...
// Payload: internal -> children (NumChildren x uint64)
// Leaf nodes store only keys and leaf links; values live in the underlying KV
// keyed by the user key.
func encodeNode(n *Node) ([]byte, error) {
	if n.Type == NodeInternal && len(n.Children) != len(n.Keys)+1 {
		return nil, fmt.Errorf("internal node %d has inconsistent children", n.ID)
	}

	// Pre-size buffer: header (1 + 2 + 2 + 8 + 8) + keys + children.
	size := 1 + 2 + 2 + 8 + 8
	for _, k := range n.Keys {
		size += 4 + len(k) // uint32 length + bytes
	}
	if n.Type == NodeInternal {
		size += 8 * (len(n.Keys) + 1)
	}

	buf := make([]byte, 0, size)
	buf = append(buf, byte(n.Type))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(n.Keys)))

	childCount := uint16(0)
	if n.Type == NodeInternal {
		childCount = uint16(len(n.Children))
	}
	buf = binary.LittleEndian.AppendUint16(buf, childCount)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(n.NextLeaf))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(n.PrevLeaf))

	for _, k := range n.Keys {
		buf = appendBytes(buf, k)
	}

	switch n.Type {
	case NodeInternal:
		for _, child := range n.Children {
			buf = binary.LittleEndian.AppendUint64(buf, uint64(child))
		}
	case NodeLeaf:
		// No additional payload for leaves; values are stored separately.
	default:
		return nil, fmt.Errorf("unknown node type %d", n.Type)
	}

	return buf, nil
}

func decodeNode(id NodeID, data []byte) (*Node, error) {
	reader := bytes.NewReader(data)
	n := &Node{ID: id}

	t, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read type: %w", err)
	}
	n.Type = NodeType(t)

	var numKeys uint16
	if err := binary.Read(reader, binary.LittleEndian, &numKeys); err != nil {
		return nil, fmt.Errorf("read numKeys: %w", err)
	}

	var numChildren uint16
	if err := binary.Read(reader, binary.LittleEndian, &numChildren); err != nil {
		return nil, fmt.Errorf("read numChildren: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &n.NextLeaf); err != nil {
		return nil, fmt.Errorf("read nextLeaf: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &n.PrevLeaf); err != nil {
		return nil, fmt.Errorf("read prevLeaf: %w", err)
	}

	n.Keys = make([][]byte, numKeys)
	for i := 0; i < int(numKeys); i++ {
		key, err := readBytes(reader)
		if err != nil {
			return nil, fmt.Errorf("read key %d: %w", i, err)
		}
		n.Keys[i] = key
	}

	switch n.Type {
	case NodeInternal:
		expectedChildren := int(numKeys) + 1
		if int(numChildren) != expectedChildren {
			return nil, fmt.Errorf("internal node %d: expected %d children, got %d", id, expectedChildren, numChildren)
		}
		n.Children = make([]NodeID, expectedChildren)
		for i := 0; i < expectedChildren; i++ {
			if err := binary.Read(reader, binary.LittleEndian, &n.Children[i]); err != nil {
				return nil, fmt.Errorf("read child %d: %w", i, err)
			}
		}
	case NodeLeaf:
		// Leaf nodes no longer store values inline; they act as a pure key index.
	default:
		return nil, fmt.Errorf("unknown node type %d", n.Type)
	}

	return n, nil
}

func writeBytes(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// appendBytes is an allocation-free helper to append a length-prefixed byte slice.
func appendBytes(buf []byte, b []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

func readBytes(r io.Reader) ([]byte, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, err
	}
	if l == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
