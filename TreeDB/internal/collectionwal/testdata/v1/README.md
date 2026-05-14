These fixtures pin the local collection WAL v1 byte format described in
`TreeDB/docs/spec/storage-format.md`.

- `transaction_frame.hex` is one committed v1 transaction frame.
- `segment_one_txn.hex` is a v1 segment header followed by that frame.

The bytes are local physical storage fixtures, not cross-replica or native-wire
fixtures.
