# Frozen Q32 map-admission component repair

This non-PR WIP checkpoint follows 514dab33ef07ab09cacb89efd862043a59f15318. Exactly two source paths change: generation_v1_begin_work.go and generation_v1_tree_work_test.go. The full GREEN compiler census has 4424 entries; the other 4422 match the original frozen80 census. RED and GREEN use the same new regression; RED changes only the begin source to its rejected preimage. All three RED cases fail, while GREEN completes the two fitting boundaries and explicitly refuses oversized growth. Independent bounded review ACCEPT closes frozen80 F1 only. The original REJECT review remains unchanged.

Native ACK=0. This is not general Q32, public ACK, native performance or final qualification. Later mutable owner work is excluded. No Go runs, PR, merge or issue closure occurred during publication. Foreign2 source files and protected fixture are not staged. [skip ci] is explicitly authorized for this non-PR WIP checkpoint only.
