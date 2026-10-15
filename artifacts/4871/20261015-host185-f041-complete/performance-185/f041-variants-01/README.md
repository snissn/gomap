# f041 read-cost attribution and correctness

The hosted f041 gate rejected GetVersioned (+13.09%) and snapshot_seek (+6.54%). The frozen baseline is 5bca87ffe6e1bfba2d693264d2dc82936adc234f; the frozen control is f0415773f13a522dea5c1debdaecba09d15585dd. Local builds use pinned Go 1.26.3, while original hosted binaries used Go 1.26.9.

Two causal variants remove repeated nil-reader interface probes and route ordinary local DATA reads directly to the same mapping kernel, preserving finite-work/PRIMARY/overlay/metadata-guard routes. The combined candidate also clears active root-tree cache entries once and only scrubs the overflowed inline entry if populated. Mandatory main-tree, reader and owner alias withdrawal remains before physical cleanup completion.

Full pager and tree packages passed under their respective overlays. Combined snapshot/one-shot/native snapshot/PRIMARY snapshot controls passed. Profiles are busy-host CPU attribution, never latency acceptance. The 5-second uninstrumented preflight rejected pulseaudio, gvfs-udisks2 and htop above the original 2% threshold even with both workers runtime-idle. No uninstrumented sample started and no retry or service mutation occurred.

The proposed source changes require frozen successor correctness, independent review and the unchanged hosted raw-path gate. All exact overlay/build inputs and binary digests are retained in manifest-final.json.
