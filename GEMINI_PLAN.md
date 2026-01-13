# Gemini Plan: Implement Slab V2 (Zonal Dictionary Compression)

The goal is to restore and correctly implement the Slab V2 format to enable dictionary-based compression. This will address the current issue where trained dictionaries are not applied because the V2 format (needed to store them) was reverted.

## Directives (from AGENTS_SLAB_OPTIMIZATIONS.md & local_dictionary_compression.md)
*   **V2 Layout:** 32KB File Header -> 32KB Global Dict -> Zone 0 Data ... 64B Zone 1 Header -> Zone 1 Data.
*   **Dictionary Logic:** Global Dictionary is standard. Local dictionaries are optional overrides triggered by entropy drift.
*   **Read Path:** O(1) selection via `zoneID = offset / 2MB`.
*   **Write Path:** Serialize dictionary choices.
*   **Safety:** Checksums for dicts and headers.

## Phase 1: V2 Format & Reader Infrastructure (Slab Skeleton)

**Focus:** Define the V2 constants and implement the read-side logic to detect and parse V2 slabs, ensuring backward compatibility with V1.

1.  **Define Constants & Structures (in `slab/slab_v2.go`):**
    *   `Version2`: 0x02
    *   `ZoneSize`: 2MB (2 * 1024 * 1024)
    *   `FileHeaderSize`: 32KB (includes Magic + Version + Metadata space)
    *   `GlobalDictSize`: 32KB
    *   `SlabV2DataStart`: 64KB (File Header + Global Dict)
    *   `ZoneHeaderSize`: 64B
    *   `ZoneHeader` struct: Magic, DictType (Global/Local/Ref), DictCRC, DictLen, Padding.
2.  **Update `SlabFile` (in `slab/slab.go`):**
    *   Add `version` field (detected on Open).
    *   Update `Read` / `readViaMmap` / `ReadUnsafe` to handle V2 address translation:
        *   If V2:
            *   Calculate `zoneID = offset / ZoneSize`.
            *   Identify Zone Header location.
            *   **Crucial:** Check if `offset` falls *inside* a Zone Header (invalid read) or crosses a Zone Boundary (handled by logic).
            *   **Address Translation?** No, the spec implies physical layout. The "offset" passed to Read is the physical file offset. The reader simply needs to know that at specific offsets (multiples of 2MB), there are headers, not data.
3.  **Implement Dictionary Loading (Read-Side):**
    *   On `OpenSlab` (V2), map/load the Global Dictionary (bytes [32KB..64KB]).
    *   Initialize `GlobalDecoderPool`.
4.  **Tests:**
    *   Unit tests for `zoneID` calculation.
    *   Mock V2 file creation (manually writing headers) and verifying `OpenSlab` detects it.

## Phase 2: Write Path (Global Dictionary MVP)

**Focus:** Enable the primary benefit (dictionary compression) using a single Global Dictionary per slab.

1.  **Update `SlabManager.Rotate` (in `slab/manager.go`):**
    *   Fetch `ActiveProfile` from `compressionTrainer`.
    *   If valid profile exists:
        *   Write V2 File Header (Magic + Version=2).
        *   Write Global Dictionary (32KB, padded).
        *   Initialize `SlabFile` as V2.
        *   Set `activeSlab.compressionConfig` to use this dictionary.
    *   If no profile: Fallback to V1 (or write V2 with empty dict? Prefer V1 for compatibility/simplicity if no dict is ready).
2.  **Update `SlabManager.Append` (in `slab/manager.go` & `slab/slab.go`):**
    *   **Zone Tracking:** In `Write` loop, check if `writeOffset + recordLen` crosses a 2MB boundary.
    *   **Zone Header Insertion:** If crossing boundary:
        *   Pad current Zone to 2MB (if needed).
        *   Write `ZoneHeader` (Flags: USE_GLOBAL).
        *   Reset Zone-local counters if any.
    *   **Compression:** Use the Global Dictionary encoder for records.
3.  **Tests:**
    *   E2E test: Train a dict -> Rotate -> Write Data -> Read Data. Verify `IsCompressed` and `IsFullCompressed` are effective.
    *   Verify file structure (headers at correct offsets).

## Phase 3: Adaptive/Local Dictionaries (Full Spec)

**Focus:** Implement the "Zonal" part—switching dictionaries mid-file.

1.  **Writer Integration:**
    *   On Zone Boundary, query `compressionTrainer` for a "New Local Dict" (based on recent entropy/samples).
    *   If available:
        *   Write `ZoneHeader` with `USE_LOCAL` flag.
        *   Write the new 32KB Local Dict immediately after header.
        *   Update `activeSlab` encoder to use new dict.
    *   If not: Write `USE_GLOBAL` (or `USE_REF`).
2.  **Reader Integration:**
    *   Update `Read` to check Zone Header flags.
    *   If `USE_LOCAL`: Read dict from file (after header).
    *   Manage `LocalDecoderCache` (LRU) to avoid OOM with many local dicts.
3.  **Tests:**
    *   Simulate entropy drift (force trainer to produce new dicts).
    *   Verify mixed-mode slab reading.

## Execution Order
I will start with **Phase 1 & 2 combined (MVP)** because V2 format without a Global Dictionary is useless. I will aim for a "Global-Only V2" first, then add Local support.