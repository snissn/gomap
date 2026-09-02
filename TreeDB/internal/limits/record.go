package limits

// MaxRecordSize bounds a single record (header + key + value).
// Set <= 0 to disable the cap.
var MaxRecordSize int64 = 64 * 1024 * 1024
