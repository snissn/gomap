package colgranule

import (
	"fmt"
	"sort"
	"time"
)

type JSONBenchQueryTiming struct {
	Query        string          `json:"query"`
	Description  string          `json:"description"`
	Attempts     []time.Duration `json:"attempts"`
	Best         time.Duration   `json:"best"`
	ResultRows   int             `json:"result_rows"`
	ResultDigest uint64          `json:"result_digest"`
}

func RunJSONBenchQueries(ds JSONBenchDataset, attempts int) ([]JSONBenchQueryTiming, error) {
	if attempts <= 0 {
		attempts = 3
	}
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		return nil, err
	}
	queries := []struct {
		name        string
		description string
		run         func(JSONBenchDataset, queryCodeSet) (int, uint64)
	}{
		{"Q1", "Top event types", runJSONBenchQ1},
		{"Q2", "Top event types with unique users", runJSONBenchQ2},
		{"Q3", "Event counts by hour", runJSONBenchQ3},
		{"Q4", "Top 3 post veterans", runJSONBenchQ4},
		{"Q5", "Top 3 users with longest activity", runJSONBenchQ5},
	}
	out := make([]JSONBenchQueryTiming, 0, len(queries))
	for _, q := range queries {
		timing := JSONBenchQueryTiming{
			Query:       q.name,
			Description: q.description,
			Attempts:    make([]time.Duration, 0, attempts),
		}
		for i := 0; i < attempts; i++ {
			start := time.Now()
			rows, digest := q.run(ds, codes)
			elapsed := time.Since(start)
			timing.Attempts = append(timing.Attempts, elapsed)
			timing.ResultRows = rows
			timing.ResultDigest = digest
			if timing.Best == 0 || elapsed < timing.Best {
				timing.Best = elapsed
			}
		}
		out = append(out, timing)
	}
	return out, nil
}

type queryCodeSet struct {
	kindCommit       int64
	operationCreate  int64
	collectionPost   int64
	collectionRepost int64
	collectionLike   int64
}

func jsonBenchQueryCodes(ds JSONBenchDataset) (queryCodeSet, error) {
	lookup := func(dictName, value string) (int64, error) {
		dict := ds.Dictionaries[dictName]
		if dict == nil {
			return 0, fmt.Errorf("missing dictionary %s", dictName)
		}
		code, ok := dict[value]
		if !ok {
			return -1, nil
		}
		return code, nil
	}
	var out queryCodeSet
	var err error
	if out.kindCommit, err = lookup("kind_code", "commit"); err != nil {
		return queryCodeSet{}, err
	}
	if out.operationCreate, err = lookup("commit_operation_code", "create"); err != nil {
		return queryCodeSet{}, err
	}
	if out.collectionPost, err = lookup("commit_collection_code", "app.bsky.feed.post"); err != nil {
		return queryCodeSet{}, err
	}
	if out.collectionRepost, err = lookup("commit_collection_code", "app.bsky.feed.repost"); err != nil {
		return queryCodeSet{}, err
	}
	if out.collectionLike, err = lookup("commit_collection_code", "app.bsky.feed.like"); err != nil {
		return queryCodeSet{}, err
	}
	return out, nil
}

func runJSONBenchQ1(ds JSONBenchDataset, _ queryCodeSet) (int, uint64) {
	collection := ds.Columns["commit_collection_code"]
	counts := make(map[int64]int64, 16)
	for _, code := range collection {
		counts[code]++
	}
	return len(counts), digestCounts(counts)
}

func runJSONBenchQ2(ds JSONBenchDataset, codes queryCodeSet) (int, uint64) {
	kind := ds.Columns["kind_code"]
	operation := ds.Columns["commit_operation_code"]
	collection := ds.Columns["commit_collection_code"]
	did := ds.Columns["did_code"]
	counts := make(map[int64]int64, 16)
	unique := make(map[int64]map[int64]struct{}, 16)
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate {
			continue
		}
		event := collection[i]
		counts[event]++
		if unique[event] == nil {
			unique[event] = make(map[int64]struct{})
		}
		unique[event][did[i]] = struct{}{}
	}
	digest := digestCounts(counts)
	events := make([]int64, 0, len(unique))
	for event := range unique {
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool { return events[i] < events[j] })
	for _, event := range events {
		users := unique[event]
		digest = digestMix(digest, uint64(event), uint64(len(users)))
	}
	return len(counts), digest
}

func runJSONBenchQ3(ds JSONBenchDataset, codes queryCodeSet) (int, uint64) {
	kind := ds.Columns["kind_code"]
	operation := ds.Columns["commit_operation_code"]
	collection := ds.Columns["commit_collection_code"]
	timeUS := ds.Columns["time_us"]
	counts := make(map[int64]int64, 128)
	for i := range collection {
		event := collection[i]
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate {
			continue
		}
		if event != codes.collectionPost && event != codes.collectionRepost && event != codes.collectionLike {
			continue
		}
		hour := unixMicroHour(timeUS[i])
		counts[event*100+hour]++
	}
	return len(counts), digestCounts(counts)
}

func runJSONBenchQ4(ds JSONBenchDataset, codes queryCodeSet) (int, uint64) {
	kind := ds.Columns["kind_code"]
	operation := ds.Columns["commit_operation_code"]
	collection := ds.Columns["commit_collection_code"]
	did := ds.Columns["did_code"]
	timeUS := ds.Columns["time_us"]
	first := make(map[int64]int64, 128*1024)
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate || collection[i] != codes.collectionPost {
			continue
		}
		user := did[i]
		if prev, ok := first[user]; !ok || timeUS[i] < prev {
			first[user] = timeUS[i]
		}
	}
	type pair struct {
		user int64
		t    int64
	}
	top := make([]pair, 0, len(first))
	for user, t := range first {
		top = append(top, pair{user: user, t: t})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].t == top[j].t {
			return top[i].user < top[j].user
		}
		return top[i].t < top[j].t
	})
	if len(top) > 3 {
		top = top[:3]
	}
	var digest uint64
	for _, p := range top {
		digest = digestMix(digest, uint64(p.user), uint64(p.t))
	}
	return len(top), digest
}

func runJSONBenchQ5(ds JSONBenchDataset, codes queryCodeSet) (int, uint64) {
	kind := ds.Columns["kind_code"]
	operation := ds.Columns["commit_operation_code"]
	collection := ds.Columns["commit_collection_code"]
	did := ds.Columns["did_code"]
	timeUS := ds.Columns["time_us"]
	type span struct {
		min int64
		max int64
	}
	spans := make(map[int64]span, 128*1024)
	for i := range collection {
		if kind[i] != codes.kindCommit || operation[i] != codes.operationCreate || collection[i] != codes.collectionPost {
			continue
		}
		user := did[i]
		s, ok := spans[user]
		if !ok {
			spans[user] = span{min: timeUS[i], max: timeUS[i]}
			continue
		}
		if timeUS[i] < s.min {
			s.min = timeUS[i]
		}
		if timeUS[i] > s.max {
			s.max = timeUS[i]
		}
		spans[user] = s
	}
	type pair struct {
		user int64
		span int64
	}
	top := make([]pair, 0, len(spans))
	for user, s := range spans {
		top = append(top, pair{user: user, span: s.max - s.min})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].span == top[j].span {
			return top[i].user < top[j].user
		}
		return top[i].span > top[j].span
	})
	if len(top) > 3 {
		top = top[:3]
	}
	var digest uint64
	for _, p := range top {
		digest = digestMix(digest, uint64(p.user), uint64(p.span))
	}
	return len(top), digest
}

func unixMicroHour(us int64) int64 {
	return (us / 1_000_000 / 3600) % jsonBenchHoursPerDay
}

func digestCounts(counts map[int64]int64) uint64 {
	keys := make([]int64, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var digest uint64
	for _, k := range keys {
		digest = digestMix(digest, uint64(k), uint64(counts[k]))
	}
	return digest
}

func digestMix(seed, a, b uint64) uint64 {
	const prime uint64 = 1099511628211
	h := seed
	if h == 0 {
		h = 1469598103934665603
	}
	h ^= a
	h *= prime
	h ^= b
	h *= prime
	return h
}
