package pebblecompat

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/pebble"
)

type decodedCheckpointOptions struct {
	flushWAL       bool
	restrictToSpan bool
}

func callCheckpointOption(opt reflect.Value, arg reflect.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: checkpoint option panicked: %v", ErrCheckpointOptionUnsupported, r)
		}
	}()
	opt.Call([]reflect.Value{arg})
	return nil
}

func decodeCheckpointOptions(opts []pebble.CheckpointOption) (decodedCheckpointOptions, error) {
	var decoded decodedCheckpointOptions
	if len(opts) == 0 {
		return decoded, nil
	}

	var cfgPtr reflect.Value
	var argType reflect.Type
	for i := range opts {
		if opts[i] == nil {
			return decoded, fmt.Errorf("%w: nil checkpoint option at index %d", ErrCheckpointOptionUnsupported, i)
		}
		optVal := reflect.ValueOf(opts[i])
		optType := optVal.Type()
		if optType.Kind() != reflect.Func || optType.NumIn() != 1 || optType.NumOut() != 0 {
			return decoded, fmt.Errorf("%w: invalid checkpoint option signature", ErrCheckpointOptionUnsupported)
		}
		curArgType := optType.In(0)
		if curArgType.Kind() != reflect.Ptr || curArgType.Elem().Kind() != reflect.Struct {
			return decoded, fmt.Errorf("%w: invalid checkpoint option target", ErrCheckpointOptionUnsupported)
		}
		if i == 0 {
			argType = curArgType
			cfgPtr = reflect.New(argType.Elem())
		} else if curArgType != argType {
			return decoded, fmt.Errorf("%w: mixed checkpoint option target types", ErrCheckpointOptionUnsupported)
		}
		if err := callCheckpointOption(optVal, cfgPtr); err != nil {
			return decoded, err
		}
	}

	cfg := cfgPtr.Elem()
	flushWALField := cfg.FieldByName("flushWAL")
	if !flushWALField.IsValid() || flushWALField.Kind() != reflect.Bool {
		return decoded, fmt.Errorf("%w: unknown checkpoint options layout (flushWAL)", ErrCheckpointOptionUnsupported)
	}
	restrictToSpansField := cfg.FieldByName("restrictToSpans")
	if !restrictToSpansField.IsValid() || restrictToSpansField.Kind() != reflect.Slice {
		return decoded, fmt.Errorf("%w: unknown checkpoint options layout (restrictToSpans)", ErrCheckpointOptionUnsupported)
	}

	decoded.flushWAL = flushWALField.Bool()
	decoded.restrictToSpan = restrictToSpansField.Len() > 0
	return decoded, nil
}
