package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

// Strict decoding is shared by the process/result boundary and note guards.
// encoding/json alone accepts duplicate object keys, including nested ones.
func reviewStrictJSON(data []byte, target interface{}) error {
	if !utf8.Valid(data) { return fmt.Errorf("review JSON is not UTF-8") }
	dec := json.NewDecoder(bytes.NewReader(data))
	var consume func() error
	consume = func() error {
		token, err := dec.Token()
		if err != nil { return err }
		delim, composite := token.(json.Delim)
		if !composite { return nil }
		switch delim {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				key, err := dec.Token()
				if err != nil { return err }
				name, ok := key.(string)
				if !ok || seen[name] { return fmt.Errorf("duplicate or invalid review JSON key %q", key) }
				seen[name] = true
				if err := consume(); err != nil { return err }
			}
			end, err := dec.Token()
			if err != nil || end != json.Delim('}') { return fmt.Errorf("unclosed review object") }
		case '[':
			for dec.More() { if err := consume(); err != nil { return err } }
			end, err := dec.Token()
			if err != nil || end != json.Delim(']') { return fmt.Errorf("unclosed review array") }
		default: return fmt.Errorf("unexpected review delimiter")
		}
		return nil
	}
	if err := consume(); err != nil { return err }
	if _, err := dec.Token(); err != io.EOF { return fmt.Errorf("review JSON must contain exactly one value") }
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil { return err }
	var raw interface{}
	if err := json.Unmarshal(data,&raw); err != nil { return err }
	return reviewExactStructKeys(raw,reflect.TypeOf(target))
}

// Go's decoder matches struct fields case-insensitively. Wire contracts do
// not: a second differently cased name must not shadow the actual field.
func reviewExactStructKeys(value interface{}, target reflect.Type) error {
	if target == nil { return fmt.Errorf("missing review decode target") }
	for target.Kind() == reflect.Ptr { target = target.Elem() }
	if value == nil { return nil }
	switch target.Kind() {
	case reflect.Struct:
		object,ok := value.(map[string]interface{})
		if !ok { return fmt.Errorf("review object required") }
		fields := map[string]reflect.Type{}
		for i:=0;i<target.NumField();i++ {
			field := target.Field(i)
			name := strings.Split(field.Tag.Get("json"),",")[0]
			if name != "" && name != "-" { fields[name] = field.Type }
		}
		for name,child := range object {
			field,exists := fields[name]
			if !exists { return fmt.Errorf("unknown exact review field %q",name) }
			if err := reviewExactStructKeys(child,field); err != nil { return err }
		}
	case reflect.Slice,reflect.Array:
		if array,ok := value.([]interface{}); ok { for _,child := range array { if err := reviewExactStructKeys(child,target.Elem()); err != nil { return err } } }
	}
	return nil
}

func reviewNoteObject(value interface{}) (map[string]interface{}, error) {
	if value == nil { return map[string]interface{}{}, nil }
	if object, ok := value.(map[string]interface{}); ok { return object, nil }
	text, ok := value.(string)
	if !ok { return nil, fmt.Errorf("note must be text or an object") }
	if !strings.HasPrefix(strings.TrimSpace(text), "{") { return map[string]interface{}{}, nil }
	var object map[string]interface{}
	// Notes remain untrusted ordinary metadata. Match the actual JSON.parse
	// consumer projection (last duplicate key wins); malformed prose has no
	// acceptance fields. Replacing an existing protected object with such
	// prose still fails the before/after comparison below. Strict review wire
	// and process output use reviewStrictJSON instead.
	if err := json.Unmarshal([]byte(text), &object); err != nil { return map[string]interface{}{}, nil }
	return object, nil
}

func reviewClosedFields(data []byte, allowed map[string]bool) error {
	var object map[string]interface{}
	if err := reviewStrictJSON(data,&object); err != nil { return err }
	if object == nil { return fmt.Errorf("review wire value must be an object") }
	for key := range object { if !allowed[key] { return fmt.Errorf("unknown review field %q",key) } }
	return nil
}

// This is a structural fence, not a recursive prose/keyword filter. The
// projection is keyed by existing slot coordinates, so reordering an array or
// echoing an unchanged protected object does not manufacture authority.
func reviewProtectedProjection(value interface{}) (map[string]interface{}, error) {
	note, err := reviewNoteObject(value)
	if err != nil { return nil, err }
	out := map[string]interface{}{}
	if v, exists := note["validation_acceptance"]; exists { out["validation_acceptance"] = v }
	slots := []interface{}{}
	if v, exists := note["queue_assignments"]; exists {
		array, ok := v.([]interface{})
		if !ok { return nil, fmt.Errorf("queue_assignments must be an array") }
		slots = append(slots, array...)
	}
	if v, exists := note["queue_assignment"]; exists { slots = append(slots, v) }
	for _, raw := range slots {
		slot, ok := raw.(map[string]interface{})
		if !ok { return nil, fmt.Errorf("execution slot must be an object") }
		protected := map[string]interface{}{}
		for _, key := range []string{"acceptance", "tdd_red_receipt", "tdd_green_receipt", "reopen_history"} {
			if v, exists := slot[key]; exists { protected[key] = v }
		}
		if tests, ok := slot["tests"].(map[string]interface{}); ok {
			for _, key := range []string{"acceptance", "tdd_audit_receipt"} { if v, exists := tests[key]; exists { protected["tests."+key] = v } }
		}
		if len(protected) == 0 { continue }
		key := reviewProtectedSlotKey(slot)
		if _, exists := out[key]; exists { return nil, fmt.Errorf("duplicate protected execution slot") }
		out[key] = protected
	}
	return out, nil
}

func reviewProtectedSlotKey(slot map[string]interface{}) string {
	identity := []interface{}{slot["project_id"], slot["task_id"], slot["repo"], slot["worktree"], slot["slot_id"]}
	key, _ := json.Marshal(identity)
	return "slot:"+string(key)
}

func guardReviewNoteWrite(before, after interface{}) error {
	old, oldErr := reviewProtectedProjection(before)
	next, nextErr := reviewProtectedProjection(after)
	if oldErr != nil || nextErr != nil || !reflect.DeepEqual(old, next) {
		return fmt.Errorf("REVIEW_ORIGIN_REQUIRED: protected acceptance/lineage may only be published by its typed server producer (unchanged echo and ordinary metadata remain allowed)")
	}
	return nil
}
