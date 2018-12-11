package sqler

import (
	"reflect"
	"testing"
)

func TestInsert_Format(t *testing.T) {
	tests := []struct {
		name  string
		ins   Insert
		want  string
		want1 []interface{}
	}{
		{
			name:  "single value",
			ins:   Insert{Table: "table", Values: ValuesMap{"f1": 1}},
			want:  "INSERT INTO table (f1) VALUES(?)",
			want1: []interface{}{1},
		},
		{
			name:  "multi values",
			ins:   Insert{Table: "table", Values: ValuesMap{"f1": 1, "f2": nil}},
			want:  "INSERT INTO table (f1, f2) VALUES(?, ?)",
			want1: []interface{}{1, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.ins.Format()
			if got != tt.want {
				t.Errorf("Insert.Format() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Insert.Format() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestReplace_Format(t *testing.T) {
	tests := []struct {
		name  string
		rep   Replace
		want  string
		want1 []interface{}
	}{
		{
			name:  "single value",
			rep:   Replace{Table: "table", Values: ValuesMap{"f1": 1}},
			want:  "REPLACE INTO table (f1) VALUES(?)",
			want1: []interface{}{1},
		},
		{
			name:  "multi values",
			rep:   Replace{Table: "table", Values: ValuesMap{"f1": 1, "f2": nil}},
			want:  "REPLACE INTO table (f1, f2) VALUES(?, ?)",
			want1: []interface{}{1, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.rep.Format()
			if got != tt.want {
				t.Errorf("Replace.Format() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Replace.Format() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestUpdate_Format(t *testing.T) {

	testStruct := &TestStruct{F1: 1, F2: "qwe"}
	mapper := Mapper{testStruct}

	tests := []struct {
		name  string
		upd   Update
		want  string
		want1 []interface{}
	}{
		{
			name:  "single field",
			upd:   Update{Table: "t1", Values: ValuesMap{"f1": 1}},
			want:  "UPDATE t1 SET f1 = ?",
			want1: []interface{}{1},
		},
		{
			name: "multi field with Where",
			upd: Update{
				Table:  "t1",
				Values: ValuesMap{"f1": 1, "f2": nil},
				Where:  Part("f2 > ?", 10),
			},
			want:  "UPDATE t1 SET f1 = ?, f2 = ? WHERE f2 > ?",
			want1: []interface{}{1, nil, 10},
		},
		{
			name: "with mapper",
			upd: Update{
				Table:  "t1",
				Values: mapper.Values(&testStruct.F1),
				Where:  Equal(mapper.Values(&testStruct.F2)),
			},
			want:  "UPDATE t1 SET f1 = ? WHERE f_2 = ?",
			want1: []interface{}{testStruct.F1, testStruct.F2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.upd.Format()
			if got != tt.want {
				t.Errorf("Update.Format() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Update.Format() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSelect_Format(t *testing.T) {
	tests := []struct {
		name  string
		sel   Select
		want  string
		want1 []interface{}
	}{
		{
			name: "all",
			sel:  Select{Table: "t1"},
			want: "SELECT * FROM t1",
		},
		{
			name: "single field",
			sel:  Select{Table: "t1", Fields: []string{"f1"}},
			want: "SELECT f1 FROM t1",
		},
		{
			name:  "multi fields with Where",
			sel:   Select{Table: "t1", Fields: []string{"f1", "f2"}, Where: Part("f3 = ?", "qwe")},
			want:  "SELECT f1, f2 FROM t1 WHERE f3 = ?",
			want1: []interface{}{"qwe"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.sel.Format()
			if got != tt.want {
				t.Errorf("Select.Format() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Select.Format() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

type TestStruct struct {
	F1 int
	F2 string `db:"f_2"`
}

func TestStructValues(t *testing.T) {

	ptr := &TestStruct{F1: 1, F2: "qwe"}
	mapper := Mapper{ptr}

	tests := []struct {
		name       string
		memberPtrs []interface{}
		want       ValuesMap
	}{
		{
			name: "all",
			want: ValuesMap{"f1": 1, "f_2": "qwe"},
		},
		{
			name:       "single specified",
			memberPtrs: []interface{}{&ptr.F2},
			want:       ValuesMap{"f_2": "qwe"},
		},
		{
			name:       "both specified",
			memberPtrs: []interface{}{&ptr.F1, &ptr.F2},
			want:       ValuesMap{"f1": 1, "f_2": "qwe"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapper.Values(tt.memberPtrs...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StructValuesMap{) = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		name   string
		values ValuesMap
		want   *QueryPart
	}{
		{
			name:   "single value",
			values: ValuesMap{"f1": 1},
			want:   &QueryPart{Query: "f1 = ?", Values: []interface{}{1}},
		},
		{
			name:   "multi values",
			values: ValuesMap{"f1": 1, "f2": "qwe"},
			want:   &QueryPart{Query: "f1 = ? AND f2 = ?", Values: []interface{}{1, "qwe"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Equal(tt.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestMapper_Fields(t *testing.T) {
	ptr := &TestStruct{}
	m := Mapper{ptr}
	want := []string{"f1", "f_2"}
	if got := m.Fields(&ptr.F1, &ptr.F2); !reflect.DeepEqual(got, want) {
		t.Errorf("Mapper.Fields() = %v, want %v", got, want)
	}
}

func TestMapper_SetValues(t *testing.T) {
	tests := []struct {
		name   string
		values ValuesMap
		want   TestStruct
	}{
		{
			name:   "f1",
			values: ValuesMap{"f1": 1},
			want:   TestStruct{F1: 1},
		},
		{
			name:   "f_2",
			values: ValuesMap{"f_2": "qwe"},
			want:   TestStruct{F2: "qwe"},
		},
		{
			name:   "f1 & f_2",
			values: ValuesMap{"f1": 1, "f_2": "qwe"},
			want:   TestStruct{F1: 1, F2: "qwe"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ptr := &TestStruct{}
			mapper := Mapper{ptr}
			mapper.SetValues(tt.values)
			if !reflect.DeepEqual(*ptr, tt.want) {
				t.Errorf("Mapper.Fields() = %v, want %v", *ptr, tt.want)
			}
		})
	}
}
