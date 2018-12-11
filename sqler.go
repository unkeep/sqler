package sqler

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type ValuesMap map[string]interface{}

func (m ValuesMap) fields() []string {
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	return fields
}

func (m ValuesMap) format(pattern string, sep string) string {
	tokens := make([]string, 0, len(m))
	for _, f := range m.fields() {
		tokens = append(tokens, strings.Replace(pattern, "%field", f, -1))
	}
	return strings.Join(tokens, sep)
}

func (m ValuesMap) values() []interface{} {
	values := make([]interface{}, 0, len(m))
	for _, f := range m.fields() {
		values = append(values, m[f])
	}
	return values
}

func dbFieldName(stuctField reflect.StructField) string {
	if tag, ok := stuctField.Tag.Lookup("db"); ok {
		return tag
	} else {
		return strings.ToLower(stuctField.Name)
	}
}

type Mapper struct {
	Ptr interface{}
}

func Map(ptr interface{}) Mapper {
	return Mapper{Ptr: ptr}
}

func (m Mapper) Values(memberPtrs ...interface{}) ValuesMap {
	valuesMap := make(ValuesMap, len(memberPtrs))

	structVal := reflect.ValueOf(m.Ptr).Elem()
	structType := reflect.TypeOf(m.Ptr).Elem()

	if len(memberPtrs) == 0 {
		for i := 0; i < structVal.NumField(); i++ {
			memberPtrVal := structVal.Field(i)
			valuesMap[dbFieldName(structType.Field(i))] = memberPtrVal.Interface()
		}
	} else {
		for _, memberPtr := range memberPtrs {
			memberPtrVal := reflect.ValueOf(memberPtr).Elem()
			for i := 0; i < structVal.NumField(); i++ {
				if structVal.Field(i) == memberPtrVal {
					valuesMap[dbFieldName(structType.Field(i))] = memberPtrVal.Interface()
					break
				}
			}
		}
	}

	return valuesMap
}

func (m Mapper) EqualValues(memberPtrs ...interface{}) *QueryPart {
	return Equal(m.Values(memberPtrs...))
}

func (m Mapper) Fields(memberPtrs ...interface{}) []string {
	fields := make([]string, 0, len(memberPtrs))

	structVal := reflect.ValueOf(m.Ptr).Elem()
	structType := reflect.TypeOf(m.Ptr).Elem()

	if len(memberPtrs) == 0 {
		for i := 0; i < structVal.NumField(); i++ {
			fields = append(fields, dbFieldName(structType.Field(i)))
		}
	} else {
		for _, memberPtr := range memberPtrs {
			memberPtrVal := reflect.ValueOf(memberPtr).Elem()
			for i := 0; i < structVal.NumField(); i++ {
				if structVal.Field(i) == memberPtrVal {
					fields = append(fields, dbFieldName(structType.Field(i)))
					break
				}
			}
		}
	}

	return fields
}

func (m Mapper) SetValues(values ValuesMap) {
	structVal := reflect.ValueOf(m.Ptr).Elem()
	structType := reflect.TypeOf(m.Ptr).Elem()

	for f, v := range values {
		if memberVal, ok := m.findField(f, structType, structVal); ok {
			memberVal.Set(reflect.ValueOf(v))
		}
	}
}

func (m Mapper) findField(name string, structType reflect.Type, structVal reflect.Value) (reflect.Value, bool) {
	for i := 0; i < structVal.NumField(); i++ {
		if dbFieldName(structType.Field(i)) == name {
			return structVal.Field(i), true
		}
	}
	return reflect.Value{}, false
}

func (m Mapper) ScanRow(rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	structVal := reflect.ValueOf(m.Ptr).Elem()
	structType := reflect.TypeOf(m.Ptr).Elem()

	fields := make([]reflect.Value, 0, len(columns))
	ptrs := make([]interface{}, 0, len(columns))
	valsPtrs := make([]reflect.Value, 0, len(columns))

	for _, col := range columns {
		if field, ok := m.findField(col, structType, structVal); ok {
			ptrVal := reflect.New(field.Type())
			valsPtrs = append(valsPtrs, ptrVal)
			ptrs = append(ptrs, ptrVal.Interface())
			fields = append(fields, field)
		}
	}

	if err := rows.Scan(ptrs...); err != nil {
		return err
	}

	for i, field := range fields {
		field.Set(valsPtrs[i].Elem())
	}

	return nil
}

func ScanRow(rows *sql.Rows, ptr interface{}) error {
	mapper := Mapper{Ptr: ptr}
	return mapper.ScanRow(rows)
}

type QueryPart struct {
	Query  string
	Values []interface{}
}

func Part(query string, values ...interface{}) *QueryPart {
	return &QueryPart{Query: query, Values: values}
}

func Equal(values ValuesMap) *QueryPart {
	return &QueryPart{
		Query:  values.format("%field = ?", " AND "),
		Values: values.values(),
	}
}

type Insert struct {
	Table  string
	Values ValuesMap
}

func (ins Insert) Format() (string, []interface{}) {
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)",
		ins.Table,
		ins.Values.format("%field", ", "),
		ins.Values.format("?", ", "))

	return query, ins.Values.values()
}

func (ins Insert) Exec(ex Execer) (sql.Result, error) {
	q, v := ins.Format()
	return ex.Exec(q, v...)
}

type Replace struct {
	Table  string
	Values ValuesMap
}

func (rep Replace) Format() (string, []interface{}) {
	query := fmt.Sprintf("REPLACE INTO %s (%s) VALUES(%s)",
		rep.Table,
		rep.Values.format("%field", ", "),
		rep.Values.format("?", ", "))

	return query, rep.Values.values()
}

func (rep Replace) Exec(ex Execer) (sql.Result, error) {
	q, v := rep.Format()
	return ex.Exec(q, v...)
}

type Update struct {
	Table  string
	Values ValuesMap
	Where  *QueryPart
}

func (upd Update) Format() (string, []interface{}) {
	query := fmt.Sprintf("UPDATE %s SET %s",
		upd.Table,
		upd.Values.format("%field = ?", ", "))

	values := upd.Values.values()
	if upd.Where != nil {
		query += " WHERE " + upd.Where.Query
		values = append(values, upd.Where.Values...)
	}

	return query, values
}

func (upd Update) Exec(ex Execer) (sql.Result, error) {
	q, v := upd.Format()
	return ex.Exec(q, v...)
}

type Select struct {
	Table  string
	Fields []string
	Where  *QueryPart
}

func (sel Select) Format() (string, []interface{}) {
	fields := "*"
	if len(sel.Fields) != 0 {
		fields = strings.Join(sel.Fields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", fields, sel.Table)
	var values []interface{}
	if sel.Where != nil {
		query += " WHERE " + sel.Where.Query
		values = append(values, sel.Where.Values...)
	}

	return query, values
}

func (sel Select) Query(querier Querier) (*sql.Rows, error) {
	query, values := sel.Format()
	return querier.Query(query, values...)
}

func (sel Select) QueryRow(querier Querier) *sql.Row {
	query, values := sel.Format()
	return querier.QueryRow(query, values...)
}
