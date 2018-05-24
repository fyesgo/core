package config

import (
	"bytes"
	"fmt"
	"reflect"
	"unicode"
)

type SnakeCaseTagger string

func (m SnakeCaseTagger) MakeTag(t reflect.Type, fieldIndex int) reflect.StructTag {
	key := string(m)
	field := t.Field(fieldIndex)
	value := field.Tag.Get(key)
	if value == "" {
		value = camelCaseToSnakeCase(field.Name)
	}
	tag := fmt.Sprintf(`%s:"%s"`, key, value)
	return reflect.StructTag(tag)
}

func camelCaseToSnakeCase(src string) string {
	var b bytes.Buffer
	for i, r := range src {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}
