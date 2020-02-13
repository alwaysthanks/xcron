package json

import (
	"github.com/json-iterator/go"
	"io"
)

var jsonParser = jsoniter.ConfigCompatibleWithStandardLibrary

func Unmarshal(data []byte, v interface{}) error {
	return jsonParser.Unmarshal(data, v)
}

func Marshal(v interface{}) ([]byte, error) {
	return jsonParser.Marshal(v)
}

func NewDecoder(reader io.Reader) *jsoniter.Decoder {
	return jsonParser.NewDecoder(reader)
}

func NewEncoder(writer io.Writer) *jsoniter.Encoder {
	return jsonParser.NewEncoder(writer)
}
