package common

import (
	"io"
	"encoding/json"
)

type DataPoint struct {
	Metrics		string					`json:"metrics"`
	Fields 		map[string]interface{}	`json:"fields"`
	Tags 		map[string]string		`json:"tags"`
	Timestamp   int64					`json:"timestamp"`
}

type serializerNervDB struct {
}

func NewSerializerNervDB() *serializerNervDB {
	return &serializerNervDB{}
}

// SerializeInfluxNervDB writes Point data to the given writer, conforming to the
// NervDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (s *serializerNervDB) SerializePoint(w io.Writer, p *Point) (err error) {

	tags := make(map[string]string, len(p.TagKeys))
	for i, key := range p.TagKeys {
		tags[string(key)] = string(p.TagValues[i])
	}

	dp := &DataPoint{
		Metrics: string(p.MeasurementName),
		Fields: make(map[string]interface{}, len(p.FieldKeys)),
		Tags: tags,
		Timestamp: p.Timestamp.Local().UnixNano() / 1e6,
	}


	for i, fieldKey := range p.FieldKeys {
		dp.Fields[string(fieldKey)] = p.FieldValues[i]
	}

	encoder := json.NewEncoder(w)
	err = encoder.Encode(dp)
	if err != nil {
		return
	}

	return err
}

func (s *serializerNervDB) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}

