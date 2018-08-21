package common

import (
	"io"
	"encoding/json"
)

type DataPoint struct {
	Metrics		string				`json:"metrics"`
	Field 		string				`json:"field"`
	Tags 		map[string]string	`json:"tags"`
	Step 		int					`json:"step"`
	Type 		string				`json:"type"`
	Value 		interface{} 		`json:"value"`
	Timestamp   int64				`json:"timestamp"`
	DStore		uint8				`json:"dStore"`
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

	for i, fieldKey := range p.FieldKeys {
		dp := &DataPoint{
			Metrics: string(p.MeasurementName),
			Field: string(fieldKey),
			Tags: tags,
			Timestamp: p.Timestamp.Unix(),
			Value: p.FieldValues[i],
		}

		encoder := json.NewEncoder(w)
		err = encoder.Encode(dp)
		if err != nil {
			return
		}
	}

	return err
}

func (s *serializerNervDB) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}

