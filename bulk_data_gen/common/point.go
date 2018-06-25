package common

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
	"github.com/influxdata/influxdb-comparisons/timescale_serializaition"
	"log"
)

// Point wraps a single data point. It stores database-agnostic data
// representing one point in time of one measurement.
//
// Internally, Point uses byte slices instead of strings to try to minimize
// overhead.
type Point struct {
	MeasurementName []byte
	TagKeys         [][]byte
	TagValues       [][]byte
	FieldKeys       [][]byte
	FieldValues     []interface{}
	Timestamp       *time.Time

	encoder *gob.Encoder
}

// Using these literals prevents the slices from escaping to the heap, saving
// a few micros per call:
var ()

// scratchBufPool helps reuse serialization scratch buffers.
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

func (p *Point) Reset() {
	p.MeasurementName = nil
	p.TagKeys = p.TagKeys[:0]
	p.TagValues = p.TagValues[:0]
	p.FieldKeys = p.FieldKeys[:0]
	p.FieldValues = p.FieldValues[:0]
	p.Timestamp = nil
}

func (p *Point) SetTimestamp(t *time.Time) {
	p.Timestamp = t
}

func (p *Point) SetMeasurementName(s []byte) {
	p.MeasurementName = s
}

func (p *Point) AppendTag(key, value []byte) {
	p.TagKeys = append(p.TagKeys, key)
	p.TagValues = append(p.TagValues, value)
}

func (p *Point) AppendField(key []byte, value interface{}) {
	p.FieldKeys = append(p.FieldKeys, key)
	p.FieldValues = append(p.FieldValues, value)
}

// SerializeInfluxBulk writes Point data to the given writer, conforming to the
// InfluxDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (p *Point) SerializeInfluxBulk(w io.Writer) (err error) {
	buf := scratchBufPool.Get().([]byte)
	buf = append(buf, p.MeasurementName...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, p.TagValues[i]...)
	}

	if len(p.FieldKeys) > 0 {
		buf = append(buf, ' ')
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, '=')

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf)

		// Influx uses 'i' to indicate integers:
		switch v.(type) {
		case int, int64:
			buf = append(buf, 'i')
		}

		if i+1 < len(p.FieldKeys) {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, ' ')
	buf = fastFormatAppend(p.Timestamp.UTC().UnixNano(), buf)
	buf = append(buf, '\n')
	_, err = w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)

	return err
}

// SerializeESBulk writes Point data to the given writer, conforming to the
// ElasticSearch bulk load protocol.
//
// This function writes output that looks like:
// <action line>
// <tags, fields, and timestamp>
//
// For example:
// { "index" : { "_index" : "measurement_otqio", "_type" : "point" } }\n
// { "tag_launx": "btkuw", "tag_gaijk": "jiypr", "field_wokxf": 0.08463898963964356, "field_zqstf": -0.043641533500086316, "timestamp": 171300 }\n
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (p *Point) SerializeESBulk(w io.Writer) error {
	buf := scratchBufPool.Get().([]byte)

	buf = append(buf, "{ \"index\" : { \"_index\" : \""...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, "\", \"_type\" : \"point\" } }\n"...)

	buf = append(buf, '{')

	for i := 0; i < len(p.TagKeys); i++ {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, "\""...)
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, "\": \""...)
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, "\""...)
	}

	if len(p.TagKeys) > 0 && len(p.FieldKeys) > 0 {
		buf = append(buf, ',')
		buf = append(buf, ' ')
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, '"')
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, "\": "...)

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf)
	}

	if len(p.TagKeys) > 0 || len(p.FieldKeys) > 0 {
		buf = append(buf, ", "...)
	}
	// Timestamps in ES must be millisecond precision:
	buf = append(buf, "\"timestamp\": "...)
	buf = fastFormatAppend(p.Timestamp.UTC().UnixNano()/1e6, buf)
	buf = append(buf, " }\n"...)

	_, err := w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)

	return err
}

// SerializeCassandra writes Point data to the given writer, conforming to the
// Cassandra query format.
//
// This function writes output that looks like:
// INSERT INTO <tablename> (series_id, ts_ns, value) VALUES (<series_id>, <timestamp_nanoseconds>, <field value>)
// where series_id looks like: <measurement>,<tagset>#<field name>#<time shard>
//
// For example:
// INSERT INTO all_series (series_id, timestamp_ns, value) VALUES ('cpu,hostname=host_01#user#2016-01-01', 12345, 42.1)\n
func (p *Point) SerializeCassandra(w io.Writer) (err error) {
	seriesIdPrefix := make([]byte, 0, 256)
	seriesIdPrefix = append(seriesIdPrefix, p.MeasurementName...)
	for i := 0; i < len(p.TagKeys); i++ {
		seriesIdPrefix = append(seriesIdPrefix, ',')
		seriesIdPrefix = append(seriesIdPrefix, p.TagKeys[i]...)
		seriesIdPrefix = append(seriesIdPrefix, '=')
		seriesIdPrefix = append(seriesIdPrefix, p.TagValues[i]...)
	}

	timestampNanos := p.Timestamp.UTC().UnixNano()
	timestampBucket := p.Timestamp.UTC().Format("2006-01-02")

	for fieldId := 0; fieldId < len(p.FieldKeys); fieldId++ {
		v := p.FieldValues[fieldId]
		tableName := fmt.Sprintf("measurements.series_%s", typeNameForCassandra(v))

		buf := make([]byte, 0, 256)
		buf = append(buf, []byte("INSERT INTO ")...)
		buf = append(buf, []byte(tableName)...)
		buf = append(buf, []byte(" (series_id, timestamp_ns, value) VALUES ('")...)
		buf = append(buf, seriesIdPrefix...)
		buf = append(buf, byte('#'))
		buf = append(buf, p.FieldKeys[fieldId]...)
		buf = append(buf, byte('#'))
		buf = append(buf, []byte(timestampBucket)...)
		buf = append(buf, byte('\''))
		buf = append(buf, ", "...)
		buf = append(buf, []byte(fmt.Sprintf("%d, ", timestampNanos))...)

		buf = fastFormatAppend(v, buf)

		buf = append(buf, []byte(")\n")...)

		_, err := w.Write(buf)
		if err != nil {
			return err
		}
	}

	return nil
}

// SerializeMongo writes Point data to the given writer, conforming to the
// mongo_serialization FlatBuffers format.
func (p *Point) SerializeMongo(w io.Writer) (err error) {
	// Prepare the series id prefix, which is the set of tags associated
	// with this point. The series id prefix is the base of each value's
	// particular collection name:
	lenBuf := bufPool8.Get().([]byte)

	// Prepare the timestamp, which is the same for each value in this
	// Point:
	timestampNanos := p.Timestamp.UTC().UnixNano()

	// Fetch a flatbuffers builder from a pool:
	builder := fbBuilderPool.Get().(*flatbuffers.Builder)

	// For each field in this Point, serialize its:
	// collection name (series id prefix + the name of the value)
	// timestamp in nanos (int64)
	// numeric value (int, int64, or float64 -- determined by reflection)
	tagOffsets := make([]flatbuffers.UOffsetT, 0, len(p.TagKeys))
	for fieldId := 0; fieldId < len(p.FieldKeys); fieldId++ {
		tagOffsets = tagOffsets[:0]
		builder.Reset()

		// write the tag data, which must be separate:
		for i := 0; i < len(p.TagKeys); i++ {
			keyData := builder.CreateByteVector(p.TagKeys[i])
			valData := builder.CreateByteVector(p.TagValues[i])
			mongo_serialization.TagStart(builder)
			mongo_serialization.TagAddKey(builder, keyData)
			mongo_serialization.TagAddVal(builder, valData)

			tagOffset := mongo_serialization.TagEnd(builder)
			tagOffsets = append(tagOffsets, tagOffset)
		}
		mongo_serialization.ItemStartTagsVector(builder, len(tagOffsets))
		for _, tagOffset := range tagOffsets {
			builder.PrependUOffsetT(tagOffset)
		}
		tagsVecOffset := builder.EndVector(len(tagOffsets))

		fieldName := p.FieldKeys[fieldId]
		genericValue := p.FieldValues[fieldId]

		// build the flatbuffer representing this point:
		measurementNameOffset := builder.CreateByteVector(p.MeasurementName)
		fieldNameOffset := builder.CreateByteVector(fieldName)

		mongo_serialization.ItemStart(builder)
		mongo_serialization.ItemAddTimestampNanos(builder, timestampNanos)
		mongo_serialization.ItemAddMeasurementName(builder, measurementNameOffset)
		mongo_serialization.ItemAddFieldName(builder, fieldNameOffset)
		mongo_serialization.ItemAddTags(builder, tagsVecOffset)

		switch v := genericValue.(type) {
		// (We can't switch on sets of types (e.g. int, int64) because
		// that does not make v concrete.)
		case int, int64:
			mongo_serialization.ItemAddValueType(builder, mongo_serialization.ValueTypeLong)
			switch v2 := v.(type) {
			case int:
				mongo_serialization.ItemAddLongValue(builder, int64(v2))
			case int64:
				mongo_serialization.ItemAddLongValue(builder, v2)
			}
		case float64:
			mongo_serialization.ItemAddValueType(builder, mongo_serialization.ValueTypeDouble)
			mongo_serialization.ItemAddDoubleValue(builder, v)
		default:
			panic(fmt.Sprintf("logic error in mongo serialization, %s", reflect.TypeOf(v)))
		}
		rootTable := mongo_serialization.ItemEnd(builder)
		builder.Finish(rootTable)

		// Access the finished byte slice representing this flatbuffer:
		buf := builder.FinishedBytes()

		// Write the metadata for the flatbuffer object:
		binary.LittleEndian.PutUint64(lenBuf, uint64(len(buf)))
		_, err = w.Write(lenBuf)
		if err != nil {
			return err
		}

		// Write the flatbuffer object:
		_, err := w.Write(buf)
		if err != nil {
			return err
		}
	}

	// Give the flatbuffers builder back to a pool:
	builder.Reset()
	fbBuilderPool.Put(builder)

	// Give the 8-byte buf back to a pool:
	bufPool8.Put(lenBuf)

	return nil
}

// SerializeOpenTSDBBulk writes Point data to the given writer, conforming to
// the OpenTSDB bulk load protocol (the /api/put endpoint). Note that no line
// has a trailing comma. Downstream programs are responsible for creating
// batches for POSTing using a JSON array, and for adding any trailing commas
// (to conform to JSON). We use only millisecond-precision timestamps.
//
// N.B. OpenTSDB only supports millisecond or second resolution timestamps.
// N.B. OpenTSDB millisecond timestamps must be 13 digits long.
// N.B. OpenTSDB only supports floating-point field values.
//
// This function writes JSON lines that looks like:
// { <metric>, <timestamp>, <value>, <tags> }
//
// For example:
// { "metric": "cpu.usage_user", "timestamp": 14516064000000, "value": 99.5170917755353770, "tags": { "hostname": "host_01", "region": "ap-southeast-2", "datacenter": "ap-southeast-2a" } }
func (p *Point) SerializeOpenTSDBBulk(w io.Writer) error {
	type wirePoint struct {
		Metric    string            `json:"metric"`
		Timestamp int64             `json:"timestamp"`
		Tags      map[string]string `json:"tags"`
		Value     float64           `json:"value"`
	}

	metricBase := string(p.MeasurementName) // will be re-used
	encoder := json.NewEncoder(w)

	wp := wirePoint{}
	// Timestamps in OpenTSDB must be millisecond precision:
	wp.Timestamp = p.Timestamp.UTC().UnixNano() / 1e6
	// sanity check
	{
		x := fmt.Sprintf("%d", wp.Timestamp)
		if len(x) != 13 {
			panic("serialized timestamp was not 13 digits")
		}
	}
	wp.Tags = make(map[string]string, len(p.TagKeys))
	for i := 0; i < len(p.TagKeys); i++ {
		// so many allocs..
		key := string(p.TagKeys[i])
		val := string(p.TagValues[i])
		wp.Tags[key] = val
	}

	// for each Value, generate a new line in the output:
	for i := 0; i < len(p.FieldKeys); i++ {
		wp.Metric = metricBase + "." + string(p.FieldKeys[i])
		switch x := p.FieldValues[i].(type) {
		case int:
			wp.Value = float64(x)
		case int64:
			wp.Value = float64(x)
		case float32:
			wp.Value = float64(x)
		case float64:
			wp.Value = float64(x)
		default:
			panic("bad numeric value for OpenTSDB serialization")
		}

		err := encoder.Encode(wp)
		if err != nil {
			return err
		}
	}

	return nil
}

// SerializeTimeScale writes Point data to the given writer, conforming to the
// TimeScale insert format.
//
// This function writes output that looks like:
// INSERT INTO <tablename> (time,<tag_name list>,<field_name list>') VALUES (<timestamp in nanoseconds>, <tag values list>, <field values>)
//
// For example:
// INSERT INTO all_series (series_id, timestamp_ns, value) VALUES ('cpu,hostname=host_01#user#2016-01-01', 12345, 42.1)\n
func (p *Point) SerializeTimeScale(w io.Writer) (err error) {
	timestampNanos := p.Timestamp.UTC().UnixNano()
	buf := make([]byte, 0, 4096)
	buf = append(buf, []byte("INSERT INTO ")...)
	buf = append(buf, []byte(p.MeasurementName)...)
	buf = append(buf, []byte(" (time")...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ","...)
		buf = append(buf, p.TagKeys[i]...)
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, ","...)
		buf = append(buf, p.FieldKeys[i]...)
	}
	buf = append(buf, []byte(") VALUES (")...)
	buf = append(buf, []byte(fmt.Sprintf("%d", timestampNanos))...)

	for i := 0; i < len(p.TagValues); i++ {
		buf = append(buf, ",'"...)
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, byte('\''))
	}

	for i := 0; i < len(p.FieldValues); i++ {
		buf = append(buf, ","...)
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf)
	}
	buf = append(buf, []byte(");\n")...)

	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

// SerializeTimeScaleBin writes Point data to the given writer, conforming to the
// Binary GOP encoded format to write
//
//
func (p *Point) SerializeTimeScaleBin(w io.Writer) (err error) {

	var f timescale_serialization.FlatPoint
	f.MeasurementName = string(p.MeasurementName)
	// Write the batch.
	f.Columns = make([]string, len(p.TagKeys)+len(p.FieldKeys)+1)
	c := 0
	for i := 0; i < len(p.TagKeys); i++ {
		f.Columns[c] = string(p.TagKeys[i])
		c++
	}
	for i := 0; i < len(p.FieldKeys); i++ {
		f.Columns[c] = string(p.FieldKeys[i])
		c++
	}
	f.Columns[c] = "time"

	c = 0
	f.Values = make([]*timescale_serialization.FlatPoint_FlatPointValue, len(p.TagValues)+len(p.FieldValues)+1)
	for i := 0; i < len(p.TagValues); i++ {
		v := timescale_serialization.FlatPoint_FlatPointValue{}
		v.Type = timescale_serialization.FlatPoint_STRING
		v.StringVal = string(p.TagValues[i])
		f.Values[c] = &v
		c++
	}
	for i := 0; i < len(p.FieldValues); i++ {
		v := timescale_serialization.FlatPoint_FlatPointValue{}
		switch p.FieldValues[i].(type) {
		case int64:
			v.Type = timescale_serialization.FlatPoint_INTEGER
			v.IntVal = p.FieldValues[i].(int64)
			break
		case int:
			v.Type = timescale_serialization.FlatPoint_INTEGER
			v.IntVal = int64(p.FieldValues[i].(int))
			break
		case float64:
			v.Type = timescale_serialization.FlatPoint_FLOAT
			v.DoubleVal = p.FieldValues[i].(float64)
			break
		case string:
			v.Type = timescale_serialization.FlatPoint_STRING
			v.StringVal = p.FieldValues[i].(string)
			break
		default:
			panic(fmt.Sprintf("logic error in timescale serialization, %s", reflect.TypeOf(v)))
		}
		f.Values[c] = &v
		c++
	}
	timeVal := timescale_serialization.FlatPoint_FlatPointValue{}
	timeVal.Type = timescale_serialization.FlatPoint_INTEGER
	timeVal.IntVal = p.Timestamp.UnixNano()
	f.Values[c] = &timeVal

	out, err := f.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	s := uint64(len(out))
	binary.Write(w, binary.LittleEndian, s)
	w.Write(out)
	return nil
}

func typeNameForCassandra(v interface{}) string {
	switch v.(type) {
	case int, int64:
		return "bigint"
	case float64:
		return "double"
	case float32:
		return "float"
	case bool:
		return "boolean"
	case []byte, string:
		return "blob"
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

func fastFormatAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', 16, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', 16, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, v.([]byte)...)
		return buf
	case string:
		buf = append(buf, v.(string)...)
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}