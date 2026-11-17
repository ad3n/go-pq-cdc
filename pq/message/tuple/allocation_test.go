package tuple

import (
	"encoding/binary"
	"strconv"
	"testing"
)

func BenchmarkDecodeTuple(b *testing.B) {
	for _, count := range []int{4, 32} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			data := binary.BigEndian.AppendUint16(nil, uint16(count))
			for range count {
				data = append(data, DataTypeText)
				data = binary.BigEndian.AppendUint32(data, 5)
				data = append(data, "value"...)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var d Data
				if err := d.Decode(data, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDecodeOwnsColumnsAndReplacesPreviousTuple(t *testing.T) {
	data := []byte{0, 2, DataTypeText, 0, 0, 0, 1, 'x', DataTypeNull}
	var d Data
	if err := d.Decode(data, 0); err != nil {
		t.Fatal(err)
	}

	previous := d.Columns
	data[7] = 'y'
	if string(previous[0].Data) != "x" {
		t.Fatal("decoded data aliases input")
	}

	if err := d.Decode([]byte{0, 1, DataTypeToast}, 0); err != nil {
		t.Fatal(err)
	}

	if len(d.Columns) != 1 || d.Columns[0].DataType != DataTypeToast {
		t.Fatal("previous tuple retained")
	}

	if string(previous[0].Data) != "x" {
		t.Fatal("previous tuple mutated")
	}
}

func TestDecodeRejectsImpossibleColumnCount(t *testing.T) {
	var d Data
	if err := d.Decode([]byte{255, 255, DataTypeNull}, 0); err == nil {
		t.Fatal("expected truncated tuple error")
	}
}
