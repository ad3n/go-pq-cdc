package snapshot

import (
	"github.com/jackc/pgx/v5/pgtype"
	"testing"
)

func BenchmarkDecodeColumn(b *testing.B) {
	s := &Snapshotter{typeMap: pgtype.NewMap()}
	data := []byte("42")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.decodeColumnData(data, pgtype.Int4OID); err != nil {
			b.Fatal(err)
		}
	}
}
