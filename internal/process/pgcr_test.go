package process

import (
	"context"
	"fmt"

	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/types/pgcr"
)

type fakeMapper struct {
	shouldErr bool
}

// PgcrToPgcrInfo implements [mapper.Mapper].
func (f *fakeMapper) PgcrToPgcrInfo(context.Context, *pgcr.PostGameCarnageReport) (*pgcr.PgcrInfo, error) {
	if f.shouldErr {
		return nil, fmt.Errorf("Error while mapping pgcr to pgcrInfo")
	}
	return &pgcr.PgcrInfo{}, nil
}

// WeaponInfoToDBEntity implements [mapper.Mapper].
func (f *fakeMapper) WeaponInfoToDBEntity(context.Context, *pgcr.WeaponInfo) (db.CreateWeaponParams, error) {
	if f.shouldErr {
		return db.CreateWeaponParams{}, fmt.Errorf("Error while mapping WeaponInfo to DB entity")
	}

	return db.CreateWeaponParams{}, nil
}

// var _ mapper.Mapper = (*fakeMapper)(nil)
//
// func TestProcessSuccessful(t *testing.T) {
// 	mockDb, sqlmock, err := sqlmock.New()
// 	if err != nil {
// 		t.Fatalf("Unable to create sqlmock: %v", err)
// 	}
//
// 	queries := db.New(mockDb)
// 	sut := NewPgcrProcessor(mockDb, queries, &fakeMapper{})
//
//
// }
