package mapper

import (
	"context"

	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/types/pgcr"
)

type Mapper interface {
	WeaponInfoToDBEntity(context.Context, *pgcr.WeaponInfo) (db.CreateWeaponParams, error)
	PgcrToPgcrInfo(context.Context, *pgcr.PostGameCarnageReport) (*pgcr.PgcrInfo, error)
}
