package manifest

type Entry struct {
	Mode                      int               `json:"directActivityModeType"`
	DisplayProperties         DisplayProperties `json:"displayProperties"`
	OriginalDisplayProperties DisplayProperties `json:"originalDisplayProperties"`
	ReleaseIcon               string            `json:"releaseIcon"`
	ReleaseTime               int               `json:"releaseTime"`
	ItemType                  int               `json:"itemType"`
	DefaultDamageTypeHash     int64             `json:"defaultDamageTypeHash"`
	DamageTypeHashes          []int64           `json:"damageTypeHashes"`
	EquippingBlock            EquippingBlock    `json:"equippingBlock"`
}

type DisplayProperties struct {
	Description string `json:"description"`
	Name        string `json:"name"`
	Icon        string `json:"icon"`
	HasIcon     bool   `json:"hasIcon"`
}

type EquippingBlock struct {
	EquipmentSlotTypeHash int64 `json:"equipmentSlotTypeHash"`
	AmmoType              int   `json:"ammoType"`
}
