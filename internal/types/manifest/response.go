package manifest

type Response struct {
	Response Entry `json:"Response"`
}

type Entry struct {
	Mode                      int               `json:"directActivityModeType"`
	DisplayProperties         DisplayProperties `json:"displayProperties"`
	OriginalDisplayProperties DisplayProperties `json:"originalDisplayProperties"`
	ReleaseIcon               string            `json:"releaseIcon"`
	ReleaseTime               int               `json:"releaseTime"`
	ItemType                  int               `json:"itemType"`
	EquippingBlock            EquippingBlock    `json:"equippingBlock"`
}

type DisplayProperties struct {
	Description string `json:"description"`
	Name        string `json:"name"`
	Icon        string `json:"icon"`
	HasIcon     bool   `json:"hasIcon"`
}

type EquippingBlock struct {
	EquipmentSlotTypeHash int `json:"equipmentSlotTypeHash"`
	AmmoType              int `json:"ammoType"`
}
