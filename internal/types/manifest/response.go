package manifest

type Response[T any] struct {
	Response T `json:"Response"`
}
