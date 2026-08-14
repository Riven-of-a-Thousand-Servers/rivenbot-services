package manifest

// RawComponent represents calling the direct manifest definition
// gotten from /Destiny2/Manifest and individually creating the URL
// from the returned content paths
type RawComponent[T any] map[string]T
