package dataset

type RawContent []byte

func (r RawContent) Len() int64 {
	return int64(len(r))
}
