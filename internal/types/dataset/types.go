package dataset

type (
	RawContent []byte

	Entry struct {
		Payload    RawContent
		LineOffset int
	}
)

func (r RawContent) Len() int64 {
	return int64(len(r))
}
