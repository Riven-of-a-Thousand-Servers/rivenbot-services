package ui

type layout struct {
	filesW, logsW int
	logsInnerH    int
	logsInnerW    int
	topH          int
	gaugeW        int
	gaugeH        int
}

func computeLayout(width, height int) layout {
	const (
		gaugeH   = 7
		footerH  = 1
		minFiles = 118
	)

	topH := max(0, height-gaugeH-footerH)
	filesW := min(width, max(minFiles, width*3/5))
	logsW := width - filesW
	gaugeW := width
	logsInnerH := max(0, topH-2)
	logsInnerW := max(0, logsW-2)

	return layout{
		filesW:     filesW,
		logsW:      logsW,
		topH:       topH,
		gaugeW:     gaugeW,
		gaugeH:     gaugeH,
		logsInnerH: logsInnerH,
		logsInnerW: logsInnerW,
	}
}
