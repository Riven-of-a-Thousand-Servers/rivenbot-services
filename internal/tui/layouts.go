package ui

type layout struct {
	filesW, logsW int
	logsInnerH    int
	logsInnerW    int
	topH          int
	runtimeW      int
	runtimeH      int
	gaugeW        int
	gaugeH        int
}

func computeLayout(width, height int) layout {
	const (
		gaugeH, runtimeH = 7, 7
		footerH          = 1
		headerH          = 1
		minFiles         = 118
	)

	topH := max(0, height-gaugeH-footerH-headerH-2)
	filesW := min(width, max(minFiles, width*1/2))
	logsW := width - filesW
	gaugeW := width * 1 / 2
	runtimeW := width - gaugeW
	logsInnerH := max(0, topH-2)
	logsInnerW := max(0, logsW-2)

	return layout{
		filesW:     filesW,
		logsW:      logsW,
		topH:       topH,
		gaugeW:     gaugeW,
		gaugeH:     gaugeH,
		runtimeH:   runtimeH,
		runtimeW:   runtimeW,
		logsInnerH: logsInnerH,
		logsInnerW: logsInnerW,
	}
}
