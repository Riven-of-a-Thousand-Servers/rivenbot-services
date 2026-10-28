package telemetry

type Gauge struct {
	id        int
	name      string
	unit      string
	gaugeFunc GaugeFunc
}

type GaugeView struct {
	Id     int
	Name   string
	Unit   string
	Length int
	Cap    int
}

// Represents the atomic unit of work for the TUI
// which is processing a zst-compressed file
type GaugeFunc func() (gaugeLen, gaugeCap int)

func (q *Gauge) View() GaugeView {
	length, cap := q.gaugeFunc()
	return GaugeView{
		Id:     q.id,
		Name:   q.name,
		Unit:   q.unit,
		Length: length,
		Cap:    cap,
	}
}
