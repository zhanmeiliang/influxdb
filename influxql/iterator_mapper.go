package influxql

type iteratorMapper struct {
	e *Emitter
}

func NewIteratorMapper(itrs []Iterator, opt IteratorOptions) Iterator {
	e := NewEmitter(itrs, opt.Ascending, 0)
	e.OmitTime = true
	return &iteratorMapper{e: e}
}

func (itr *iteratorMapper) Next() (*FloatPoint, error) {
	t, name, tags, err := itr.e.loadBuf()
	if err != nil || t == ZeroTime {
		return nil, err
	}
	return &FloatPoint{
		Name: name,
		Tags: tags,
		Time: t,
		Aux:  itr.e.readAt(t, name, tags),
	}, nil
}

func (itr *iteratorMapper) Stats() IteratorStats {
	stats := IteratorStats{}
	for _, itr := range itr.e.itrs {
		stats.Add(itr.Stats())
	}
	return stats
}

func (itr *iteratorMapper) Close() error {
	return itr.e.Close()
}
