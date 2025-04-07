package reader

import (
	"errors"
	"fmt"
	"io"
)

func (r *DataReader) decode2Chan() {
	// 1 means running
	if r.inputLock.CompareAndSwap(0, 1) {
		defer r.inputLock.Store(0)
	} else {
		return
	}
	var err error
	defer func() {
		if rc := recover(); rc != nil {
			r.err = errors.Join(r.err, err, fmt.Errorf("panic %v", rc))
		}
	}()
	defer close(r.anyChan)
	b := true
	for {
		datumBytes, err := r.br.ReadBytes(r.delim)
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.err = nil
				return
			}
			r.err = err
			return
		}
		datum, err := InputMap(datumBytes[:len(datumBytes)-1])
		if err != nil {
			r.err = errors.Join(r.err, err)
			continue
		}
		r.anyChan <- datum
		r.inputCount++
		if b {
			r.wg.Done() // sync.WaitGroup to allow Next() to wait for records to be available
			b = false
		}
		select {
		case <-r.readerCtx.Done():
			return
		default:
		}
	}
}

// recordFactory... the hits just keep on coming
func (r *DataReader) recordFactory() {
	if r.factoryLock.CompareAndSwap(0, 1) {
		defer r.factoryLock.Store(0)
	} else {
		return
	}
	defer close(r.recChan)
	recChunk := 0

	r.wg.Done() // sync.WaitGroup to allow Next() to wait for records to be available

	switch {
	case r.chunk < 1:
		for data := range r.anyChan {
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.err = err
				return
			}
			select {
			case <-r.readerCtx.Done():
				r.bldDone <- struct{}{}
				return
			case <-r.recReq:
				r.recChan <- r.bld.NewRecord()
			default:
			}
		}
		r.recChan <- r.bld.NewRecord()
		r.bldDone <- struct{}{}
	case r.chunk >= 1:
		for data := range r.anyChan {
			if recChunk == 0 {
				r.bld.Reserve(r.chunk)
			}
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.err = err
				return
			}
			recChunk++
			if recChunk >= r.chunk {
				r.recChan <- r.bld.NewRecord()
				recChunk = 0
			}
			select {
			case <-r.readerCtx.Done():
				if len(r.anyChan) == 0 {
					break
				}
			default:
			}
		}
		if recChunk != 0 {
			r.recChan <- r.bld.NewRecord()
		}
		r.bldDone <- struct{}{}
	}
}
