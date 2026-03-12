package pool

import (
	"bytes"
	"testing"
)

func TestBytesBufferPool(t *testing.T) {
	bufPool := New(func() *bytes.Buffer {
		return new(bytes.Buffer)
	})
	b := bufPool.Get()
	b.Reset()
	b.WriteString("hello world")

	t.Logf("buffer pool size: %d and content %s", b.Len(), b.String())
	bufPool.Put(b)
}

type RequestContext struct {
	Id     string
	Buffer *bytes.Buffer
}

func TestRequestContextPool(t *testing.T) {
	ctxPool := New(func() *RequestContext {
		return &RequestContext{
			Buffer: new(bytes.Buffer),
		}
	})
	ctx := ctxPool.Get()
	ctx.Buffer.Reset()

	ctx.Id = "ABC123"
	ctx.Buffer.WriteString("hello world request payload")
	ctxPool.Put(ctx)

	reuseCtx := ctxPool.Get()
	t.Logf("reuse context size: %d, Id: %s and payload: %s", reuseCtx.Buffer.Len(), reuseCtx.Id, reuseCtx.Buffer.String())
}
