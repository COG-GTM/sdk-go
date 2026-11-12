package interceptor_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/internal/interceptortest"
)

type testTracer struct {
	interceptor.BaseTracer
	T *testing.T
}

type testSpan struct{}

func (t testSpan) Finish(options *interceptor.TracerFinishSpanOptions) {}

type testSpanRef struct{}

func (t testTracer) Options() interceptor.TracerOptions {
	return interceptor.TracerOptions{
		SpanContextKey:       "test-tracer",
		HeaderKey:            "test-tracer",
		DisableSignalTracing: false,
		DisableQueryTracing:  false,
	}
}

func (t *testTracer) UnmarshalSpan(m map[string]string) (interceptor.TracerSpanRef, error) {
	return testSpanRef{}, nil
}

func (t *testTracer) MarshalSpan(span interceptor.TracerSpan) (map[string]string, error) {
	return map[string]string{}, nil
}

func (t *testTracer) SpanFromContext(ctx context.Context) interceptor.TracerSpan {
	return testSpan{}
}

func (t *testTracer) ContextWithSpan(ctx context.Context, span interceptor.TracerSpan) context.Context {
	return ctx
}

func (t *testTracer) StartSpan(options *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	// Require start time to be set
	if options.Time.IsZero() {
		switch options.Operation {
		case "RunWorkflow", "RunActivity":
		// Do nothing; the test env doesn't set these at the moment.
		default:
			t.T.Errorf("Got zero value for span start time: %v", options)
		}
	}
	return testSpan{}, nil
}

func TestSpanTimestamps(t *testing.T) {
	interceptortest.RunTestWorkflow(t, &testTracer{T: t})
}

type recordedSpan struct {
	operation string
	tags      map[string]string
}

type tagRecordingTracer struct {
	interceptor.BaseTracer
	mu    sync.Mutex
	spans []recordedSpan
}

func (t *tagRecordingTracer) Options() interceptor.TracerOptions {
	return interceptor.TracerOptions{SpanContextKey: "tag-recording-tracer", HeaderKey: "tag-recording-tracer"}
}

func (t *tagRecordingTracer) UnmarshalSpan(map[string]string) (interceptor.TracerSpanRef, error) {
	return nil, nil
}

func (t *tagRecordingTracer) MarshalSpan(interceptor.TracerSpan) (map[string]string, error) {
	return nil, nil
}

func (t *tagRecordingTracer) SpanFromContext(context.Context) interceptor.TracerSpan { return nil }

func (t *tagRecordingTracer) ContextWithSpan(ctx context.Context, _ interceptor.TracerSpan) context.Context {
	return ctx
}

func (t *tagRecordingTracer) StartSpan(options *interceptor.TracerStartSpanOptions) (interceptor.TracerSpan, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.spans = append(t.spans, recordedSpan{operation: options.Operation, tags: maps.Clone(options.Tags)})
	return testSpan{}, nil
}

func (t *tagRecordingTracer) recorded() []recordedSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.spans)
}

func requireNamespaceTag(t *testing.T, spans []recordedSpan, namespace string, operations ...string) {
	t.Helper()
	seen := map[string]bool{}
	for _, span := range spans {
		seen[span.operation] = true
		require.Equal(t, namespace, span.tags["temporalNamespace"], "temporalNamespace tag on %s span", span.operation)
	}
	for _, op := range operations {
		require.True(t, seen[op], "expected a %s span, got %v", op, spans)
	}
}

func TestNamespaceTagOnWorkerSpans(t *testing.T) {
	tracer := &tagRecordingTracer{}
	interceptortest.RunTestWorkflow(t, tracer)
	requireNamespaceTag(t, tracer.recorded(), "default-test-namespace",
		"RunWorkflow", "StartActivity", "RunActivity", "StartChildWorkflow", "SignalChildWorkflow",
		"HandleSignal", "HandleQuery", "ValidateUpdate", "HandleUpdate",
		"StartNexusOperation", "RunStartNexusOperationHandler", "RunCancelNexusOperationHandler")
}

func TestNamespaceTagOnClientSpans(t *testing.T) {
	service := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	service.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	service.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil)
	service.EXPECT().SignalWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.SignalWorkflowExecutionResponse{}, nil)
	service.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: "run-id"}, nil)

	tracer := &tagRecordingTracer{}
	c := internal.NewServiceClient(service, nil, internal.ClientOptions{
		Namespace:    "my-namespace",
		Interceptors: []interceptor.ClientInterceptor{interceptor.NewTracingInterceptor(tracer)},
	})
	defer c.Close()

	ctx := t.Context()
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "wf-id", TaskQueue: "tq"}, "MyWorkflow")
	require.NoError(t, err)
	require.NoError(t, c.SignalWorkflow(ctx, "wf-id", "", "my-signal", nil))
	_, err = c.SignalWithStartWorkflow(ctx, "wf-id", "my-signal", nil, client.StartWorkflowOptions{TaskQueue: "tq"}, "MyWorkflow")
	require.NoError(t, err)

	requireNamespaceTag(t, tracer.recorded(), "my-namespace", "StartWorkflow", "SignalWorkflow", "SignalWithStartWorkflow")
}
