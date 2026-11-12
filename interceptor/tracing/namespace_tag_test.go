package tracing_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/interceptor/tracing"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type recordedSpan struct {
	operation string
	tags      map[string]string
}

type spanRecorder struct {
	mu    sync.Mutex
	spans []recordedSpan
}

func (r *spanRecorder) record(options *tracing.TracerStartSpanOptions) tracing.TracerSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, recordedSpan{operation: options.Operation, tags: maps.Clone(options.Tags)})
	return noopSpan{}
}

func (r *spanRecorder) recorded() []recordedSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.spans)
}

type noopSpan struct{}

func (noopSpan) Finish(*tracing.TracerFinishSpanOptions) {}

type tracerCommon struct {
	tracing.BaseTracer
	recorder *spanRecorder
}

func (tracerCommon) Options() tracing.TracerOptions {
	return tracing.TracerOptions{HeaderKey: "namespace-tag-test", AddTemporalSpans: true}
}

func (tracerCommon) UnmarshalSpan(map[string]string) (tracing.TracerSpanRef, error) { return nil, nil }

func (tracerCommon) MarshalSpan(tracing.TracerSpanRef) (map[string]string, error) { return nil, nil }

type testTracer struct{ tracerCommon }

func (testTracer) SpanFromContext(context.Context) tracing.TracerSpanRef { return nil }

func (testTracer) ContextWithSpan(ctx context.Context, _ tracing.TracerSpanRef) context.Context {
	return ctx
}

func (t testTracer) CreateSpan(_ context.Context, options *tracing.TracerStartSpanOptions) tracing.TracerSpan {
	return t.recorder.record(options)
}

type testWorkflowTracer struct{ tracerCommon }

func (testWorkflowTracer) SpanFromContext(workflow.Context) tracing.TracerSpanRef { return nil }

func (testWorkflowTracer) ContextWithSpan(ctx workflow.Context, _ tracing.TracerSpanRef) workflow.Context {
	return ctx
}

func (t testWorkflowTracer) CreateSpan(_ workflow.Context, options *tracing.TracerStartSpanOptions) tracing.TracerSpan {
	return t.recorder.record(options)
}

func newTestInterceptors(recorder *spanRecorder) (interceptor.ClientInterceptor, interceptor.WorkerInterceptor) {
	common := tracerCommon{recorder: recorder}
	return tracing.NewTracingInterceptor(testTracer{common}, testWorkflowTracer{common})
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

func namespaceTagActivity(context.Context) error { return nil }

func namespaceTagWorkflow(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, "my-query", func() (string, error) { return "ok", nil }); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "my-update",
		func(workflow.Context) error { return nil },
		workflow.UpdateHandlerOptions{Validator: func() error { return nil }},
	); err != nil {
		return err
	}
	workflow.GetSignalChannel(ctx, "my-signal").Receive(ctx, nil)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	return workflow.ExecuteActivity(ctx, namespaceTagActivity).Get(ctx, nil)
}

func TestNamespaceTagOnWorkerSpans(t *testing.T) {
	recorder := &spanRecorder{}
	_, workerInterceptor := newTestInterceptors(recorder)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{workerInterceptor}})
	env.RegisterWorkflow(namespaceTagWorkflow)
	env.RegisterActivity(namespaceTagActivity)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("my-update", "update-id", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { require.Fail(t, "update rejected", err) },
			OnAccept:   func() {},
			OnComplete: func(any, error) {},
		})
		env.SignalWorkflow("my-signal", nil)
	}, time.Second)
	env.ExecuteWorkflow(namespaceTagWorkflow)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	_, err := env.QueryWorkflow("my-query")
	require.NoError(t, err)

	requireNamespaceTag(t, recorder.recorded(), "default-test-namespace",
		"RunWorkflow", "HandleSignal", "HandleQuery", "ValidateUpdate", "HandleUpdate", "StartActivity", "RunActivity")
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
	service.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.RequestCancelWorkflowExecutionResponse{}, nil)
	service.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflowservice.TerminateWorkflowExecutionResponse{}, nil)

	recorder := &spanRecorder{}
	clientInterceptor, _ := newTestInterceptors(recorder)
	c := internal.NewServiceClient(service, nil, internal.ClientOptions{
		Namespace:    "my-namespace",
		Interceptors: []interceptor.ClientInterceptor{clientInterceptor},
	})
	defer c.Close()

	ctx := t.Context()
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "wf-id", TaskQueue: "tq"}, "MyWorkflow")
	require.NoError(t, err)
	require.NoError(t, c.SignalWorkflow(ctx, "wf-id", "", "my-signal", nil))
	_, err = c.SignalWithStartWorkflow(ctx, "wf-id", "my-signal", nil, client.StartWorkflowOptions{TaskQueue: "tq"}, "MyWorkflow")
	require.NoError(t, err)
	require.NoError(t, c.CancelWorkflow(ctx, "wf-id", ""))
	require.NoError(t, c.TerminateWorkflow(ctx, "wf-id", "", "done"))

	requireNamespaceTag(t, recorder.recorded(), "my-namespace",
		"StartWorkflow", "SignalWorkflow", "SignalWithStartWorkflow", "CancelWorkflow", "TerminateWorkflow")
}
