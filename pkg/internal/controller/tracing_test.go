/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type capturingLogSink struct {
	mu     sync.Mutex
	values map[string]any
}

func newCapturingLogSink() *capturingLogSink {
	return &capturingLogSink{values: make(map[string]any)}
}

func (s *capturingLogSink) Init(logr.RuntimeInfo) {}

func (s *capturingLogSink) Enabled(int) bool { return true }

func (s *capturingLogSink) Info(_ int, _ string, keysAndValues ...any) {
	s.record(keysAndValues...)
}

func (s *capturingLogSink) Error(_ error, _ string, keysAndValues ...any) {
	s.record(keysAndValues...)
}

func (s *capturingLogSink) WithValues(keysAndValues ...any) logr.LogSink {
	s.record(keysAndValues...)
	return s
}

func (s *capturingLogSink) WithName(string) logr.LogSink {
	return s
}

func (s *capturingLogSink) record(keysAndValues ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if k, ok := keysAndValues[i].(string); ok {
			s.values[k] = keysAndValues[i+1]
		}
	}
}

func (s *capturingLogSink) getString(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, _ := s.values[key].(string)
	return v
}

func (s *capturingLogSink) getAny(key string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key]
}

func spanAttrMap(attrs []attribute.KeyValue) map[string]any {
	m := make(map[string]any, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.AsInterface()
	}
	return m
}

var _ = Describe("Distributed Tracing in reconcileHandler", func() {
	var (
		exporter  *tracetest.InMemoryExporter
		tp        *sdktrace.TracerProvider
		scheme    *runtime.Scheme
		deployGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	)

	BeforeEach(func() {
		exporter = tracetest.NewInMemoryExporter()
		tp = sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		scheme = runtime.NewScheme()
		Expect(appsv1.AddToScheme(scheme)).To(Succeed())
	})

	AfterEach(func() {
		Expect(tp.Shutdown(context.Background())).To(Succeed())
	})

	Describe("ObjectCarrier", func() {
		It("should round-trip W3C traceparent and tracestate via prefixed Kubernetes annotations", func() {
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "demo",
					Namespace: "default",
				},
			}
			carrier := NewObjectCarrier(deploy)
			Expect(carrier.Get("traceparent")).To(BeEmpty())
			Expect(carrier.Keys()).To(BeNil())

			// Ignoring empty Set values should leave Annotations nil.
			carrier.Set("traceparent", "")
			Expect(deploy.Annotations).To(BeNil())

			traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
			Expect(err).NotTo(HaveOccurred())
			spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
			Expect(err).NotTo(HaveOccurred())
			ts, err := trace.ParseTraceState("vendor=val")
			Expect(err).NotTo(HaveOccurred())

			sc := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceFlags: trace.FlagsSampled,
				TraceState: ts,
				Remote:     true,
			})
			ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)

			prop := propagation.TraceContext{}
			prop.Inject(ctx, carrier)

			Expect(deploy.Annotations).To(HaveKeyWithValue(
				TraceparentAnnotationKey,
				"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			))
			Expect(deploy.Annotations).To(HaveKeyWithValue(
				TracestateAnnotationKey,
				"vendor=val",
			))
			// Case-insensitive lookup and sorted Keys().
			Expect(carrier.Get("TRACEPARENT")).To(Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
			Expect(carrier.Get("TRACESTATE")).To(Equal("vendor=val"))
			Expect(carrier.Keys()).To(Equal([]string{"traceparent", "tracestate"}))

			extractedCtx := prop.Extract(context.Background(), carrier)
			extractedSC := trace.SpanContextFromContext(extractedCtx)
			Expect(extractedSC.IsValid()).To(BeTrue())
			Expect(extractedSC.TraceID()).To(Equal(traceID))
			Expect(extractedSC.SpanID()).To(Equal(spanID))
			Expect(extractedSC.TraceState().Get("vendor")).To(Equal("val"))
		})

		It("should handle nil and typed-nil metav1.Object safely without panicking", func() {
			carrier := NewObjectCarrier(nil)
			Expect(carrier.Get("traceparent")).To(BeEmpty())
			carrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			Expect(carrier.Keys()).To(BeNil())

			var typedNil *appsv1.Deployment
			typedNilCarrier := NewObjectCarrier(typedNil)
			Expect(typedNilCarrier.Get("traceparent")).To(BeEmpty())
			typedNilCarrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			Expect(typedNilCarrier.Keys()).To(BeNil())
		})
	})

	Describe("reconcileHandler fast path and Span Links", func() {
		It("should execute zero tracing logic when TracerProvider is nil", func(ctx SpecContext) {
			extractorCalled := false
			q := priorityqueue.New[reconcile.Request]("fast-path")
			defer q.ShutDown()

			c := New(Options[reconcile.Request]{
				Name:                    "fast-path",
				MaxConcurrentReconciles: 1,
				Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
					return reconcile.Result{}, nil
				}),
				TraceMetadataExtractor: func(context.Context, reconcile.Request) (ObjectTraceMetadata, error) {
					extractorCalled = true
					return ObjectTraceMetadata{}, nil
				},
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.Discard()
				},
			})
			c.Queue = q

			c.reconcileHandler(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"},
			}, 0)

			Expect(extractorCalled).To(BeFalse())
			Expect(exporter.GetSpans()).To(BeEmpty())

			// Also verify noop.NewTracerProvider() takes the zero-overhead fast path.
			cNoop := New(Options[reconcile.Request]{
				Name:                    "fast-path-noop",
				MaxConcurrentReconciles: 1,
				TracerProvider:          noop.NewTracerProvider(),
				Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
					return reconcile.Result{}, nil
				}),
				TraceMetadataExtractor: func(context.Context, reconcile.Request) (ObjectTraceMetadata, error) {
					extractorCalled = true
					return ObjectTraceMetadata{}, nil
				},
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.Discard()
				},
			})
			cNoop.Queue = q
			cNoop.reconcileHandler(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo"},
			}, 0)
			Expect(extractorCalled).To(BeFalse())
		})

		It("should start an independent root span with a Span Link to tracing.k8s.io/traceparent and enrich klog/logr context", func(ctx SpecContext) {
			mutationTraceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
			Expect(err).NotTo(HaveOccurred())
			mutationSpanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "demo-deploy",
					Namespace:  "default",
					UID:        types.UID("uid-abc-123"),
					Generation: 7,
					Annotations: map[string]string{
						TraceparentAnnotationKey: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					},
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deploy).Build()

			sink := newCapturingLogSink()
			q := priorityqueue.New[reconcile.Request]("deployment")
			defer q.ShutDown()

			var (
				observedLogrTraceID string
				observedLogrSpanID  string
				observedKlogTraceID string
				observedKlogSpanID  string
				observedSpanCtx     trace.SpanContext
			)

			prototype := &appsv1.Deployment{}
			prototype.SetGroupVersionKind(deployGVK)

			c := New(Options[reconcile.Request]{
				Name:                    "deployment",
				MaxConcurrentReconciles: 1,
				TracerProvider:          tp,
				TraceMetadataExtractor:  NewCacheTraceMetadataExtractor(fakeClient, prototype, deployGVK, nil),
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.New(sink)
				},
				Do: reconcile.Func(func(recCtx context.Context, _ reconcile.Request) (reconcile.Result, error) {
					observedSpanCtx = trace.SpanFromContext(recCtx).SpanContext()

					// Verify controller-runtime log.FromContext carries trace_id and span_id.
					logf.FromContext(recCtx).Info("reconciling via logf")
					observedLogrTraceID = sink.getString("trace_id")
					observedLogrSpanID = sink.getString("span_id")

					// Verify klog.FromContext carries trace_id and span_id.
					klog.FromContext(recCtx).Info("reconciling via klog")
					observedKlogTraceID = sink.getString("trace_id")
					observedKlogSpanID = sink.getString("span_id")

					return reconcile.Result{}, nil
				}),
			})
			c.Queue = q

			// Even if the caller context has an ambient span, reconcileHandler MUST start a new root span.
			ambientCtx, ambientSpan := tp.Tracer("ambient").Start(ctx, "ambient-parent")
			defer ambientSpan.End()

			c.reconcileHandler(ambientCtx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "demo-deploy"},
			}, 0)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			recSpan := spans[0]

			Expect(recSpan.Name).To(Equal("ReconcileDeployment"))
			Expect(recSpan.SpanKind).To(Equal(trace.SpanKindInternal))
			// Verify independent root span (no parent span, new TraceID distinct from mutation and ambient span).
			Expect(recSpan.Parent.IsValid()).To(BeFalse())
			Expect(recSpan.SpanContext.TraceID()).NotTo(Equal(mutationTraceID))
			Expect(recSpan.SpanContext.TraceID()).NotTo(Equal(ambientSpan.SpanContext().TraceID()))

			// Verify OpenTelemetry Span Link points to the mutation's SpanContext.
			Expect(recSpan.Links).To(HaveLen(1))
			Expect(recSpan.Links[0].SpanContext.TraceID()).To(Equal(mutationTraceID))
			Expect(recSpan.Links[0].SpanContext.SpanID()).To(Equal(mutationSpanID))

			// Verify k8s.controller.* and k8s.resource.* semantic convention attributes.
			attrs := spanAttrMap(recSpan.Attributes)
			Expect(attrs).To(HaveKeyWithValue("k8s.controller.name", "deployment"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.group", "apps"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.version", "v1"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.kind", "Deployment"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.namespace", "default"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.name", "demo-deploy"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.uid", "uid-abc-123"))
			Expect(attrs).To(HaveKeyWithValue("k8s.resource.generation", int64(7)))
			Expect(attrs).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
			Expect(attrs).To(HaveKeyWithValue("k8s.controller.result", "success"))
			Expect(recSpan.Status.Code).To(Equal(codes.Ok))

			// Verify contextual logger decoration with trace_id and span_id.
			Expect(observedSpanCtx.IsValid()).To(BeTrue())
			Expect(observedLogrTraceID).To(Equal(recSpan.SpanContext.TraceID().String()))
			Expect(observedLogrSpanID).To(Equal(recSpan.SpanContext.SpanID().String()))
			Expect(observedKlogTraceID).To(Equal(recSpan.SpanContext.TraceID().String()))
			Expect(observedKlogSpanID).To(Equal(recSpan.SpanContext.SpanID().String()))
		})

		It("should distinguish LowPriority periodic_resync (and its requeues) from untraced/deleted mutations and superseding mutations", func(ctx SpecContext) {
			deployWithStaleTrace := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "stale-deploy",
					Namespace: "default",
					Annotations: map[string]string{
						TraceparentAnnotationKey: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					},
				},
			}
			deployWithoutTrace := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "plain-deploy",
					Namespace: "default",
				},
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(deployWithStaleTrace, deployWithoutTrace).
				Build()

			q := priorityqueue.New[reconcile.Request]("deployment")
			defer q.ShutDown()

			prototype := &appsv1.Deployment{}
			prototype.SetGroupVersionKind(deployGVK)

			var staleStep int
			c := New(Options[reconcile.Request]{
				Name:                    "deployment",
				MaxConcurrentReconciles: 1,
				TracerProvider:          tp,
				TraceMetadataExtractor:  NewCacheTraceMetadataExtractor(fakeClient, prototype, deployGVK, nil),
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.Discard()
				},
				Do: reconcile.Func(func(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
					if req.Name == "stale-deploy" {
						staleStep++
						switch staleStep {
						case 1, 2:
							// Requeue the periodic resync while keeping LowPriority on step 1, then test superseding priority on step 2.
							return reconcile.Result{RequeueAfter: 10 * time.Millisecond}, nil
						}
					}
					return reconcile.Result{}, nil
				}),
			})
			c.Queue = q

			staleReq := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "stale-deploy"}}

			// 1. LowPriority (initial list / unchanged-ResourceVersion resync) must emit periodic_resync with 0 Span Links.
			c.reconcileHandler(ctx, staleReq, handler.LowPriority)

			// 2. Requeue of that periodic_resync (still at LowPriority) must emit requeue with 0 Span Links.
			c.reconcileHandler(ctx, staleReq, handler.LowPriority)

			// 3. If a watch mutation arrives while queued (priority upgraded > LowPriority), it promotes to mutation and attaches the Span Link.
			c.reconcileHandler(ctx, staleReq, 0)

			// 4. Untraced mutation (priority == 0, no traceparent annotation) emits mutation with 0 Span Links.
			c.reconcileHandler(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "plain-deploy"},
			}, 0)

			// 5. Delete event (object no longer in cache, priority == 0) emits mutation with 0 Span Links and preserves GVK/Namespace/Name.
			c.reconcileHandler(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "deleted-deploy"},
			}, 0)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(5))

			// Span 0: periodic_resync with 0 links
			Expect(spans[0].Links).To(BeEmpty())
			Expect(spanAttrMap(spans[0].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypePeriodicResync))

			// Span 1: requeue of periodic_resync still has 0 links
			Expect(spans[1].Links).To(BeEmpty())
			Expect(spanAttrMap(spans[1].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeRequeue))

			// Span 2: superseding mutation during requeue attaches the Span Link
			Expect(spans[2].Links).To(HaveLen(1))
			Expect(spanAttrMap(spans[2].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))

			// Span 3: untraced mutation has trigger.type = mutation and 0 links
			Expect(spans[3].Links).To(BeEmpty())
			Expect(spanAttrMap(spans[3].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))

			// Span 4: deleted object (NotFound in cache) has trigger.type = mutation, 0 links, and GVK/Namespace/Name attributes
			Expect(spans[4].Links).To(BeEmpty())
			attrs4 := spanAttrMap(spans[4].Attributes)
			Expect(attrs4).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
			Expect(attrs4).To(HaveKeyWithValue("k8s.resource.kind", "Deployment"))
			Expect(attrs4).To(HaveKeyWithValue("k8s.resource.namespace", "default"))
			Expect(attrs4).To(HaveKeyWithValue("k8s.resource.name", "deleted-deploy"))
		})

		It("should record requeue_after, requeue, error, and terminal_error outcomes and classify follow-up reconciles as requeue", func(ctx SpecContext) {
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "retry-deploy",
					Namespace: "default",
					Annotations: map[string]string{
						TraceparentAnnotationKey: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					},
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deploy).Build()

			q := priorityqueue.New[reconcile.Request]("deployment")
			defer q.ShutDown()

			prototype := &appsv1.Deployment{}
			prototype.SetGroupVersionKind(deployGVK)

			var step int
			reconcileErr := errors.New("temporary backend failure")
			terminalErr := reconcile.TerminalError(errors.New("invalid configuration"))

			c := New(Options[reconcile.Request]{
				Name:                    "deployment",
				MaxConcurrentReconciles: 1,
				TracerProvider:          tp,
				TraceMetadataExtractor:  NewCacheTraceMetadataExtractor(fakeClient, prototype, deployGVK, nil),
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.Discard()
				},
				Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
					step++
					switch step {
					case 1:
						return reconcile.Result{RequeueAfter: 2500 * time.Millisecond}, nil
					case 2:
						return reconcile.Result{Requeue: true}, nil //nolint:staticcheck // testing deprecated Requeue
					case 3:
						return reconcile.Result{}, reconcileErr
					case 4:
						return reconcile.Result{}, terminalErr
					default:
						return reconcile.Result{}, nil
					}
				}),
			})
			c.Queue = q

			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "retry-deploy"}}

			// Step 1: initial mutation -> returns RequeueAfter(2.5s)
			c.reconcileHandler(ctx, req, 0)
			// Step 2: requeued from RequeueAfter -> returns Requeue: true
			c.reconcileHandler(ctx, req, 0)
			// Step 3: requeued from Requeue -> returns transient error
			c.reconcileHandler(ctx, req, 0)
			// Step 4: requeued from error -> returns TerminalError (not re-enqueued)
			c.reconcileHandler(ctx, req, 0)
			// Step 5: fresh mutation after terminal error -> returns success
			c.reconcileHandler(ctx, req, 0)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(5))

			// Span 0: mutation -> requeue_after (2.5s)
			attrs0 := spanAttrMap(spans[0].Attributes)
			Expect(attrs0).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
			Expect(attrs0).To(HaveKeyWithValue("k8s.controller.result", "requeue_after"))
			Expect(attrs0).To(HaveKeyWithValue("k8s.controller.requeue_after", 2.5))
			Expect(spans[0].Status.Code).To(Equal(codes.Ok))
			Expect(spans[0].Links).To(HaveLen(1))

			// Span 1: requeue -> requeue
			attrs1 := spanAttrMap(spans[1].Attributes)
			Expect(attrs1).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeRequeue))
			Expect(attrs1).To(HaveKeyWithValue("k8s.controller.result", "requeue"))
			Expect(spans[1].Status.Code).To(Equal(codes.Ok))
			Expect(spans[1].Links).To(HaveLen(1))

			// Span 2: requeue -> transient error
			attrs2 := spanAttrMap(spans[2].Attributes)
			Expect(attrs2).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeRequeue))
			Expect(attrs2).To(HaveKeyWithValue("k8s.controller.result", "error"))
			Expect(attrs2).NotTo(HaveKey("k8s.controller.terminal_error"))
			Expect(spans[2].Status.Code).To(Equal(codes.Error))
			Expect(spans[2].Status.Description).To(Equal(reconcileErr.Error()))
			Expect(spans[2].Events).NotTo(BeEmpty())

			// Span 3: requeue -> terminal error
			attrs3 := spanAttrMap(spans[3].Attributes)
			Expect(attrs3).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeRequeue))
			Expect(attrs3).To(HaveKeyWithValue("k8s.controller.result", "error"))
			Expect(attrs3).To(HaveKeyWithValue("k8s.controller.terminal_error", true))
			Expect(spans[3].Status.Code).To(Equal(codes.Error))

			// Span 4: fresh mutation (since terminal error did not requeue) -> success
			attrs4 := spanAttrMap(spans[4].Attributes)
			Expect(attrs4).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
			Expect(attrs4).To(HaveKeyWithValue("k8s.controller.result", "success"))
			Expect(spans[4].Status.Code).To(Equal(codes.Ok))
		})

		It("should record panic errors and end the reconciliation span when RecoverPanic is true or false, and fall back cleanly on extractor error", func(ctx SpecContext) {
			sink := newCapturingLogSink()
			q := priorityqueue.New[reconcile.Request]("custom-ctrl")
			defer q.ShutDown()

			extractorErr := errors.New("cache not synced")
			cRecover := New(Options[reconcile.Request]{
				Name:                    "custom-ctrl",
				MaxConcurrentReconciles: 1,
				RecoverPanic:            ptr.To(true),
				TracerProvider:          tp,
				TraceMetadataExtractor: func(context.Context, reconcile.Request) (ObjectTraceMetadata, error) {
					return ObjectTraceMetadata{}, extractorErr
				},
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.New(sink)
				},
				Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
					panic("boom-recovered")
				}),
			})
			cRecover.Queue = q

			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "item"}}
			cRecover.reconcileHandler(ctx, req, 0)

			// Verify extractor error was logged at V(5) and fallback span name ("Reconcile custom-ctrl") was used.
			Expect(sink.getAny("err")).To(Equal(extractorErr))
			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(1))
			Expect(spans[0].Name).To(Equal("Reconcile custom-ctrl"))
			Expect(spans[0].Status.Code).To(Equal(codes.Error))
			Expect(spanAttrMap(spans[0].Attributes)).To(HaveKeyWithValue("k8s.controller.result", "error"))
			Expect(spanAttrMap(spans[0].Attributes)).To(HaveKeyWithValue("k8s.resource.namespace", "default"))
			Expect(spanAttrMap(spans[0].Attributes)).To(HaveKeyWithValue("k8s.resource.name", "item"))

			// Now test RecoverPanic = false: reconcileHandler should record the error, end the span, and re-panic.
			cNoRecover := New(Options[reconcile.Request]{
				Name:                    "custom-ctrl",
				MaxConcurrentReconciles: 1,
				RecoverPanic:            ptr.To(false),
				TracerProvider:          tp,
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.Discard()
				},
				Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
					panic("boom-unrecovered")
				}),
			})
			cNoRecover.Queue = q

			Expect(func() {
				cNoRecover.reconcileHandler(ctx, req, 0)
			}).To(PanicWith("boom-unrecovered"))

			spans = exporter.GetSpans()
			Expect(spans).To(HaveLen(2))
			Expect(spans[1].Status.Code).To(Equal(codes.Error))
			Expect(spans[1].Status.Description).To(ContainSubstring("boom-unrecovered"))
			Expect(spanAttrMap(spans[1].Attributes)).To(HaveKeyWithValue("k8s.controller.result", "error"))
			// Unrecovered panic skips Queue.AddWithOpts, so req must not be left in requeuedKeys.
			_, exists := cNoRecover.requeuedKeys.Load(req)
			Expect(exists).To(BeFalse())
		})

		It("should detect superseding delete or generation updates during a pending requeue and auto-set GVK on PartialObjectMetadata prototype", func(ctx SpecContext) {
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "gen-deploy",
					Namespace:  "default",
					UID:        types.UID("uid-1"),
					Generation: 1,
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deploy).Build()
			q := priorityqueue.New[reconcile.Request]("deployment")
			defer q.ShutDown()

			// Pass PartialObjectMetadata without pre-calling SetGroupVersionKind to verify NewCacheTraceMetadataExtractor sets it.
			extractor := NewCacheTraceMetadataExtractor(fakeClient, &metav1.PartialObjectMetadata{}, deployGVK, nil)

			var step int
			c := New(Options[reconcile.Request]{
				Name:                    "deployment",
				MaxConcurrentReconciles: 1,
				TracerProvider:          tp,
				TraceMetadataExtractor:  extractor,
				LogConstructor: func(*reconcile.Request) logr.Logger {
					return logr.Discard()
				},
				Do: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
					step++
					if step <= 2 {
						return reconcile.Result{RequeueAfter: 10 * time.Millisecond}, nil
					}
					return reconcile.Result{}, nil
				}),
			})
			c.Queue = q

			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "gen-deploy"}}

			// Pass 1: initial mutation (UID=uid-1, Generation=1) -> RequeueAfter
			c.reconcileHandler(ctx, req, 0)

			// Simulate an untraced spec update bumping Generation to 2 while req is waiting in the queue.
			deploy.Generation = 2
			Expect(fakeClient.Update(ctx, deploy)).To(Succeed())

			// Pass 2: superseding generation bump -> classified as mutation -> RequeueAfter
			c.reconcileHandler(ctx, req, 0)

			// Simulate object deletion while req is waiting in the queue.
			Expect(fakeClient.Delete(ctx, deploy)).To(Succeed())

			// Pass 3: superseding delete (UID changed from uid-1 to "") -> classified as mutation
			c.reconcileHandler(ctx, req, 0)

			spans := exporter.GetSpans()
			Expect(spans).To(HaveLen(3))
			Expect(spanAttrMap(spans[0].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
			Expect(spanAttrMap(spans[1].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
			Expect(spanAttrMap(spans[1].Attributes)).To(HaveKeyWithValue("k8s.resource.generation", int64(2)))
			Expect(spanAttrMap(spans[2].Attributes)).To(HaveKeyWithValue("k8s.controller.trigger.type", TriggerTypeMutation))
		})
	})
})
