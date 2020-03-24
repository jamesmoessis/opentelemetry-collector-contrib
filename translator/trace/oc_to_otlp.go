// Copyright 2019 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracetranslator

import (
	"strings"

	octrace "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	otlpcommon "github.com/open-telemetry/opentelemetry-proto/gen/go/common/v1"
	otlptrace "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"

	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/internal"
	translatorcommon "github.com/open-telemetry/opentelemetry-collector/translator/common"
	"github.com/open-telemetry/opentelemetry-collector/translator/conventions"
)

func OCToOTLP(td consumerdata.TraceData) []*otlptrace.ResourceSpans {

	if td.Node == nil && td.Resource == nil && len(td.Spans) == 0 {
		return nil
	}

	resource := translatorcommon.OCNodeResourceToOtlp(td.Node, td.Resource)

	resourceSpans := &otlptrace.ResourceSpans{
		Resource: resource,
	}
	resourceSpanList := []*otlptrace.ResourceSpans{resourceSpans}

	if len(td.Spans) != 0 {
		ils := &otlptrace.InstrumentationLibrarySpans{}
		resourceSpans.InstrumentationLibrarySpans = []*otlptrace.InstrumentationLibrarySpans{ils}

		ils.Spans = make([]*otlptrace.Span, 0, len(td.Spans))

		for _, ocSpan := range td.Spans {
			if ocSpan == nil {
				// Skip nil spans.
				continue
			}

			otlpSpan := ocSpanToOtlp(ocSpan)

			if ocSpan.Resource != nil {
				// Add a separate ResourceSpans item just for this span since it
				// has a different Resource.
				separateRS := &otlptrace.ResourceSpans{
					Resource: translatorcommon.OCNodeResourceToOtlp(td.Node, ocSpan.Resource),
					InstrumentationLibrarySpans: []*otlptrace.InstrumentationLibrarySpans{
						{
							Spans: []*otlptrace.Span{otlpSpan},
						},
					},
				}
				resourceSpanList = append(resourceSpanList, separateRS)
			} else {
				// Otherwise add the span to the first ResourceSpans item.
				ils.Spans = append(ils.Spans, otlpSpan)
			}
		}
	}

	return resourceSpanList
}

func timestampToUnixNano(ts *timestamp.Timestamp) uint64 {
	return uint64(internal.TimestampToUnixNano(ts))
}

func ocSpanToOtlp(ocSpan *octrace.Span) *otlptrace.Span {
	attrs, droppedAttrCount := ocAttrsToOtlp(ocSpan.Attributes)
	events, droppedEventCount := ocEventsToOtlp(ocSpan.TimeEvents)
	links, droppedLinkCount := ocLinksToOtlp(ocSpan.Links)

	childSpanCount := int32(0)
	if ocSpan.ChildSpanCount != nil {
		childSpanCount = int32(ocSpan.ChildSpanCount.Value)
	}
	_ = childSpanCount // TODO(nilebox): Handle once OTLP supports it

	otlpSpan := &otlptrace.Span{
		TraceId:                ocSpan.TraceId,
		SpanId:                 ocSpan.SpanId,
		TraceState:             ocTraceStateToOtlp(ocSpan.Tracestate),
		ParentSpanId:           ocSpan.ParentSpanId,
		Name:                   truncableStringToStr(ocSpan.Name),
		Kind:                   ocSpanKindToOtlp(ocSpan.Kind, ocSpan.Attributes),
		StartTimeUnixNano:      timestampToUnixNano(ocSpan.StartTime),
		EndTimeUnixNano:        timestampToUnixNano(ocSpan.EndTime),
		Attributes:             attrs,
		DroppedAttributesCount: droppedAttrCount,
		Events:                 events,
		DroppedEventsCount:     droppedEventCount,
		Links:                  links,
		DroppedLinksCount:      droppedLinkCount,
		Status:                 ocStatusToOtlp(ocSpan.Status),
	}

	return otlpSpan
}

func ocStatusToOtlp(ocStatus *octrace.Status) *otlptrace.Status {
	if ocStatus == nil {
		return nil
	}
	return &otlptrace.Status{
		Code:    otlptrace.Status_StatusCode(ocStatus.Code),
		Message: ocStatus.Message,
	}
}

// Convert tracestate to W3C format. See the https://w3c.github.io/trace-context/
func ocTraceStateToOtlp(ocTracestate *octrace.Span_Tracestate) string {
	if ocTracestate == nil {
		return ""
	}
	var sb strings.Builder
	for i, entry := range ocTracestate.Entries {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strings.Join([]string{entry.Key, entry.Value}, "="))
	}
	return sb.String()
}

func ocAttrsToOtlp(ocAttrs *octrace.Span_Attributes) (otlpAttrs []*otlpcommon.AttributeKeyValue, droppedCount uint32) {
	if ocAttrs == nil {
		return
	}

	otlpAttrs = make([]*otlpcommon.AttributeKeyValue, 0, len(ocAttrs.AttributeMap))
	for key, ocAttr := range ocAttrs.AttributeMap {

		otlpAttr := &otlpcommon.AttributeKeyValue{Key: key}

		switch attribValue := ocAttr.Value.(type) {
		case *octrace.AttributeValue_StringValue:
			otlpAttr.StringValue = truncableStringToStr(attribValue.StringValue)
			otlpAttr.Type = otlpcommon.AttributeKeyValue_STRING

		case *octrace.AttributeValue_IntValue:
			otlpAttr.IntValue = attribValue.IntValue
			otlpAttr.Type = otlpcommon.AttributeKeyValue_INT

		case *octrace.AttributeValue_BoolValue:
			otlpAttr.BoolValue = attribValue.BoolValue
			otlpAttr.Type = otlpcommon.AttributeKeyValue_BOOL

		case *octrace.AttributeValue_DoubleValue:
			otlpAttr.DoubleValue = attribValue.DoubleValue
			otlpAttr.Type = otlpcommon.AttributeKeyValue_DOUBLE

		default:
			str := "<Unknown OpenCensus Attribute>"
			otlpAttr.StringValue = str
			otlpAttr.Type = otlpcommon.AttributeKeyValue_STRING
		}
		otlpAttrs = append(otlpAttrs, otlpAttr)
	}
	droppedCount = uint32(ocAttrs.DroppedAttributesCount)
	return
}

func ocSpanKindToOtlp(ocKind octrace.Span_SpanKind, ocAttrs *octrace.Span_Attributes) otlptrace.Span_SpanKind {
	switch ocKind {
	case octrace.Span_SERVER:
		return otlptrace.Span_SERVER

	case octrace.Span_CLIENT:
		return otlptrace.Span_CLIENT

	case octrace.Span_SPAN_KIND_UNSPECIFIED:
		// Span kind field is unspecified, check if TagSpanKind attribute is set.
		// This can happen if span kind had no equivalent in OC, so we could represent it in
		// the SpanKind. In that case the span kind may be a special attribute TagSpanKind.
		if ocAttrs != nil {
			kindAttr := ocAttrs.AttributeMap[TagSpanKind]
			if kindAttr != nil {
				strVal, ok := kindAttr.Value.(*octrace.AttributeValue_StringValue)
				if ok && strVal != nil {
					var otlpKind otlptrace.Span_SpanKind
					switch OpenTracingSpanKind(truncableStringToStr(strVal.StringValue)) {
					case OpenTracingSpanKindConsumer:
						otlpKind = otlptrace.Span_CONSUMER
					case OpenTracingSpanKindProducer:
						otlpKind = otlptrace.Span_PRODUCER
					default:
						return otlptrace.Span_SPAN_KIND_UNSPECIFIED
					}
					delete(ocAttrs.AttributeMap, TagSpanKind)
					return otlpKind
				}
			}
		}
		return otlptrace.Span_SPAN_KIND_UNSPECIFIED

	default:
		return otlptrace.Span_SPAN_KIND_UNSPECIFIED
	}
}

func ocEventsToOtlp(ocEvents *octrace.Span_TimeEvents) (otlpEvents []*otlptrace.Span_Event, droppedCount uint32) {
	if ocEvents == nil {
		return
	}

	droppedCount = uint32(ocEvents.DroppedMessageEventsCount + ocEvents.DroppedAnnotationsCount)
	if len(ocEvents.TimeEvent) == 0 {
		return
	}

	otlpEvents = make([]*otlptrace.Span_Event, 0, len(ocEvents.TimeEvent))

	for _, ocEvent := range ocEvents.TimeEvent {
		if ocEvent == nil {
			continue
		}

		otlpEvent := &otlptrace.Span_Event{
			TimeUnixNano: timestampToUnixNano(ocEvent.Time),
		}

		switch teValue := ocEvent.Value.(type) {
		case *octrace.Span_TimeEvent_Annotation_:
			if teValue.Annotation != nil {
				otlpEvent.Name = truncableStringToStr(teValue.Annotation.Description)
				attrs, droppedCount := ocAttrsToOtlp(teValue.Annotation.Attributes)
				otlpEvent.Attributes = attrs
				otlpEvent.DroppedAttributesCount = droppedCount
			}

		case *octrace.Span_TimeEvent_MessageEvent_:
			otlpEvent.Attributes = ocMessageEventToOtlpAttrs(teValue.MessageEvent)

		default:
			otlpEvent.Name = "An unknown OpenCensus TimeEvent type was detected when translating to OTLP"
		}

		otlpEvents = append(otlpEvents, otlpEvent)
	}
	return
}

func ocLinksToOtlp(ocLinks *octrace.Span_Links) (otlpLinks []*otlptrace.Span_Link, droppedCount uint32) {
	if ocLinks == nil {
		return nil, 0
	}

	droppedCount = uint32(ocLinks.DroppedLinksCount)
	if len(ocLinks.Link) == 0 {
		return nil, droppedCount
	}

	otlpLinks = make([]*otlptrace.Span_Link, 0, len(ocLinks.Link))

	for _, ocLink := range ocLinks.Link {
		if ocLink == nil {
			continue
		}

		attrs, droppedCount := ocAttrsToOtlp(ocLink.Attributes)
		otlpLink := &otlptrace.Span_Link{
			TraceId:                ocLink.TraceId,
			SpanId:                 ocLink.SpanId,
			TraceState:             ocTraceStateToOtlp(ocLink.Tracestate),
			Attributes:             attrs,
			DroppedAttributesCount: droppedCount,
		}

		otlpLinks = append(otlpLinks, otlpLink)
	}
	return otlpLinks, droppedCount
}

func ocMessageEventToOtlpAttrs(msgEvent *octrace.Span_TimeEvent_MessageEvent) []*otlpcommon.AttributeKeyValue {
	if msgEvent == nil {
		return nil
	}

	return []*otlpcommon.AttributeKeyValue{
		{
			Key:         conventions.OCTimeEventMessageEventType,
			StringValue: msgEvent.Type.String(),
			Type:        otlpcommon.AttributeKeyValue_STRING,
		},
		{
			Key:      conventions.OCTimeEventMessageEventID,
			IntValue: int64(msgEvent.Id),
			Type:     otlpcommon.AttributeKeyValue_INT,
		},
		{
			Key:      conventions.OCTimeEventMessageEventUSize,
			IntValue: int64(msgEvent.UncompressedSize),
			Type:     otlpcommon.AttributeKeyValue_INT,
		},
		{
			Key:      conventions.OCTimeEventMessageEventCSize,
			IntValue: int64(msgEvent.CompressedSize),
			Type:     otlpcommon.AttributeKeyValue_INT,
		},
	}
}

func truncableStringToStr(ts *octrace.TruncatableString) string {
	if ts == nil {
		return ""
	}
	return ts.Value
}
