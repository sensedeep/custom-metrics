/*
    spans.ts - Custom spans
 */
import {client, table, CustomMetrics, SpanDef, log, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

export const Spans: SpanDef[] = [
    // {period: 1 * 60, samples: 12}, //  60, 1 mins, interval: 5 secs
    {period: 5 * 60, samples: 10}, //  300, 5 mins, interval: 30 secs
    {period: 60 * 60, samples: 12}, //  3600, 1 hr, interval: 5 mins
    {period: 3 * 60 * 60, samples: 12}, // 10800, 3 hrs, interval: 15 mins
    {period: 6 * 60 * 60, samples: 12}, // 21600, 6 hrs, interval: 30 mins
    {period: 24 * 60 * 60, samples: 12}, // 86400, 24 hrs, interval: 2 hrs
    {period: 7 * 24 * 60 * 60, samples: 14}, // 604,800, 7 days, interval: 1/2 day
    {period: 28 * 24 * 60 * 60, samples: 14}, // 2,419,200, 28 days, interval: 2 days
    {period: 3 * 28 * 24 * 60 * 60, samples: 12}, // 7,257,600, 1 quarter, interval: 1 week
    {period: 6 * 28 * 24 * 60 * 60, samples: 12}, // 14,515,200, 2 quarters, interval: 2 weeks
    {period: 365 * 24 * 60 * 60, samples: 12}, // 31,536,000, 1 year, interval: 1 month
]

test('Basic test harness', async () => {
    let metrics = new CustomMetrics({client, table, log: true, spans: Spans})
    let timestamp = Date.UTC(2000, 0, 1)
    let interval = 60 * 15

    for (let i = 0; i < 10; i++) {
        let metric = await metrics.emit('test/spans', 'MyMetric', 10, [], {timestamp})
        for (let span of metric.spans) {
            (span as any).ee = new Date(span.end * 1000)
        }
        expect(metric).toBeDefined()
        timestamp += interval * 1000
    }
})