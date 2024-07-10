/*
    upgrade.ts - Upgrade spans
 */
import {client, table, CustomMetrics, SpanDef, log, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const LessSpans: SpanDef[] = [
    {period: 24 * 60 * 60, samples: 12}, // 86400, 24 hrs, interval: 2 hrs
    {period: 365 * 24 * 60 * 60, samples: 12}, // 31,536,000, 1 year, interval: 1 month
]

const MoreSpans: SpanDef[] = [
    {period: 1 * 60, samples: 12}, //  60, 1 min, interval: 5 secs
    {period: 5 * 60, samples: 10}, //  300, 5 mins, interval: 30 secs
    {period: 60 * 60, samples: 12}, //  3600, 1 hr, interval: 5 mins
    {period: 3 * 60 * 60, samples: 12}, //  10,800, 3 hrs, interval: 15 mins
    {period: 6 * 60 * 60, samples: 12}, //  21600, 6 hr, interval: 30 mins
    {period: 24 * 60 * 60, samples: 12}, // 86400, 24 hrs, interval: 2 hrs
    {period: 7 * 24 * 60 * 60, samples: 14}, // 604,800, 7 days, interval: 1/2 day
    {period: 28 * 24 * 60 * 60, samples: 14}, // 2,419,200, 28 days, interval: 2 days
    {period: 365 * 24 * 60 * 60, samples: 12}, // 31,536,000, 1 year, interval: 1 month
]

test('Upgrade Spans', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()

    let metric
    for (let i = 0; i < 140; i++) {
        metric = await metrics.emit('test/upgrade', 'UpMetric', 7, [], {timestamp})
        timestamp += 30 * 1000
    }
    expect(metric).toBeDefined()
    expect(metric.spans.length).toBe(6)
    expect(metric.spans[0].points.length).toBe(10)
    expect(metric.spans[1].points.length).toBe(12)
    expect(metric.spans[2].points.length).toBe(1)
    expect(metric.spans[3].points.length).toBe(0)

    //  Test upgrade
    metrics = new CustomMetrics({client, table, log: true, spans: MoreSpans})
    metric = await metrics.upgrade('test/upgrade', 'UpMetric', [])
    expect(metric).toBeDefined()
    expect(metric.spans.length).toBe(9)
    expect(metric.spans[0].points.length).toBe(0)
    expect(metric.spans[1].points.length).toBe(10)
    expect(metric.spans[2].points.length).toBe(0)
    expect(metric.spans[3].points.length).toBe(9)

    //  Test downgrade
    metrics = new CustomMetrics({client, table, log: true, spans: LessSpans})
    metric = await metrics.emit('test/upgrade', 'UpMetric', 7, [], {upgrade: true})
    expect(metric).toBeDefined()
    expect(metric.spans.length).toBe(2)
    expect(metric.spans[0].points.length).toBe(1)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(7)
    expect(metric.spans[1].points.length).toBe(1)
    expect(metric.spans[1].points[0].sum).toBe(980)
    expect(metric.spans[1].points[0].count).toBe(140)

    //  Test already upgraded
    metrics = new CustomMetrics({client, table, log: true, spans: LessSpans})
    metric = await metrics.emit('test/upgrade', 'UpMetric', 7, [], {upgrade: true})
    expect(metric).toBeDefined()
})