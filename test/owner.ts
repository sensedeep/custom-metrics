/*
    owner.ts - test owner scoping
 */
import {client, table, CustomMetrics} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Constructor no owner', async () => {
    let metrics = new CustomMetrics({client, table})
    expect(metrics).toBeDefined()
})

test('Constructor with different owners', async () => {
    let m1 = new CustomMetrics({client, table, owner: 'app1'})
    let m2 = new CustomMetrics({client, table, owner: 'service2'})

    //  These should not clash
    await m1.emit('test/owner', 'Launches', 5)
    await m2.emit('test/owner', 'Launches', 10)

    let r1 = await m1.query('test/owner', 'Launches', {}, 86400, 'sum')
    expect(r1).toBeDefined()
    expect(r1.owner).toBe('app1')
    expect(r1.points.length).toBe(r1.samples)
    expect(r1.points[11].value).toBe(5)

    let r2 = await m2.query('test/owner', 'Launches', {}, 86400, 'sum')
    expect(r2).toBeDefined()
    expect(r2.owner).toBe('service2')
    expect(r2.points.length).toBe(r1.samples)
    expect(r2.points[11].value).toBe(10)
})

test('Owner with namespaces', async () => {
    let metrics = new CustomMetrics({client, table})

    let metric = await metrics.emit('test/owner', 'FirstMetric', 10, [{}, {Rocket: 'SaturnV'}])
    expect(metric).toBeDefined()

    await metrics.emit('test/owner', 'SecondMetric', 10)
    await metrics.emit('test/owner/2', 'ThirdMetric', 10)

    let list = await metrics.getMetricList('test/owner')
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()

    list = await metrics.getMetricList('test/owner', 'FirstMetric')
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
})
