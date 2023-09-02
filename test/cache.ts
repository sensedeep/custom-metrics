/*
    cache.ts - CustomMetrics instance cache

    UNDOCUMENTED AND DEPRECATED
 */
import {client, table, CustomMetrics} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Alloc', async () => {
    let tags = {name: 'primary'}
    let metrics = CustomMetrics.allocInstance(tags, {client, table, log: true})
    expect(metrics).toBeDefined()
    expect(metrics instanceof CustomMetrics).toBe(true)
    expect(typeof metrics.emit == 'function').toBe(true)

    let inst = CustomMetrics.getInstance(tags)
    expect(inst).toEqual(metrics)

    let cache = CustomMetrics.getCache()
    expect(cache).toBeDefined()
    expect(Object.keys(cache).length).toBe(1)
    expect(cache[JSON.stringify(tags)]).toEqual(metrics)

    inst = CustomMetrics.allocInstance(tags)
    expect(inst).toEqual(metrics)
    cache = CustomMetrics.getCache()
    expect(Object.keys(cache).length).toBe(1)
    CustomMetrics.flushAll()

    CustomMetrics.freeInstance(tags)
    inst = CustomMetrics.getInstance(tags)
    expect(inst).toBeUndefined()
    expect(cache).toBeDefined()
    expect(Object.keys(cache).length).toBe(0)

    CustomMetrics.freeInstanceByKey('unknown')
})