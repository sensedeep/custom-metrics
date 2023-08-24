/*
    api.ts - Test misc api routines
 */
import {table, CustomMetrics} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test flush', async () => {
    let metrics = new CustomMetrics({table})
    await metrics.flush()
    await CustomMetrics.flushAll()
    await CustomMetrics.terminate()
})