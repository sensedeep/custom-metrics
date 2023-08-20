import process from 'process';
import { Schema, Version } from './schema';
import { Table } from 'dynamodb-onetable';
const Assert = true;
const Buffering = true;
const DefaultResolution = 0;
const MaxSeq = Number.MAX_SAFE_INTEGER;
const MaxRetries = 10;
const MetricListLimit = 10000;
export const DefaultSpans = [
    { period: 5 * 60, samples: 10 },
    { period: 60 * 60, samples: 12 },
    { period: 24 * 60 * 60, samples: 12 },
    { period: 7 * 24 * 60 * 60, samples: 14 },
    { period: 28 * 24 * 60 * 60, samples: 14 },
    { period: 365 * 24 * 60 * 60, samples: 12 },
];
var Instances = {};
process.on('SIGTERM', async () => {
    await CustomMetrics.terminate();
});
export class CustomMetrics {
    buffer;
    buffers = {};
    db;
    log;
    MetricModel;
    options;
    owner;
    pResolution;
    source;
    spans;
    timestamp;
    ttl;
    constructor(options = {}) {
        if (options.log == true) {
            this.log = { info: this.nop, error: this.error };
        }
        else if (options.log == 'verbose') {
            this.log = { info: this.info, error: this.error };
        }
        else if (options.log) {
            this.log = options.log;
        }
        else {
            this.log = { info: this.nop, error: this.nop };
        }
        if (options.ttl && typeof options.ttl != 'number') {
            throw new Error('Bad type for "ttl" option');
        }
        if (options.spans && (!Array.isArray(options.spans) || options.spans.length == 0)) {
            throw new Error('The "spans" option must be an non-empty array');
        }
        if (options.source && typeof options.source != 'string') {
            throw new Error('Non-string "source" option');
        }
        if (options.pResolution != undefined && (options.pResolution < 0 || options.pResolution > 1000)) {
            throw new Error('Invalid "pResolution" option. Must be between 0 and 1000. Default is 0');
        }
        if (options.prefix) {
            let model = Schema.models.Metric;
            model.pk.value = model.pk.value.replace('metric', options.prefix);
            model.sk.value = model.sk.value.replace('metric', options.prefix);
        }
        if (options.buffer) {
            if (typeof options.buffer != 'object') {
                throw new Error('Bad type for "buffer" option');
            }
            this.buffer = options.buffer;
        }
        if (options.onetable) {
            this.db = options.onetable;
            try {
                this.MetricModel = this.db.getModel('Metric');
            }
            catch (err) {
                this.db.addModel('Metric', Schema.models.Metric);
                this.MetricModel = this.db.getModel('Metric');
            }
        }
        else {
            if (!options.client) {
                throw new Error('Missing AWS V3 SDK DynamoDB client instance');
            }
            if (!options.tableName) {
                throw new Error('Missing DynamoDB tableName property');
            }
            let schema = Object.assign({}, Schema);
            schema.indexes.primary.hash = options.primaryKey || 'pk';
            schema.indexes.primary.sort = options.sortKey || 'sk';
            schema.params.typeField = options.typeField || '_type';
            this.db = new Table({
                client: options.client,
                hidden: false,
                name: options.tableName,
                partial: true,
                schema,
            });
            this.MetricModel = this.db.getModel('Metric');
        }
        this.options = options;
        this.owner = options.owner || 'account';
        this.spans = options.spans || DefaultSpans;
        this.ttl = options.ttl || this.spans[this.spans.length - 1].period;
        if (this.options.source) {
            this.source = this.options.source;
        }
        this.pResolution = options.pResolution || DefaultResolution;
    }
    async emit(namespace, metricName, value, dimensionsList = [{}], options = {}) {
        if (value == undefined || value == null) {
            throw new Error('Invalid metric value');
        }
        value = Number(value);
        if (isNaN(value)) {
            throw new Error(`Value to emit is not valid`);
        }
        if (!namespace || !metricName) {
            throw new Error('Missing emit namespace / metric argument');
        }
        if (dimensionsList.length == 0) {
            dimensionsList = [{}];
        }
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        let point;
        let buffer = options.buffer || this.buffer;
        if (buffer && Buffering) {
            return await this.bufferMetric(namespace, metricName, value, dimensionsList, options);
        }
        point = { count: 1, sum: value };
        return await this.emitDimensions(namespace, metricName, point, dimensionsList, options);
    }
    async emitDimensions(namespace, metricName, point, dimensionsList, options) {
        let result;
        for (let dimensions of dimensionsList) {
            let dimString = this.makeDimensionString(dimensions);
            result = await this.emitDimensionedMetric(namespace, metricName, point, dimString, options);
        }
        return result;
    }
    async emitDimensionedMetric(namespace, metricName, point, dimensions, options = {}) {
        let ttl = options.ttl != undefined ? options.ttl : this.ttl;
        let retries = MaxRetries;
        let metric;
        do {
            let owner = options.owner || this.owner;
            metric = await this.MetricModel.get({ owner, namespace, metric: metricName, dimensions: dimensions, version: Version }, { hidden: true });
            if (!metric) {
                this.log.info(`Initializing new metric`, { namespace, metricName, dimensions, owner });
                metric = this.initMetric(owner, namespace, metricName, dimensions);
            }
            if (point.timestamp) {
                let si = metric.spans.findIndex((s) => s.end - s.period <= point.timestamp && point.timestamp < s.end);
                if (si >= 0) {
                    this.addValue(metric, point.timestamp, point, si);
                }
                else {
                    this.log.error('Cannot determine span index', { point, metric });
                }
            }
            else {
                this.addValue(metric, point.timestamp, point, 0);
            }
            try {
                await this.updateMetric(metric, point, ttl);
                break;
            }
            catch (err) {
                if (err.code != 'ConditionalCheckFailedException') {
                    this.log.info(`Emit exception code ${err.code} message ${err.message}`, err);
                    throw err;
                }
            }
            if (retries == 0) {
                this.log.error(`Metric has too many retries`, { namespace, metricName, dimensions });
                break;
            }
            this.log.info(`Retry metric update`, { retries });
        } while (retries-- > 0);
        return metric;
    }
    async bufferMetric(namespace, metricName, value, dimensionsList, options) {
        let buffer = options.buffer || this.buffer;
        let interval = this.spans[0].period / this.spans[0].samples;
        let key = `${namespace}|${metricName}|${JSON.stringify(dimensionsList)}`;
        let elt = (this.buffers[key] = this.buffers[key] || {
            count: 0,
            sum: 0,
            timestamp: this.timestamp + (buffer.elapsed || interval),
            namespace: namespace,
            metric: metricName,
            dimensions: dimensionsList,
        });
        if (buffer.force ||
            (buffer.sum && elt.sum >= buffer.sum) ||
            (buffer.count && elt.count >= buffer.count) ||
            this.timestamp >= elt.timestamp) {
            this.log.info(`Emit buffered metric ${namespace}/${metricName} = ${value}, sum ${elt.sum} count ${elt.count} remaining ${elt.timestamp - this.timestamp}`);
            let point = { count: elt.count, sum: elt.sum, timestamp: elt.timestamp };
            await this.emitDimensions(namespace, metricName, point, dimensionsList, options);
        }
        elt.count++;
        elt.sum += value;
        this.log.info(`Buffer metric ${namespace}/${metricName} = ${value}, sum ${elt.sum} count ${elt.count}, remaining ${elt.timestamp - this.timestamp}`);
        CustomMetrics.saveInstance({ key }, this);
        return {
            spans: [{ points: [{ count: elt.count, sum: elt.sum }] }],
            metric: metricName,
            namespace: namespace,
            owner: options.owner || this.owner,
            version: Version,
        };
    }
    static async terminate() {
        await CustomMetrics.flushAll();
    }
    static async flushAll() {
        for (let [key, instance] of Object.entries(Instances)) {
            await instance.flush();
            CustomMetrics.freeInstanceByKey(key);
        }
        Instances = {};
    }
    async flush() {
        for (let elt of Object.values(this.buffers)) {
            let point = { count: elt.count, sum: elt.sum };
            for (let dimensions of elt.dimensions) {
                await this.emitDimensionedMetric(elt.namespace, elt.metric, point, this.makeDimensionString(dimensions));
            }
        }
        this.buffers = {};
    }
    async query(namespace, metricName, dimensions, period, statistic, options = {}) {
        this.timestamp = Math.floor((options.timestamp || Date.now()) / 1000);
        this.log.info(`Query metrics ${namespace}/${metricName}`, { dimensions });
        let owner = options.owner || this.owner;
        await this.flush();
        let dimString = this.makeDimensionString(dimensions);
        let metric = await this.MetricModel.get({
            owner,
            namespace,
            metric: metricName,
            dimensions: dimString,
            version: Version,
        });
        if (!metric) {
            return { dimensions, metric: metricName, namespace, period, points: [], owner };
        }
        let span = metric.spans.find((s) => period <= s.period);
        if (!span) {
            span = metric.spans.at(-1);
            period = span.period;
        }
        this.log.info(`Query ${namespace} ${metricName} ${dimString} ${period} ${statistic}`, {
            owner,
            metric,
            accumulate: options.accumulate,
            dimensions,
            period,
        });
        this.addValue(metric, this.timestamp, { count: 0, sum: 0 }, 0, period);
        let result;
        if (metric && span) {
            if (options.accumulate) {
                result = this.accumulateMetric(metric, span, statistic, owner);
            }
            else {
                result = this.calculateSeries(metric, span, statistic, owner);
            }
        }
        else {
            result = { dimensions, metric: metricName, namespace, period, points: [], owner };
        }
        this.log.info(`Metric query ${namespace}, ${metricName}, ${this.makeDimensionString(dimensions) || '[]'}, ` +
            `period ${period}, statistic "${statistic}"`, { result });
        return result;
    }
    accumulateMetric(metric, span, statistic, owner) {
        let value = 0, count = 0, pvalues = [];
        if (statistic == 'max') {
            value = Number.NEGATIVE_INFINITY;
        }
        else if (statistic == 'min') {
            value = Infinity;
        }
        else if (statistic == 'sum') {
            value = 0;
            count = 0;
        }
        else if (statistic == 'count') {
            value = 0;
            count = 0;
        }
        else if (statistic.match(/^p[0-9]+/)) {
            pvalues = [];
        }
        else {
            value = 0;
            count = 0;
        }
        let points = span.points;
        for (let i = 0; i < points.length; i++) {
            let point = points[i];
            if (statistic == 'max') {
                if (point.max != undefined) {
                    value = Math.max(value, point.max);
                }
            }
            else if (statistic == 'min') {
                if (point.min != undefined) {
                    value = Math.min(value, point.min);
                }
            }
            else if (statistic == 'sum') {
                value += point.sum;
            }
            else if (statistic == 'count') {
                value += point.count;
            }
            else if (statistic.match(/^p[0-9]+/)) {
                pvalues = pvalues.concat(point.pvalues);
            }
            else {
                value += point.sum;
            }
            count += point.count;
        }
        if (statistic.match(/^p[0-9]+/)) {
            let p = parseInt(statistic.slice(1));
            pvalues.sort((a, b) => a - b);
            let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1);
            value = pvalues[nth];
        }
        else if (statistic == 'avg') {
            value /= Math.max(count, 1);
        }
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            owner: owner,
            period: span.period,
            points: [{ value, timestamp: this.timestamp * 1000, count }],
        };
    }
    calculateSeries(metric, span, statistic, owner) {
        let points = [];
        let interval = span.period / span.samples;
        let timestamp = span.end - span.points.length * interval;
        let value;
        let i = 0;
        for (let point of span.points) {
            if (point.count > 0) {
                if (statistic == 'max') {
                    if (point.max != undefined) {
                        if (value == undefined) {
                            value = point.max;
                        }
                        else {
                            value = Math.max(value, point.max);
                        }
                    }
                }
                else if (statistic == 'min') {
                    if (point.min != undefined) {
                        if (value == undefined) {
                            value = point.min;
                        }
                        else {
                            value = Math.min(value, point.min);
                        }
                    }
                }
                else if (statistic == 'sum') {
                    value = point.sum;
                }
                else if (statistic == 'count') {
                    value = point.count;
                }
                else if (statistic.match(/^p[0-9]+/)) {
                    let p = parseInt(statistic.slice(1));
                    let pvalues = point.pvalues;
                    pvalues.sort((a, b) => a - b);
                    let nth = Math.min(Math.round((pvalues.length * p) / 100 + 1), pvalues.length - 1);
                    value = pvalues[nth];
                }
                else {
                    value = point.sum / point.count;
                }
            }
            timestamp += interval;
            timestamp = Math.min(timestamp, this.timestamp);
            points.push({ value, count: point.count, timestamp: timestamp * 1000 });
            i++;
        }
        return {
            dimensions: this.makeDimensionObject(metric.dimensions),
            metric: metric.metric,
            namespace: metric.namespace,
            period: span.period,
            points: points,
            owner: owner,
        };
    }
    makeDimensionString(dimensions) {
        let result = [];
        for (let [name, value] of Object.entries(dimensions)) {
            result.push(`${name}=${value}`);
        }
        return result.join(',');
    }
    makeDimensionObject(dimensions) {
        let result = {};
        for (let dimension of dimensions.split(',')) {
            if (dimension) {
                let [key, value] = dimension.split('=');
                result[key] = value;
            }
        }
        return result;
    }
    addValue(metric, timestamp = this.timestamp, point, si, queryPeriod = 0) {
        this.assert(metric);
        this.assert(timestamp);
        this.assert(0 <= si && si < metric.spans.length);
        let span = metric.spans[si];
        let interval = span.period / span.samples;
        let points = span.points;
        let start = span.end - points.length * interval;
        let aggregate = !queryPeriod || span.period < queryPeriod ? true : false;
        while (points.length > span.samples) {
            points.shift();
        }
        if (points.length) {
            if (timestamp < start) {
                this.log.error('Bad metric span', { metric, point, timestamp, start, span });
                return;
            }
            let shift = 0;
            if (queryPeriod && aggregate) {
                shift = points.length;
            }
            else if (point.count) {
                shift = Math.floor((timestamp - start) / interval) - span.samples + 1;
            }
            else if (queryPeriod) {
                shift = Math.floor((timestamp - start) / interval) - span.samples;
            }
            shift = Math.max(0, Math.min(shift, points.length));
            this.assert(0 <= shift && shift <= points.length);
            while (shift-- > 0) {
                let p = points.shift();
                if (aggregate && p.count && si + 1 < metric.spans.length) {
                    this.addValue(metric, start, p, si + 1, queryPeriod);
                }
                start += interval;
            }
        }
        if (aggregate && queryPeriod && si + 1 < metric.spans.length) {
            this.addValue(metric, timestamp, point, si + 1, queryPeriod);
        }
        else if (point.count) {
            if (points.length == 0) {
                start = span.end = this.getTimestamp(span, timestamp);
            }
            while (timestamp < start) {
                points.unshift({ count: 0, sum: 0 });
                start -= interval;
            }
            while (timestamp >= span.end) {
                points.push({ count: 0, sum: 0 });
                span.end += interval;
            }
            this.assert(points.length <= span.samples);
            let index = Math.floor((timestamp - start) / interval);
            this.assert(0 <= index && index < points.length);
            this.setPoint(span, index, point);
        }
    }
    setPoint(span, index, add) {
        let points = span.points;
        this.assert(0 <= index && index < points.length);
        let point = points.at(index);
        if (!point) {
            this.log.error(`Metric null point`, { span, index, add });
            return;
        }
        if (add.count) {
            let value = add.sum / add.count;
            if (point.min == undefined) {
                point.min = value;
            }
            else {
                point.min = Math.min(value, point.min);
            }
            if (point.max == undefined) {
                point.max = value;
            }
            else {
                point.max = Math.max(value, point.max);
            }
        }
        if (this.pResolution) {
            point.pvalues = point.pvalues || [];
            if (add.pvalues) {
                point.pvalues.push(...add.pvalues);
            }
            else {
                point.pvalues.push(add.sum / add.count);
            }
            point.pvalues.splice(0, point.pvalues.length - this.pResolution);
        }
        point.sum += add.sum;
        point.count += add.count;
    }
    async updateMetric(metric, point, ttl) {
        if (this.source) {
            metric._source = this.source;
        }
        if (ttl) {
            metric.expires = new Date((this.timestamp + ttl) * 1000);
        }
        let where = undefined;
        if (metric.seq != undefined) {
            let seq = (metric.seq = metric.seq || 0);
            if (metric.seq++ >= MaxSeq) {
                metric.seq = 0;
            }
            where = `\${seq} = {${seq}}`;
        }
        let stats = {};
        let result = await this.db.create('Metric', metric, {
            exists: null,
            timestamps: false,
            return: false,
            stats,
            where,
        });
    }
    async getMetricList(namespace = undefined, metric = undefined, options = { limit: MetricListLimit }) {
        let map = {};
        let next;
        let owner = options.owner || this.owner;
        do {
            options.next = next;
            options.log = this.options.log == 'verbose' ? true : false;
            let list = await this.db.find('Metric', { owner, namespace, version: Version }, options);
            if (list.length) {
                for (let item of list) {
                    let ns = (map[item.namespace] = map[item.namespace] || {});
                    let met = (ns[item.metric] = ns[item.metric] || []);
                    met.push(item.dimensions);
                }
            }
            next = list.next;
        } while (next);
        let result = { namespaces: Object.keys(map) };
        if (namespace && map[namespace]) {
            result.metrics = Object.keys(map[namespace]);
            if (metric) {
                let dimensions = map[namespace][metric];
                if (dimensions) {
                    result.dimensions = [];
                    dimensions = dimensions.sort().filter((v, index, self) => self.indexOf(v) === index);
                    for (let dimension of dimensions) {
                        result.dimensions.push(this.makeDimensionObject(dimension));
                    }
                }
            }
        }
        return result;
    }
    initMetric(owner, namespace, name, dimensions) {
        let metric = {
            dimensions,
            metric: name,
            namespace,
            owner,
            spans: [],
            version: Version,
        };
        for (let sdef of this.spans) {
            let span = {
                samples: sdef.samples,
                period: sdef.period,
                end: this.timestamp,
                points: [],
            };
            span.end = this.getTimestamp(span);
            metric.spans.push(span);
        }
        return metric;
    }
    static allocInstance(tags, options = {}) {
        let key = JSON.stringify(tags);
        let metrics = Instances[key];
        if (!metrics) {
            metrics = Instances[key] = new CustomMetrics(options);
        }
        return metrics;
    }
    static freeInstance(tags) {
        let key = JSON.stringify(tags);
        delete Instances[key];
    }
    static freeInstanceByKey(key) {
        delete Instances[key];
    }
    static getInstance(tags) {
        let key = JSON.stringify(tags);
        return Instances[key];
    }
    static saveInstance(tags, metrics) {
        let key = JSON.stringify(tags);
        Instances[key] = metrics;
    }
    static getCache() {
        return Instances;
    }
    getTimestamp(span, timestamp = this.timestamp) {
        let interval = span.period / span.samples;
        return Math.ceil(timestamp / interval) * interval;
    }
    assert(c) {
        if (!c && Assert) {
            let msg = { stack: '' };
            if (typeof Error.captureStackTrace === 'function') {
                Error.captureStackTrace(msg);
            }
            else {
                msg.stack = new Error('Assert').stack;
            }
            this.log.error(`Assertion failed ${msg.stack}`);
        }
    }
    info(message, context = {}) {
        console.log('INFO: ' + message, context);
    }
    error(message, context = {}) {
        console.log('ERROR: ' + message, context);
    }
    nop() { }
}
