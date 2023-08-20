import { Schema } from './schema';
import { Entity, Table } from 'dynamodb-onetable';
type SpanDef = {
    period: number;
    samples: number;
};
export declare const DefaultSpans: SpanDef[];
export type MetricDimensions = {
    [key: string]: unknown;
};
export type MetricDimensionsList = MetricDimensions[];
export type MetricList = {
    namespaces: string[];
    metrics?: string[];
    dimensions?: MetricDimensions[];
};
export type MetricQueryPoint = {
    count: number;
    max?: number;
    min?: number;
    pvalues?: number[];
    sum?: number;
    timestamp?: number;
    value?: number;
};
export type MetricQueryResult = {
    dimensions: MetricDimensions;
    metric: string;
    namespace: string;
    owner: string;
    period: number;
    points: MetricQueryPoint[];
};
export type MetricOptions = {
    buffer?: MetricBufferOptions;
    client?: object;
    log?: true | 'verbose' | any;
    onetable?: Table;
    owner?: string;
    primaryKey?: string;
    sortKey?: string;
    prefix?: string;
    pResolution?: number;
    source?: string;
    spans?: SpanDef[];
    tableName?: string;
    typeField?: string;
    ttl?: number;
};
export type MetricBufferOptions = {
    sum?: number;
    count?: number;
    elapsed?: number;
    force?: boolean;
};
export type MetricEmitOptions = {
    buffer?: MetricBufferOptions;
    owner?: string;
    timestamp?: number;
    ttl?: number;
};
export type MetricListOptions = {
    log?: boolean;
    limit?: number;
    next?: object;
    owner?: string;
};
export type MetricQueryOptions = {
    accumulate?: boolean;
    owner?: string;
    timestamp?: number;
};
export type Span = Entity<typeof Schema.models.Metric.spans.items.schema>;
export type Point = Entity<typeof Schema.models.Metric.spans.items.schema.points.items.schema>;
export type Metric = Entity<typeof Schema.models.Metric>;
type InstanceMap = {
    [key: string]: CustomMetrics;
};
export declare class CustomMetrics {
    private buffer;
    private buffers;
    private db;
    private log;
    private MetricModel;
    private options;
    private owner;
    private pResolution;
    private source;
    private spans;
    private timestamp;
    private ttl;
    constructor(options?: MetricOptions);
    emit(namespace: string, metricName: string, value: number, dimensionsList?: MetricDimensionsList, options?: MetricEmitOptions): Promise<Metric>;
    private emitDimensions;
    private emitDimensionedMetric;
    bufferMetric(namespace: string, metricName: string, value: number, dimensionsList: MetricDimensionsList, options: MetricEmitOptions): Promise<Metric>;
    static terminate(): Promise<void>;
    static flushAll(): Promise<void>;
    flush(): Promise<void>;
    query(namespace: string, metricName: string, dimensions: MetricDimensions, period: number, statistic: string, options?: MetricQueryOptions): Promise<MetricQueryResult>;
    private accumulateMetric;
    private calculateSeries;
    private makeDimensionString;
    private makeDimensionObject;
    private addValue;
    private setPoint;
    private updateMetric;
    getMetricList(namespace?: string, metric?: string, options?: MetricListOptions): Promise<MetricList>;
    private initMetric;
    static allocInstance(tags: object, options?: MetricOptions): CustomMetrics;
    static freeInstance(tags: object): void;
    static freeInstanceByKey(key: string): void;
    static getInstance(tags: object): CustomMetrics;
    static saveInstance(tags: object, metrics: CustomMetrics): void;
    static getCache(): InstanceMap;
    private getTimestamp;
    private assert;
    private info;
    private error;
    private nop;
}
export {};
