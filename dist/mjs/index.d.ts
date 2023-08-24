import { DynamoDBClient, DynamoDBClientConfig } from '@aws-sdk/client-dynamodb';
type SpanDef = {
    period: number;
    samples: number;
};
export declare const DefaultSpans: SpanDef[];
export type Metric = {
    dimensions: string;
    expires?: number;
    id?: string;
    metric: string;
    namespace: string;
    owner?: string;
    version?: number;
    spans: Span[];
    seq?: number;
    _source?: string;
};
export type Point = {
    count: number;
    max?: number;
    min?: number;
    pvalues?: number[];
    sum: number;
    timestamp?: number;
};
export type Span = {
    end: number;
    period: number;
    samples: number;
    points: Point[];
};
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
    id?: string;
    metric: string;
    namespace: string;
    owner: string;
    period: number;
    points: MetricQueryPoint[];
    samples: number;
};
export type MetricOptions = {
    buffer?: MetricBufferOptions;
    client?: DynamoDBClient;
    consistent?: boolean;
    creds?: DynamoDBClientConfig;
    log?: true | 'verbose' | any;
    owner?: string;
    primaryKey?: string;
    sortKey?: string;
    prefix?: string;
    pResolution?: number;
    source?: string;
    spans?: SpanDef[];
    table?: string;
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
    owner?: string;
};
export type MetricQueryOptions = {
    accumulate?: boolean;
    id?: string;
    owner?: string;
    timestamp?: number;
};
type InstanceMap = {
    [key: string]: CustomMetrics;
};
export declare class CustomMetrics {
    private consistent;
    private buffer;
    private buffers;
    private client;
    private log;
    private options;
    private owner;
    private prefix;
    private primaryKey;
    private sortKey;
    private pResolution;
    private source;
    private spans;
    private table;
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
    getMetric(owner: string, namespace: string, metric: string, dimensions: string): Promise<Metric>;
    findMetrics(owner: string, namespace: string, metric: string | undefined, limit: number, startKey: object): Promise<{
        items: Metric[];
        next: object;
    }>;
    putMetric(item: Metric): Promise<import("@aws-sdk/client-dynamodb").PutItemCommandOutput>;
    mapItemFromDB(data: any): Metric;
    mapItemToDB(item: Metric): {
        [x: string]: string | number | {
            se: number;
            sp: number;
            ss: number;
            pt: {
                c: number;
                x: number;
                m: number;
                s: number;
                v: number[];
            }[];
        }[];
        expires: number;
        spans: {
            se: number;
            sp: number;
            ss: number;
            pt: {
                c: number;
                x: number;
                m: number;
                s: number;
                v: number[];
            }[];
        }[];
        seq: number;
        _source: string;
    };
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
