'use strict';

const { once } = require('events');
const Fs = require('fs');
const Http = require('http');
const Https = require('https');
const Path = require('path');
const { Stream } = require('stream');
const Url = require('url');
const { promisify } = require('util');
const debug = require('debug')('hls:uploader');

const AgentKeepalive = require('agentkeepalive');
const Boom = require('@hapi/boom');
const Hoek = require('@hapi/hoek');
const Minio = require('minio');
const Pati = require('pati');
const Uuid = require('uuid');
const WriteFileAtomic = require('write-file-atomic');

const { streamToBuffer } = require('./utils');


const internals = {
    pipeline: promisify(Stream.pipeline),

    boomify(obj, fn) {

        if (fn) {
            fn = obj[fn];
        }
        else {
            fn = obj;
            obj = null;
        }

        return async function (...args) {

            try {
                await fn.call(obj, ...args);
            }
            catch (err) {
                throw Boom.boomify(err);
            }
        };
    }
};


class HlsUploader {

    constructor(targetUri, options) {

        const url = new URL(targetUri);

        Hoek.assert(url.pathname.endsWith('/'), 'Url must end with a "/"');
        Hoek.assert(!url.hash, 'Url must not contain a hash part');

        options = Object.assign({}, options, {
            indexName: options.indexName || 'index.m3u8'
        });

        if (url.protocol === 's3:') {
            Hoek.assert(!!options.collect === false, 'Collect not supported with s3:');
            this.impl = new S3UploaderImpl(url, options);
        }
        else if (url.protocol === 'file:') {
            this.impl = new FileUploaderImpl(url, options);
        }

        if (!this.impl) {
            throw new Error('Unsupported protocol: ' + url.protocol);
        }

        this.pushing = new Set();
        this.prepared = undefined;
    }

    async pushSegment(streamOrBuffer, name, meta) {

        await this.prepared;

        const promise = this.impl.pushSegment(streamOrBuffer, name, meta);
        try {
            this.pushing.add(promise);
            return await promise;
        }
        finally {
            this.pushing.delete(promise);
        }
    }

    /**
     * @param {import('m3u8parse/lib/m3u8parse').M3U8Playlist} m3u8
     */
    async flushIndex(m3u8, { cacheDuration } = {}) {

        const { ended, target_duration } = m3u8;
        const index = m3u8.toString().trim();

        await this.prepared;

        try {
            await Promise.all([...this.pushing]);
        }
        catch {}

        return this.impl.flushIndex(index, { cacheDuration, ended, target_duration });
    }

    /* async */ prepare(exclusive, test) {

        this.prepared = this.impl.prepare(exclusive, test);
        return this.prepared;
    }

    get segmentBytes() {

        return this.impl.segmentBytes;
    }
}


class FileUploaderImpl {

    constructor(url, options) {

        this.collect = !!options.collect;
        this.indexName = options.indexName;
        this.targetPath = url;

        this.lastIndexString = '';
        this.segmentBytes = 0;
    }

    async prepare(exclusive) {

        try {
            await Fs.promises.mkdir(this.targetPath, { recursive: !exclusive });
        }
        catch (err) {
            if (exclusive || err.code !== 'EEXIST') {
                throw err;
            }
        }
    }

    async pushSegment(streamOrBuffer, name, meta) {

        const append = this.collect && this.segmentBytes !== 0;

        const url = new URL(name, this.targetPath);
        const target = Fs.createWriteStream(url, { flags: append ? 'a' : 'w' });

        try {
            if (Buffer.isBuffer(streamOrBuffer)) {
                if (meta.size >= 0 && streamOrBuffer.byteLength !== meta.size) {
                    throw Boom.badRequest('Size must match');
                }

                target.end(streamOrBuffer);
                await once(target, 'finish');

                return { url: url.href, bytesWritten: streamOrBuffer.byteLength };
            }

            const dispatcher = new Pati.EventDispatcher(streamOrBuffer);
            streamOrBuffer.pipe(target);

            dispatcher.on('end', Pati.EventDispatcher.end);

            let bytesWritten = 0;
            // eslint-disable-next-line no-return-assign
            dispatcher.on('data', (chunk) => bytesWritten += +chunk.length);

            try {
                // TODO: handle target errors & wait for end?
                await internals.boomify(dispatcher, 'finish')();

                if (meta.size >= 0 && bytesWritten !== meta.size) {
                    throw Boom.badRequest('Size must match');
                }

                return { url: url.href, bytesWritten };
            }
            finally {
                this.segmentBytes += bytesWritten;
            }
        }
        finally {
            if (!target.writableEnded) {
                target.end();
            }
        }
    }

    async flushIndex(indexString) {

        let appendString;
        if (this.lastIndexString && indexString.startsWith(this.lastIndexString)) {
            const lastLength = this.lastIndexString.length;
            appendString = indexString.substr(lastLength);
        }

        this.lastIndexString = indexString;

        const url = new URL(this.indexName, this.targetPath);

        if (appendString) {
            await internals.boomify(Fs.promises.appendFile)(url, appendString);
        }
        else {
            await internals.boomify(WriteFileAtomic)(Url.fileURLToPath(url), indexString);
        }

        return url.href;
    }
}


internals.createTransport = function (useSSL = false) {

    const http = useSSL ? Https : Http;
    const agentClass = useSSL ? AgentKeepalive.HttpsAgent : AgentKeepalive;

    const agent = new agentClass({
        maxSockets: 6,
        maxFreeSockets: 6,
        timeout: 30000,
        freeSocketTimeout: 40000
    });

    return {
        request(options, next) {

            options.agent = agent;

            const req = http.request(options, next);

            const timeout = setTimeout(() => {

                if (!req.destroyed) {
                    req.destroy(new Error('timeout'));
                }
            }, 2 * 60 * 1000);

            req.on('close', () => {

                clearTimeout(timeout);
            });

            return req;
        }
    };
};


internals.transport = {
    http: internals.createTransport(false),
    https: internals.createTransport(true)
};


internals.s3bucketmeta = new Map();


class S3UploaderImpl {

    static async credentials() {

        if (!S3UploaderImpl._credentials) {
            const { defaultProvider } = await import('@aws-sdk/credential-provider-node');
            S3UploaderImpl._credentials = await defaultProvider()();
        }

        return S3UploaderImpl._credentials;
    }

    static bucketMeta(bucket) {

        let meta = internals.s3bucketmeta.get(bucket);
        if (!meta) {
            meta = {
                ACL: 'public-read'
            };

            internals.s3bucketmeta.set(bucket, meta);
        }

        return meta;
    }

    /**
     * @param {URL} url
     * @param {*} options
     */
    constructor(url, options) {

        this.indexName = options.indexName;
        this.defaultCacheDuration = +options.cacheDuration || 7 * 24 * 3600 * 1000;

        this.Bucket = url.hostname;

        this.meta = S3UploaderImpl.bucketMeta(this.Bucket);
        this.baseKey = (url.pathname || '/').slice(1);

        /** @type { Minio.ClientOptions } */
        this.config = {
            endPoint: 's3.amazonaws.com',
            region: url.searchParams.get('region'),
            useSSL: true
        };

        const endpoint = url.searchParams.get('endpoint');
        if (endpoint) {
            const endpointUrl = new URL(endpoint);
            endpointUrl.pathname = `/${this.Bucket}/`;

            this.config.endPoint = endpointUrl.host;
            this.config.useSSL = (endpointUrl.protocol === 'https:');
        }

        // Use custom transport with keep-alive and timeout

        this.config.transport = internals.transport[this.config.useSSL ? 'https' : 'http'];

        /** @type { Minio.Client } */
        this.s3 = undefined;
    }

    async prepare(exclusive, test) {

        const creds = await S3UploaderImpl.credentials();

        this.config.accessKey = creds.accessKeyId;
        this.config.secretKey = creds.secretAccessKey;
        this.config.sessionToken = creds.sessionToken;

        this.s3 = new Minio.Client(this.config);

        if (!exclusive) {
            if (test) {
                await this.putObject(this.baseKey, Buffer.alloc(0), null, 0);
            }

            return;
        }

        const uuid = Uuid.v4();

        console.log('obtaining lock:', uuid);

        for (let i = 2; i >= 0; --i) {
            try {
                const failReason = await this.obtainLock(uuid);
                if (!failReason) {
                    return;
                }

                i = 0;
                throw new Error(failReason);
            }
            catch (err) {
                if (i === 0) {
                    throw err;
                }
            }

            await Hoek.wait(500);
        }
    }

    async getLockState(uuid) {

        // States: 'locked', 'open', 'conflict:<id>'
        // throws on endpoint errors

        try {
            const stream = await this.s3.getObject(this.Bucket, this.baseKey);
            const body = await streamToBuffer(stream);

            if (body.toString() !== uuid) {
                return `conflict:${body.toString()}`;
            }
        }
        catch (err) {
            if (err.code === 'NotFound' || err.code === 'NoSuchKey') {
                return 'open';
            }

            throw err;
        }

        return 'locked';
    }

    async obtainLock(uuid) {

        // Note: This lock could fail if it takes longer for a check + write than the wait period

        // Check

        const checkState = await this.getLockState(uuid);
        if (checkState !== 'open') {
            return checkState === 'locked' ? false : `lock object exists: ${checkState}`;
        }

        // Lock probably does not exist - Create with UUID

        try {
            await this.putObject(this.baseKey, Buffer.from(uuid), null, 0);
        }
        catch (err) { }

        // Wait - allow any other writers to finish writing

        Hoek.wait(2500);

        // Verify - check that we won the race

        const verifyState = await this.getLockState(uuid);
        if (verifyState !== 'locked') {
            if (verifyState === 'open') {
                throw new Error('lock is still open');
            }

            return `lock validation failed: ${verifyState} != ${uuid}`;
        }

        return false;
    }

    async pushSegment(streamOrBuffer, name, meta) {

        const key = Path.join(this.baseKey, name);

        /** @type { Minio.ItemBucketMetadata } */
        const headers = {
            'x-amz-acl': this.meta.ACL,
            'Content-Type': meta.mime || 'video/MP2T',
            'Cache-Control': `public, max-age=300, s-maxage=${Math.floor(this.defaultCacheDuration / 1000)}, immutable`
        };

        try {
            const buffer = Buffer.isBuffer(streamOrBuffer) ? streamOrBuffer :
                await streamToBuffer(streamOrBuffer);

            if (meta.size >= 0 && buffer.byteLength !== meta.size) {
                throw Boom.badRequest('Size must match');
            }

            await internals.boomify(this, 'putObject')(key, buffer, headers);
            debug('upload finished for', key);

            // TODO: validate returned etag

            return { url: `s3://${this.Bucket}/${key}`, bytesWritten: buffer.byteLength };
        }
        catch (err) {
            debug('upload failed for', key, err);
            throw err;
        }
    }

    async flushIndex(indexString, { cacheDuration, ended, target_duration }) {

        const cacheTime = ended !== false ? (cacheDuration || this.defaultCacheDuration) : target_duration * 1000 / 2;
        const key = Path.join(this.baseKey, this.indexName);

        const headers = {
            'x-amz-acl': this.meta.ACL,
            'Content-Type': 'application/vnd.apple.mpegURL',
            'Cache-Control': `max-age=${Math.floor(cacheTime / 1000)}, public`
        };

        await internals.boomify(this, 'putObject')(key, indexString, headers);

        return `s3://${this.Bucket}/${key}`;
    }

    async putObject(key, buffer, headers, retries = 5) {

        try {
            if (headers && !this.meta.ACL) {
                delete headers['x-amz-acl'];
            }

            await this.s3.putObject(this.Bucket, key, buffer, headers);
        }
        catch (err) {
            console.error(`S3.putObject fail (${retries}) at ${new Date().toJSON()}`, err);

            if (retries <= 0) {
                throw err;
            }

            if (this.meta.ACL && err.code === 'AccessDenied') {
                console.error('retrying without ACL');
                this.meta.ACL = null;
            }

            await Hoek.wait(10 ** (6 - retries)); // Up to 10^5 = 100s

            return this.putObject(key, buffer, headers, retries - 1);
        }
    }
}


module.exports = HlsUploader;
