'use strict';

const Assert = require('assert');
const Fs = require('fs');
const Path = require('path');
const Url = require('url');
const Util = require('util');
const debug = require('debug')('hls:uploader');

const Aws = require('aws-sdk');
const Pati = require('pati');
const WriteFileAtomic = require('write-file-atomic');


const internals = {};


internals.fs = {
    appendFile: Util.promisify(Fs.appendFile)
};


class HlsUploader {

    constructor(targetUri, options) {

        const url = new URL(targetUri);

        Assert.ok(url.pathname.endsWith('/'), 'Url must end with a "/"');
        Assert.ok(!url.hash, 'Url must not contain a hash part');
        Assert.ok(!url.search, 'Url must not contain a search string');

        options = Object.assign({}, options, {
            indexName: options.indexName || 'index.m3u8'
        });

        if (url.protocol === 's3:') {
            Assert.strictEqual(!!options.collect, false, 'Collect not supported with s3:');
            this.impl = new S3UploaderImpl(url, options);
        }
        else if (url.protocol === 'file:') {
            this.impl = new FileUploaderImpl(url, options);
        }
        else {
            Assert.fail('Unsupported protocol: ' + url.protocol);
        }
    }

    /*async*/ pushSegment(stream, name, meta) {

        return this.impl.pushSegment(stream, name, meta);
    }

    /*async*/ flushIndex(m3u8) {

        return this.impl.flushIndex(m3u8.toString().trim(), m3u8);
    }
}


class FileUploaderImpl {

    constructor(url, options) {

        this.collect = !!options.collect;
        this.indexName = options.indexName;
        this.targetPath = url;

        this.lastIndexString = '';
        this.segmentBytes = 0;

        // TODO: make async?
        if (!Fs.existsSync(this.targetPath)) {
            Fs.mkdirSync(this.targetPath, { recursive: true });
        }
    }

    async pushSegment(stream, name, meta) {

        const append = this.collect && this.segmentBytes !== 0;

        const url = new URL(name, this.targetPath);
        const target = Fs.createWriteStream(url, { flags: append ? 'a' : 'w' });
        stream.pipe(target);

        const dispatcher = new Pati.EventDispatcher(stream);

        dispatcher.on('end', Pati.EventDispatcher.end);

        let bytesWritten = 0;
        dispatcher.on('data', (chunk) => {

            bytesWritten += +chunk.length;
        });

        try {
            // TODO: handle target errors & wait for end?
            await dispatcher.finish();

            Assert.equal(bytesWritten, meta.size);

            return url.href;
        }
        finally {
            this.segmentBytes += bytesWritten;
        }
    }

    async flushIndex(indexString, m3u8) {

        let appendString;
        if (this.lastIndexString && indexString.startsWith(this.lastIndexString)) {
            const lastLength = this.lastIndexString.length;
            appendString = indexString.substr(lastLength);
        }

        this.lastIndexString = indexString;

        const url = new URL(this.indexName, this.targetPath);

        if (appendString) {
            await internals.fs.appendFile(url, appendString);
        }
        else {
            await WriteFileAtomic(Url.fileURLToPath(url), indexString);
        }

        return url.href;
    }
}


class S3UploaderImpl {

    constructor(url, options) {

        this.indexName = options.indexName;
        this.cacheDuration = options.cacheDuration || 7 * 24 * 3600 * 1000;

        const params = {
            params: {
                Bucket: url.host,
                ACL: 'public-read'
            }
        };

        this.s3 = new Aws.S3(params);
        this.baseKey = (url.pathname || '/').slice(1);
    }

    async pushSegment(stream, name, meta) {

        const { s3 } = this;

        const params = {
            Body: stream,
            Key: Path.join(this.baseKey, name),
            ContentType: meta.mime || 'video/MP2T',
            CacheControl: `max-age=300, s-max-age=${Math.floor(this.cacheDuration / 1000)}, public`,
            ContentLength: meta.size
        };

        try {
            const data = await s3.upload(params).promise();
            debug('upload finished for', params.Key);

            return `s3://${data.Bucket}/${data.Key}`;
        }
        catch (err) {
            debug('upload failed for', params.Key, err);
            throw err;
        }
    }

    async flushIndex(indexString, m3u8) {

        const { s3 } = this;

        const cacheTime = m3u8.ended ? this.cacheDuration : m3u8.target_duration * 1000 / 2;

        const params = {
            Body: indexString,
            Key: Path.join(this.baseKey, this.indexName),
            ContentType: 'application/vnd.apple.mpegURL',
            CacheControl: `max-age=${Math.floor(cacheTime / 1000)}, public`
        };

        const data = await s3.upload(params).promise();

        return `s3://${data.Bucket}/${data.Key}`;
    }
}


module.exports = HlsUploader;
