'use strict';

const Path = require('path');
const Url = require('url');

const Code = require('@hapi/code');
const HlsRecord = require('..');
const { HlsSegmentReader } = require('hls-segment-reader');
const Lab = require('@hapi/lab');


const internals = {
    fixtureDir: Path.join(__dirname, 'fixtures')
};


const { describe, it } = exports.lab = Lab.script();
const { expect } = Code;


describe('HlsStreamRecorder', () => {

    it('handles immediate reader.destroy()', async () => {

        const reader = new HlsSegmentReader(Url.pathToFileURL(Path.join(internals.fixtureDir, '500.m3u8')), { withData: true });
        const recorder = HlsRecord(reader, 's3://not-really-there/ingest');

        expect(recorder).to.be.instanceof(HlsRecord.HlsStreamRecorder);

        reader.destroy();

        await expect(recorder.completed()).to.reject('premature close');
    });

    it('handles immediate recorder.destroy() with error', async () => {

        const reader = new HlsSegmentReader(Url.pathToFileURL(Path.join(internals.fixtureDir, '500.m3u8')), { withData: true });
        const recorder = HlsRecord(reader, 's3://not-really-there/ingest');

        expect(recorder).to.be.instanceof(HlsRecord.HlsStreamRecorder);

        reader.destroy(new Error('aborted'));

        await expect(recorder.completed()).to.reject('aborted');
    });
});
