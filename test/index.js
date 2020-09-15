'use strict';

const Path = require('path');
const Url = require('url');

const Code = require('@hapi/code');
const HlsRecord = require('..');
const { createSimpleReader } = require('hls-segment-reader');
const Lab = require('@hapi/lab');


const internals = {
    fixtureDir: Path.join(__dirname, 'fixtures')
};


const { describe, it } = exports.lab = Lab.script();
const { expect } = Code;


describe('HlsStreamRecorder', () => {

    it('creates new instances', () => {

        const reader = createSimpleReader(Url.pathToFileURL(Path.join(internals.fixtureDir, '500.m3u8')), { withData: true });
        const recorder = HlsRecord(reader, 's3://not-really-there/ingest');

        expect(recorder).to.be.instanceof(HlsRecord.HlsStreamRecorder);

        reader.abort();
    });
});
