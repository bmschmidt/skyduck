"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var firehose_1 = require("@skyware/firehose");
// import { Database } from 'duckdb-async';
// const db = await Database.create('skeets.duckdb');
var firehose = new firehose_1.Firehose({ relay: 'wss://bsky.network' });
var queue = [];
firehose.on('commit', function (message) {
    var _a, _b;
    for (var _i = 0, _c = message.ops; _i < _c.length; _i++) {
        var op = _c[_i];
        if (!op.record) {
            continue;
        }
        if (!((_a = op.record) === null || _a === void 0 ? void 0 : _a.text)) {
            continue;
        }
        if (!((_b = op.record) === null || _b === void 0 ? void 0 : _b.langs.includes('en'))) {
            continue;
        }
        queue.push(op);
        var uri = 'at://' + message.repo + '/' + op.path;
        console.log('Operation:', op);
    }
});
firehose.start();
setInterval(function () {
    queue.map(function (c) {
        console.log({ c: c });
    });
    queue.length = 0;
}, 1000);
