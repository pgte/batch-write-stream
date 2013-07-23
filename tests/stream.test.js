var setImmediate = global.setImmediate || process.nextTick;
var test = require('tap').test;

var BatchWriteStream = require('../');

test('can write and end', function(t) {
  var s = new BatchWriteStream();

  var wrote = [];
  s._writeBatch = function(batch, cb) {
    batch.forEach(function(d) {
      wrote.push(d);
    });

    cb();
  };

  s.once('finish', function() {
    t.ok(called);
    t.deepEqual(wrote, ['ABC', 'DEF']);
    t.notOk(s.writable);
    t.end();
  });

  s.write('ABC', onWrite);
  s.end('DEF');

  var called = false;
  function onWrite(err) {
    if (err) throw err;
    called = true;
  }

});

test('batches correctly', function(t) {
  var s = new BatchWriteStream();


  var max = 1000;
  var chunkSize = 100;

  s._writeBatch = function(batch, cb) {
    t.equal(batch.length, 100);
    setImmediate(cb);
  };


  var wrote = 0;
  function writeSome() {
    setImmediate(function() {
      for (var i = 0 ; i < chunkSize; i ++) {
        wrote ++;
        s.write(wrote, onWrote);
      }
      if (wrote < max) writeSome();
      else s.end();
    });
  }

  writeSome();

  var onWrotes = 0;
  function onWrote() {
    onWrotes ++;
  }

  s.once('finish', onFinish);

  function onFinish() {
    t.equal(onWrotes, max);
    t.end();
  }

});

test('allows parallel batches', function(t) {

  var maxParallel = 4;
  var max = 1000;
  var chunkSize = 100;

  var s = new BatchWriteStream({maxConcurrentBatches: 4});

  var parallel = 0;

  s._writeBatch = function(batch, cb) {

    t.equal(batch.length, 100);
    parallel ++;
    if (parallel >= maxParallel) process.nextTick(cb);
    else {
      (function schedule() {
        setTimeout(function() {
          if (parallel >= maxParallel) process.nextTick(cb);
          else schedule();
        }, 100);
      }());
    }
  };


  var wrote = 0;
  function writeSome() {
    setImmediate(function() {
      for (var i = 0 ; i < chunkSize; i ++) {
        wrote ++;
        s.write(wrote, onWrote);
      }
      if (wrote < max) writeSome();
      else s.end();
    });
  }

  writeSome();

  var onWrotes = 0;
  function onWrote() {
    onWrotes ++;
  }

  s.once('finish', onFinish);

  function onFinish() {
    t.equal(onWrotes, max);
    t.end();
  }
});

function xtest() {}