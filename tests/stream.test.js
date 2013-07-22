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
    process.nextTick(cb);
  };


  var wrote = 0;
  function writeSome() {
    process.nextTick(function() {
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