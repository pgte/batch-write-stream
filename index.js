var setImmediate = global.setImmediate || process.nextTick;

exports = module.exports = BatchObjectWriteStream;
BatchObjectWriteStream.WritableState = WritableState;

var util = require('util');
var assert = require('assert');
var Stream = require('stream');

util.inherits(BatchObjectWriteStream, Stream);

function WritableState(options, stream) {
  options = options || {};

  // the point at which write() starts returning false
  // Note: 0 is a valid value, means that we always return false if
  // the entire buffer is not flushed immediately on write()
  var hwm = options.highWaterMark;
  this.highWaterMark = (hwm || hwm === 0) ? hwm : 100000;

  // max number of concurrent batch writes
  this.maxConcurrentBatches = options.maxConcurrentBatches || 1;

  // time to wait for when scheduling a flush
  this.flushWait = options.flushWait || 10;

  // cast to ints.
  this.highWaterMark = ~~this.highWaterMark;

  this.needDrain = false;
  // at the start of calling end()
  this.ending = false;
  // when end() has been called, and returned
  this.ended = false;
  // when 'finish' is emitted
  this.finished = false;

  // if we have a flush scheduled
  this.scheduled = false;

  // not an actual buffer we keep track of, but a measurement
  // of how much we're waiting to get pushed to some underlying
  // socket or file.
  this.length = 0;

  // a flag to see when we're in the middle of a write.
  this.writing = 0;

  this.buffer = [];
  this.callbacks = [];
}

function BatchObjectWriteStream(options) {
  if (!(this instanceof BatchObjectWriteStream) && !(this instanceof Stream.Duplex))
    return new BatchObjectWriteStream(options);

  var state = this._writableState = new WritableState(options, this);

  // legacy.
  this.writable = true;
  this.readable = false;

  var stream = this;
  this._flushFn = function() {
    flush(stream, state);
  };

  Stream.call(this);
}

// Otherwise people can pipe Writable streams, which is just wrong.
BatchObjectWriteStream.prototype.pipe = function() {
  this.emit('error', new Error('Cannot pipe. Not readable.'));
};


function writeAfterEnd(stream, state, cb) {
  var er = new Error('write after end');
  // TODO: defer error events consistently everywhere, not just the cb
  stream.emit('error', er);
  setImmediate(function() { cb(er); });
}

BatchObjectWriteStream.prototype.write = function(chunk, encoding, cb) {
  var state = this._writableState;
  var ret = false;

  if (! this.writable) return;

  if (arguments.length < 3) {
    cb = encoding;
    encoding = undefined;
  }

  if (state.ended)
    writeAfterEnd(this, state, cb);
  else {
    state.length += 1;

    ret = state.length < state.highWaterMark;
    state.needDrain = !ret;

    if (encoding && ! chunk.encoding) chunk.encoding = encoding;

    chunk = this._map(chunk);

    state.buffer.push(chunk);
    if (cb) {
      state.callbacks.push(cb);
    }

    scheduleFlushMaybe(this, state);
  }

  return ret;
};

function scheduleFlushMaybe(stream, state) {
  if (! state.scheduled) {
    state.scheduled = true;
    setTimeout(stream._flushFn, stream.flushWait);
  }
}


function flush(stream, state) {
  state.scheduled = false;
  var buffer = state.buffer;
  if ((state.writing < state.maxConcurrentBatches) && buffer.length) {
    var callbacks = state.callbacks;
    state.buffer = [];
    state.callbacks = [];

    state.writing ++;
    if (state.writing > 1) console.log('writing = ', state.writing);
    if (stream.writable) stream._writeBatch(buffer, onWrite);

    function onWrite(err) {
      state.writing --;
      state.length -= buffer.length;
      onwrite(stream, state, err, callbacks);
    }
  }
}

function onwriteError(stream, state, er, cbs) {
  var cb;
  for(var i = 0 ; i < cbs.length; i ++) {
    cb = cbs[i];
    cb(er);
  }

  stream.emit('error', er);
}

function onwrite(stream, state, er, cbs) {

  if (er)
    onwriteError(stream, state, er, cbs);
  else {
    // Check if we're actually ready to finish, but don't emit yet
    var finished = needFinish(stream, state);

    if (!finished && state.buffer.length)
      scheduleFlushMaybe(stream, state);

    afterWrite(stream, state, finished, cbs);
  }
}

function afterWrite(stream, state, finished, cbs) {
  if (!finished && state.length == 0 && state.needDrain) {
    state.needDrain = false;
    stream.emit('drain');
  }
  for (var i = 0 ; i < cbs.length; i ++) {
    cbs[i]();
  }
  if (finished)
    finishMaybe(stream, state);
}

BatchObjectWriteStream.prototype._writeBatch = function(batch, cb) {
  cb(new Error('not implemented'));
};

BatchObjectWriteStream.prototype._map = function(d) {
  return d;
};

BatchObjectWriteStream.prototype.end = function(chunk, cb) {
  var state = this._writableState;

  if (typeof chunk !== 'undefined' && chunk !== null)
    this.write(chunk);

  // ignore unnecessary end() calls.
  if (!state.ending && !state.finished)
    endWritable(this, state, cb);
};

BatchObjectWriteStream.prototype.destroy = function destroy() {
  var state = this._writableState;

  var stream = this;
  state.writing = 0;
  this.writable = false;

  setImmediate(function() {
    state.finished = true;
    stream.emit('finish');
  });

  this.end();
};

BatchObjectWriteStream.prototype.destroySoon = function destroySoon() {
  this.end();
};

function needFinish(stream, state) {
  return (state.ending &&
          state.length === 0 &&
          !state.finished &&
          !state.writing &&
          !state.scheduled);
}

function finishMaybe(stream, state) {
  if (needFinish(stream, state)) {
    state.finished = true;
    stream.writable = false;
    stream.emit('finish');
  }
}

function endWritable(stream, state, cb) {
  state.ending = true;
  flush(stream, state);
  finishMaybe(stream, state);
  if (cb) {
    if (state.finished)
      setImmediate(cb);
    else
      stream.once('finish', cb);
  }
  state.ended = true;
}