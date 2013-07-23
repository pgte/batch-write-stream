# batch-write-stream

> Write stream that batches all writes done in the same tick.

[![Build Status](https://travis-ci.org/pgte/batch-write-stream.png?branch=master)](https://travis-ci.org/pgte/batch-write-stream)

## Install

```bash
$ npm install batch-write-stream --save
```

## Use

### Require

```javascript
var BatchWriteStream = require('batch-write-stream');
```

### Create

```javascript
var stream = BatchWriteStream();
```

Or, with options:

```javascript
var options = {
  highWaterMark: 100,  // default
  maxConcurrentBatches: 1 // default
};

var stream = BatchWriteStream(options);
```

### Implement _writeBatch

```javascript
stream._writeBatch = function(batch, cb) {
  // batch is an array
  // call cb when done
}
```

### Map

You can map each value before it is written:

```javascript
stream._map = function(d) {
  return transform(d);
};
```

## License

MIT