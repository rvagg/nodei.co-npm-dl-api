const rateLimit      = require('function-rate-limit')
    , downloads      = require('npm-download-counts')
    , listStream     = require('list-stream')
    , moment         = require('moment')
    , log            = require('bole')('process-packages')
    , db             = require('./db')
    , sumPackage     = require('./sum-package')

    , period         = 60000 //5 * 60 * 60 * 1000
    , downloadsLimit = rateLimit(100000, period, downloads)


function lastDate (pkg, callback) {
  function collected (err, data) {
    if (err)
      return callback(err)

    return callback(null, data && data[0] && data[0].key)
  }

  try {
    db.packageCountDb(pkg).createReadStream({ reverse: true, limit: 1 }).pipe(listStream.obj(collected))
  } catch (e) {
    log.error('createReadStream error for', pkg)
    log.error(e.stack)
    callback()
  }
}


function save (date, pkg, batch, callback) {
  db.packageCountDb(pkg).batch(batch, function (err) {
    if (err)
      log.error('Error saving count data for %s: %s', pkg, err.message)

    sumPackage(date, pkg, callback)
  })
}


// download data is sparse, so we need to zero-fill it and
// convert it into leveldb batch objects
function downloadDataToBatch (start, end, data) {
  var day   = start
    , batch = []
    , dayS

  var dataMap = data.reduce(function (p, c) {
    p[c.day] = c.count
    return p
  }, {})

  while (day < end) {
    dayS = day.format('YYYY-MM-DD')
    batch.push({ type: 'put', key: dayS, value: dataMap[dayS] || 0 })
    day = day.add(1, 'days')
  }

  return batch
}

/*
fetchSince(null, ~001_skt~2015-06-23
Deprecation warning: moment construction falls back to js Date. This is discouraged and will be removed in upcoming major release. Please refer to https://github.com/moment/moment/issues/1407 for more info.
Error
    at deprecate (/home/nodeico/npm-dl-api/node_modules/moment/moment.js:738:42)
    at /home/nodeico/npm-dl-api/node_modules/moment/moment.js:826:50
    at /home/nodeico/npm-dl-api/node_modules/moment/moment.js:8:85
    at Object.<anonymous> (/home/nodeico/npm-dl-api/node_modules/moment/moment.js:11:2)
    at Module._compile (module.js:428:26)
    at Object.Module._extensions..js (module.js:446:10)
    at Module.load (module.js:353:32)
    at Function.Module._load (module.js:308:12)
    at Module.require (module.js:363:17)
    at require (module.js:382:17)
~001_skt~2015-06-23 - NaN - downloadsLimit(001_test, Invalid Date, function toDate() {
        return this._offset ? new Date(+this) : this._d;
*/

function processPackage (date, pkg, callback) {
  lastDate(pkg, fetchSince)

  function fetchSince (err, lastDate) {
console.log(`fetchSince(${err}, ${lastDate}`)
    if (err) {
      log.error('Last-date fetch error for for %s: %s', pkg, err.message)
      return callback()
    }

    var start = moment(lastDate)
      , end   = moment(date).utcOffset(0).subtract('day', 1)
    if (!start.isValid())
      start = moment(date).utcOffset(0).subtract(1, 'year')

    if (start.isSame(end, 'day') || start.isAfter(end, 'day'))
{
console.log(`same day: ${start}, ${end}`)
      return sumPackage(date, pkg, callback)
}

    start = start.subtract('days', 5) // back up a bit just to make sure we have it all

console.log(`${lastDate} - ${start} - downloadsLimit(${pkg}, ${start.toDate()}, ${end.toDate})`)
    downloadsLimit(pkg, start.toDate(), end.toDate(), counts)

    function counts (err, data) {
      if (err) {
        log.error('Count fetch error for %s: %s', pkg, err.message)
        if (!/no stats for this package for this range/.test(err.message))
          return sumPackage(date, pkg, callback)
        else
          data = [] // no stats, so zero fill
      }

      if (!Array.isArray(data)) {
        log.error('Unexpected count data for %s: %s', pkg, JSON.stringify(data))
        return sumPackage(date, pkg, callback)
      }

      var batch = downloadDataToBatch(start, end, data)

      save(date, pkg, batch, callback)
    }
  }
}


module.exports = processPackage
