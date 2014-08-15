const rateLimit      = require('function-rate-limit')
    , downloads      = require('npm-download-counts')
    , listStream     = require('list-stream')
    , moment         = require('moment')
    , db             = require('./db')
    , sumPackage     = require('./sum-package')
    , updaterLog     = require('bole')('updater')


      // rate-limit the data-fetch to 100,000 calls in a 6h period
    , period         = 6 * 60 * 60 * 1000
    , downloadsLimit = rateLimit(100000, period, downloads)


function lastDate (pkg, callback) {
  var pkgCountDb = db.packageCountDb(pkg)

  function collected (err, data) {
    //console.log('lastDate', arguments)
    if (err)
      return callback(err)

    return callback(null, data && data[0] && data[0].key)
  }

  try {
    pkgCountDb.createReadStream({ reverse: true, limit: 1 }).pipe(listStream.obj(collected))
  } catch (e) {
    updaterLog.error(e)
    callback()
  }
}


function save (pkg, batch, callback) {
  db.packageCountDb(pkg).batch(batch, function (err) {
    if (err)
      updaterLog.error(err, 'Error saving count data for ' + pkg + ': ' + err.message)

    sumPackage(pkg, callback)
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

  while (day <= end) {
    dayS = day.format('YYYY-MM-DD')
    batch.push({ type: 'put', key: dayS, value: dataMap[dayS] || 0 })
    day = day.add('days', 1)
  }

  return batch
}


function processPackage (date, pkg, callback) {
  lastDate(pkg, fetchSince)

  function fetchSince (err, lastDate) {
    if (err) {
      updaterLog.error(err, 'Last-date fetch error for for ' + pkg + ': ' + err.message)
      return callback()
    }

    var start = !!lastDate ? moment(lastDate) : moment(date).zone(0).subtract('year', 1)
      , end   = moment(date).zone(0).subtract('day', 1)

    if (start.isSame(end, 'day') || start.isAfter(end, 'day'))
      return sumPackage(pkg, callback)

    start = start.subtract('days', 5) // back up a bit just to make sure we have it all

    downloadsLimit(pkg, start.toDate(), end.toDate(), counts)

    function counts (err, data) {
      if (err) {
        updaterLog.error(err, 'Count fetch error for %s: %s', pkg, err.message)
        if (!/no stats for this package for this range/.test(err.message))
          return sumPackage(pkg, callback)
        else
          data = [] // no stats, so zero fill
      }

      if (!Array.isArray(data)) {
        updaterLog.error(new Error('Unexpected count data for ' + pkg + ': ' + JSON.stringify(data)))
        return sumPackage(pkg, callback)
      }

      var batch = downloadDataToBatch(start, end, data)

      save(pkg, batch, callback)
    }
  }
}


module.exports = processPackage