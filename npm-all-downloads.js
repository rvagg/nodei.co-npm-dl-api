const moment         = require('moment')
    , through2       = require('through2')
    , db             = require('./db')
    , calculateRanks = require('./calculate-ranks')
    , processPackage = require('./process-package')
    , updaterLog     = require('bole')('updater')


function processAllPackages (callback) {
  var date = new Date()

  function clean (callback) {
    var end    = moment(date).zone(0).subtract('days', 1).format('YYYY-MM-DD')
      , dsumDb = db.dateSumDb(end)
      , batch  = []

    dsumDb.createKeyStream()
      .on('data', function (key) {
        batch.push({ type: 'del', key: key })
      })
      .on('end', function () {
        updaterLog.debug('Deleting %d entries', batch.length)
        dsumDb.batch(batch, callback)
      })
  }

  function run () {
    var processed = 0

    function onError (err) {
      callback && callback(err)
      callback = null
    }

    function onFinish () {
      if (callback)
        calculateRanks(date, processed, callback)
    }

    function onData (pkg, _, cb) {
      processPackage(date, pkg, function (err, count) {
        if (err)
          return cb(err)

        updaterLog.debug(
            '%d: %s count = %s'
          , ++processed
          , pkg
          , isFinite(count) ? String(count) : 'unknown'
        )
        cb()
      })
    }

    db.packageDb.createKeyStream({ keyEncoding: 'utf8' })
      .on('error', onError)
      .pipe(through2.obj({ highWaterMark: 4 }, onData))
      .on('error', onError)
      .on('finish', onFinish)
  }

  clean(run)
}


module.exports.processAllPackages = processAllPackages