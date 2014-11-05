const moment         = require('moment')
    , through2       = require('through2')
    , log            = require('bole')('npm-all-downloads')
    , listPackages   = require('./npm-list-packages')
    , db             = require('./db')
    , calculateRanks = require('./calculate-ranks')
    , processPackage = require('./process-package')


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
        log.debug('Deleting %d entries from sum database @ %s', batch.length, end)
        dsumDb.batch(batch, callback)
      })
  }

  function run () {
    var processed    = 0
      , packageCount = 0

    function onPackage (pkg, enc, _callback) {
      var self = this

      packageCount++

      processPackage(date, pkg, function (err, count) {
        processed++

        if (err)
          log.error(err)
        else
          log.debug({ number: processed, 'package': pkg, count: isFinite(count) ? String(count) : 'unknown' })

        if (!err)
          return _callback()

        self.push(null)

        callback && callback(err)
        callback = null
      })
    }

    function onEnd (_callback) {
      if (callback)
        calculateRanks(date, packageCount, callback)

      _callback()
    }

    db.packageDb.createKeyStream()
      .pipe(through2.obj(onPackage, onEnd))
  }

  clean(run)
}


module.exports.processAllPackages = processAllPackages