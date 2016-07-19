'use strict'

var moment         = require('moment')
  , through2       = require('through2')
  , log            = require('bole')('npm-all-downloads')
  , db             = require('./db')
  , calculateRanks = require('./calculate-ranks')
  , processPackage = require('./process-package')


function processAllPackages (callback) {
  var date = new Date()

  function clean (callback) {
    var end    = moment(date)
                  .utcOffset(0)
                  .subtract(1, 'days')
                  .format('YYYY-MM-DD')
      , dsumDb = db.dateSumDb(end)
      , batch  = []

    dsumDb.createKeyStream()
      .on('data', function onData (key) { batch.push({ type: 'del', key: key }) })
      .on('end', function onEnd () {
        log.debug('Deleting ' + batch.length + ' entries from sum database @ ' + end)
        dsumDb.batch(batch, callback)
      })
  }

  function run () {
    var processed    = 0
      , packageCount = 0

    function onPackage (pkg, enc, _callback) {
      var self = this

      packageCount++

      processPackage(date, pkg, afterProcess)

      function afterProcess (err, count) {
        processed++

        if (err)
          log.error(err)
        else {
          log.debug({
              number: processed
            , 'package': pkg
            , count: isFinite(count) ? String(count) : 'unknown'
          })
        }

        if (!err)
          return _callback()

        // short-circuit, stop the stream
        self.push(null)

        callback && callback(err)
        callback = null
      }
    }

    function onEnd (_callback) {
      if (callback)
        calculateRanks(date, packageCount, callback)
      _callback()
    }

    db.packageDb.createKeyStream({ lt: '~' })
      .pipe(through2.obj(onPackage, onEnd))
  }

  clean(run)
}


module.exports.processAllPackages = processAllPackages

if (require.main === module) {
require('bole').output({
    level  : process.env.NODE_ENV == 'development' ? 'debug' : 'info'
  , stream : process.stdout
})
  processAllPackages(function afterAll () {})
}
