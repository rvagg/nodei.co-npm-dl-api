const db         = require('./db')
    , moment     = require('moment')
    , through2   = require('through2')
    , updaterLog = require('bole')('updater')


function calculateRanks (date, total, callback) {
  var dateStr = moment(date).zone(0).subtract('days', 1).format('YYYY-MM-DD')
  var dsumDb  = db.dateSumDb(dateStr)
    , pos     = 1

  function process (data, enc, callback) {
    db.packageDb.put(
        data['package']
      , { rank: pos, total: total, date: dateStr }
      , { valueEncoding: 'json' }
      , function (err) {
          if (err)
            updaterLog.error(new Error('Error writing ranking data for ' + data.package + ':' + err.message))
          callback()
        }
    )
    pos++
  }

  function onErrorOrEnd (err) {
    if (err)
      updaterLog.error(err)
    callback && callback(err)
    callback = null
  }

  dsumDb.createValueStream({ reverse: true, valueEncoding: 'json' })
    .on('error', onErrorOrEnd)
    .pipe(through2.obj(process))
    .on('error', onErrorOrEnd)
    .on('finish', onErrorOrEnd)
}


module.exports = calculateRanks