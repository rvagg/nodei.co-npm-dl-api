const moment     = require('moment')
    , after      = require('after')
    , listStream = require('list-stream')
    , db         = require('./db')
    , updaterLog = require('bole')('updater')

    , avgPeriod  = 30


function sumKey (i) {
  var k = '000000000000000' + i.toString()
  return k.substring(k.length - 16)
}


function sumPackage (pkg, callback) {
  var pkgCountDb = db.packageCountDb(pkg)
    , start      = moment().zone(0).subtract('days', avgPeriod + 1).format('YYYY-MM-DD')
    , end        = moment().zone(0).subtract('days', 1).format('YYYY-MM-DD')
    , dsumDb     = db.dateSumDb(end)
    , done       = after(2, function (err) { callback(err, count) })
    , count

  function collected (err, data) {
    count = data.reduce(function (p, c) {
      return p + parseInt(c.value, 10)
    }, 0)

    dsumDb.put(
        sumKey(count) + '!' + pkg
      , { 'package': pkg, count: count }
      , { valueEncoding: 'json' }
      , done
    )
    db.packageDateDb(end).put(pkg, count, done)
  }
  
  function onError (err) {
    updaterLog.error(err)
  }

  try {
    pkgCountDb
      .createReadStream({ gte: start, lt: end })
      .on('error', onError)
      .pipe(listStream.obj(collected))
      .on('error', onError)
  } catch (e) {
    updaterLog.error(e)
    callback()
  }
}


module.exports = sumPackage