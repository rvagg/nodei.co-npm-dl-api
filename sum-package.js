'use strict'

const moment     = require('moment')
    , after      = require('after')
    , listStream = require('list-stream')
    , log        = require('bole')('sum-package')
    , db         = require('./db')

    , avgPeriod  = 30


function sumKey (i) {
  let k = '000000000000000' + i.toString()
  return k.substring(k.length - 16)
}


function sumPackage (date, pkg, callback) {
  let pkgCountDb = db.packageCountDb(pkg)
    , start      = moment(date).utcOffset(0).subtract(avgPeriod + 1, 'days').format('YYYY-MM-DD')
    , end        = moment(date).utcOffset(0).subtract(1, 'days').format('YYYY-MM-DD')
    , dsumDb     = db.dateSumDb(end)
    , done       = after(2, function (err) { callback(err, count) })
    , count

  function collected (err, data) {
    count = data.reduce((p, c) => p + parseInt(c.value, 10), 0)

    dsumDb.put(
        sumKey(count) + '!' + pkg
      , JSON.stringify({ 'package': pkg, count: count })
      , done
    )
    db.packageDateDb(end).put(pkg, String(count), done)
  }

  //log.debug('Calculating %d day sums for %s', avgPeriod, end)

  try {
    pkgCountDb.createReadStream({ gte: start, lt: end })
              .pipe(listStream.obj(collected))
  } catch (e) {
    log.error('createReadStream error for', pkg)
    log.error(e.stack)
    callback()
  }
}


module.exports = sumPackage
