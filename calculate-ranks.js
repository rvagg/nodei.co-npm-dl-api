'use strict'

const db       = require('./db')
    , moment   = require('moment')
    , through2 = require('through2')
    , log      = require('bole')('calculate-ranks')


function calculateRanks (date, total, callback) {
  let dateStr   = moment(date)
                    .utcOffset(0)
                    .subtract(1, 'days')
                    .format('YYYY-MM-DD')
    , dsumDb    = db.dateSumDb(dateStr)
    , pos       = 1
    , lastCount = -1
    , lastRank  = -1

  function process (_data, enc, callback) {
    let data  = JSON.parse(_data)
      , count = data.count
        // if the count for this pkg is same as the last then they have
        // the same rank, but we still increment 'pos' so there will
        // be blank ranks because of the duplicates
      , rank  = count === lastCount ? lastRank : pos
      , value = {
          rank  : rank
        , total : total
        , count : count
        , date  : dateStr
      }

    db.packageDb.put(
        data['package']
      , JSON.stringify(value)
      , (err) => {
          if (err)
            log.error(new Error(`Error writing ranking data for ${data.package}: ${err.message}`))
          callback()
        }
    )

    pos++
    lastRank  = rank
    lastCount = count
  }

  function onErrorOrEnd (err) {
    if (err)
      log.error(err)
    else
      log.debug(`Finished ranking %d packages ${pos - 1}`)

    callback && callback(err)
    callback = null
  }

  log.debug(`Calculating rankings for ${dateStr}`)

  dsumDb.createValueStream({ reverse: true })
    .on('error', onErrorOrEnd)
    .pipe(through2.obj(process))
    .on('error', onErrorOrEnd)
    .on('finish', onErrorOrEnd)
}


module.exports = calculateRanks
