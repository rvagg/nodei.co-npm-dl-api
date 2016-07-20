'use strict'

var updaterLog = require('bole')('updater')
  , moment     = require('moment')
  , db         = require('./db')
  , periodicInterval = 1000 * 60 * 60 * 12

// TODO: avoid overlap of periodic jobs
setInterval(periodic, periodicInterval)
setTimeout(periodic.bind(null, true), 1000)


function periodic (first) {
  var start   = Date.now()
    , options = {}

  updaterLog.info('Starting periodic update')

  // only collect the full data set on the first run of this instance, otherwise just go back a month
  if (!first)
    options.start = moment.utc().add(-1, 'month').toDate()

  db.update(options)
  db.once('updated', function onUpdated () {
    updaterLog.info('Finished periodic update, took ' + ((Date.now() - start) / 1000) + ' seconds')
    start = Date.now()
    updaterLog.info('Starting periodic rank')
    db.rank()
    db.once('ranked', function onRanked () {
      updaterLog.info('Finished periodic rank, took ' + ((Date.now() - start) / 1000) + ' seconds')
    })
  })
}
