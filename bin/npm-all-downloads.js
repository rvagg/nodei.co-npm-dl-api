const moment       = require('moment')
    , format       = require('humanize-number')
    , allDownloads = require('../npm-all-downloads')
    , db           = require('../db')


if (process.argv[2] == '--top') {
  var count = process.argv.length > 3 ? parseInt(process.argv[3], 10) : 25
    , r     = 1

  var dsumDb = db.sumDb.sublevel(moment().zone(0).subtract('days', 2).format('YYYY-MM-DD'))
  dsumDb.createValueStream({ reverse: true, valueEncoding: 'json', limit: count })
    .on('data', function (data) {
      console.log('%d: %s (%s)', r++, data.package, format(data.count))
    })

} else if (process.argv.length > 2) {
  db.packageDb.get(process.argv[2], function (err, data) {
    if (err)
      throw err

    console.log(data)
  })

  var end = moment().zone(0).subtract('days', 1).format('YYYY-MM-DD')
  db.packageDb.sublevel(end).get(process.argv[2], function (err, count) {
    console.log('Count:', count)
  })

} else {
  allDownloads.processAllPackages(function (err) {
    if (err)
      throw err

    console.log('Done')
  })
}
