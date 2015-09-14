const level  = require('level')
    , spaces = require('level-spaces')


const db        = level('./counts.db', { keyEncoding: 'utf8', valueEncoding: 'utf8' })
    , packageDb = spaces(db, 'package', { keyEncoding: 'utf8', valueEncoding: 'utf8' })
    , countDb   = spaces(db, 'count', { keyEncoding: 'utf8', valueEncoding: 'utf8' })
    , sumDb     = spaces(db, 'sum', { keyEncoding: 'utf8', valueEncoding: 'utf8' })


module.exports.db        = db
module.exports.packageDb = packageDb
module.exports.countDb   = countDb
module.exports.sumDb     = sumDb


module.exports.packageCountDb = function (pkg) {
  return spaces(countDb, pkg)
}


module.exports.dateSumDb = function (date) {
  return spaces(sumDb, date)
}


module.exports.packageDateDb = function (date) {
  return spaces(packageDb, date)
}