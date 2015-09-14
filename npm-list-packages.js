'use strict'

const hyperquest = require('hyperquest')
    , bl         = require('bl')
    , fs         = require('fs')


const allPackagesUrl = 'https://registry.npmjs.org/-/all/static/all.json?limit=1000'
    , debugSource    = `${__dirname}/all.json`


// load the list of all npm libs with 'repo' pointing to GitHub
function listPackages (callback) {
  function source () {
    if (process.env.DEBUG && fs.statSync(debugSource))
      return fs.createReadStream(debugSource)
    return hyperquest(allPackagesUrl)
  }

  source().pipe(bl((err, data) => {
    if (err)
      return callback(err)

    let packages

    try {
      packages = JSON.parse(data.toString())
    } catch (e) {
      return  callback(e)
    }

    let names = Object.keys(packages).filter((name) => {
      let versions = packages[name].versions
      return versions && Object.keys(versions).length > 0
    })

    callback(null, names)
  }))
}


module.exports = listPackages


if (require.main === module)
  listPackages((err, names) => console.log(`${names.length} packages`))
