const http           = require('http')
    , fs             = require('fs')
    , url            = require('url')
    , querystring    = require('querystring')
    , Router         = require('routes-router')
    , bole           = require('bole')
    , uuid           = require('node-uuid')
    , sendJson       = require('send-data/json')
    , sendError      = require('send-data/error')
    , allDownloads   = require('./npm-all-downloads')
    , updatePackages = require('./update-package-list')
    , api            = require('./api')

    , log            = bole('server')
    , reqLog         = bole('server:request')
    , updaterLog     = bole('updater')

    , isDev          = (/^dev/i).test(process.env.NODE_ENV)
    , port           = process.env.PORT || 3000
    , start          = new Date()

    , periodicInterval = 1000 * 60 * 60 * 12


bole.output({
  level  : isDev ? 'debug' : 'info',
  stream : process.stdout
})

if (process.env.LOG_FILE) {
  console.log('Starting logging to %s', process.env.LOG_FILE)
  bole.output({
    level  : 'debug',
    stream : fs.createWriteStream(process.env.LOG_FILE)
  })
}


process.on('uncaughtException', function (err) {
  log.error(err)
  process.exit(1)
})


function sendData (req, res) {
  return function (err, data) {
    if (err)
      return sendError(req, res)

    sendJson(req, res, { body: data, statusCode: 200 })
  }
}


function pkgRankRoute (req, res, opts) {
  api.pkgRank(opts.params.pkg, sendData(req, res))
}


function pkgDownloadsRoute (req, res, opts) {
  var qs   = querystring.parse(url.parse(req.url).query)
    , days = parseInt(qs.days || 30, 10)

  if (days < 1 || days > 365)
    days = 30

  api.pkgDownloads(opts.params.pkg, days, sendData(req, res))
}


function topDownloadsRoute (req, res) {
  var qs    = querystring.parse(url.parse(req.url).query)
    , count = parseInt(qs.count || 50, 10)

  if (count < 1 || count > 500)
    count = 50

  api.topDownloads(count, sendData(req, res))
}


var router = Router({
    errorHandler: function (req, res, err) {
      req.log.error(err)
      sendError(req, res, err)
    }

  , notFound: function (req, res) {
      sendJson(req, res, {
          body: { 'error': 'Not found: ' + req.url }
        , statusCode: 404
      })
    }
})


router.addRoute('/rank/:pkg', pkgRankRoute)
router.addRoute('/downloads/:pkg', pkgDownloadsRoute)
router.addRoute('/top', topDownloadsRoute)


function handler (req, res) {
  if (req.url == '/_status')
    return sendJson(req, res, { body: { ok: true }, statusCode: 200 })

  // unique logger for each request
  req.log = reqLog(uuid.v4())
  req.log.info(req)

  res.setHeader('x-startup', start)
  res.setHeader('x-powered-by', 'whatevs')

  router(req, res)
}


http.createServer(handler)
  .on('error', function (err) {
    log.error(err)
    throw err
  })
  .listen(port, function (err) {
    if (err) {
      log.error(err)
      throw err
    }

    log.info('Server started on port %d', port)
    console.log()
    console.log('>> Running: http://localhost:' + port)
    console.log()
  })


function periodic () {
  var start = Date.now()
  updaterLog.info('Starting periodic update')

  updatePackages(function (err) {
    if (err) {
      updaterLog.error(err)
      return process.exit(1)
    }

    allDownloads.processAllPackages(function (err) {
      if (err) {
        updaterLog.error(err)
        return process.exit(1)
      }

      updaterLog.info('Finished periodic update, took % seconds', Date.now() - start)
    })
  })
}


setInterval(periodic, periodicInterval)
setTimeout(periodic, 1000 * 60)