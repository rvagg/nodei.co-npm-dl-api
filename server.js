'use strict'

var http           = require('http')
  , fs             = require('fs')
  , url            = require('url')
  , querystring    = require('querystring')
  , Router         = require('routes-router')
  , bole           = require('bole')
  , uuid           = require('node-uuid')
  , sendJson       = require('send-data/json')
  , sendPlain      = require('send-data/plain')
  , sendError      = require('send-data/error')
  , api            = require('./api')

  , log            = bole('server')
  , reqLog         = bole('server:request')

  , isDev          = (/^dev/i).test(process.env.NODE_ENV)
  , port           = process.env.PORT || 3000
  , start          = new Date()


  // inherited from nodei.co/lib/valid-name.js
  , pkgregex       = '@?\\w*/?[^/@\\s\\+%:]+'
  , router

bole.output({
    level  : isDev ? 'debug' : 'info'
  , stream : process.stdout
})

if (process.env.LOG_FILE) {
  console.log('Starting logging to ' + process.env.LOG_FILE)
  bole.output({
      level  : 'debug'
    , stream : fs.createWriteStream(process.env.LOG_FILE)
  })
}

if (process.env.NODE_TITLE)
  process.title = process.env.NODE_TITLE

process.on('uncaughtException', function ue (err) {
  log.error(err)
  process.exit(1)
})


function sendData (req, res) {
  return function send (err, data) {
    if (err)
      return sendError(req, res, { body: err })

    sendJson(req, res, { body: data, statusCode: 200 })
  }
}


function pkgRankRoute (req, res, opts) {
  res.setHeader('cache-control', 'no-cache')
  api.pkgRank(opts.params.pkg, sendData(req, res))
}


function _pkgDownloadsPreRoute (req, res, opts, route) {
  var qs   = querystring.parse(url.parse(req.url).query)
    , days = parseInt(qs.days || 30, 10)

  if (days < 1)
    days = 30
  else if (days > 366)
    days = 365

  opts.days = days

  res.setHeader('cache-control', 'no-cache')
  route()
}


function pkgDownloadSumRoute (req, res, opts) {
  _pkgDownloadsPreRoute(req, res, opts, function preRoute () {
    api.pkgDownloadSum(opts.params.pkg, opts.days, sendData(req, res))
  })
}


function pkgDownloadDaysRoute (req, res, opts) {
  _pkgDownloadsPreRoute(req, res, opts, function preRoute () {
    api.pkgDownloadDays(opts.params.pkg, opts.days, sendData(req, res))
  })
}


function topDownloadsRoute (req, res) {
  var qs    = querystring.parse(url.parse(req.url).query)
    , count = parseInt(qs.count || 50, 10)

  if (count < 1 || count > 500)
    count = 50

  res.setHeader('cache-control', 'no-cache')
  api.topDownloads(count, sendData(req, res))
}


function totalDownloadsRoute (req, res) {
  res.setHeader('cache-control', 'no-cache')
  api.totalDownloads(sendData(req, res))
}


router = Router({
    errorHandler: function errorHandler (req, res, err) {
      req.log.error(err)
      sendError(req, res, { body: err })
    }

  , notFound: function notFound (req, res) {
      sendJson(req, res, {
          body: { 'error': 'Not found: ' + req.url }
        , statusCode: 404
      })
    }
})


router.addRoute('/rank/:pkg(' + pkgregex + ')'          , pkgRankRoute)
router.addRoute('/download-sum/:pkg(' + pkgregex + ')'  , pkgDownloadSumRoute)
router.addRoute('/download-days/:pkg(' + pkgregex + ')' , pkgDownloadDaysRoute)
router.addRoute('/top'                                  , topDownloadsRoute)
router.addRoute('/total'                                , totalDownloadsRoute)


function handler (req, res) {
  if (req.url == '/_status')
    return sendPlain(req, res, 'OK')

  // unique logger for each request
  req.log = reqLog(uuid.v4())
  req.log.info(req)

  res.setHeader('x-startup', start)
  res.setHeader('x-powered-by', 'whatevs')

  router(req, res)
}


http.createServer(handler)
  .on('error', function onError (err) {
    log.error(err)
    throw err
  })
  .listen(port, function afterListen (err) {
    if (err) {
      log.error(err)
      throw err
    }

    log.info('Server started on port ' + port)
    console.log()
    console.log('>> Running: http://localhost:' + port)
    console.log()
  })


require('./periodic')
