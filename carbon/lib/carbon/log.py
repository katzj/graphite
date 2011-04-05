import time
from sys import stdout, stderr
from twisted.python.log import startLoggingWithObserver, msg, err
from twisted.python.logfile import DailyLogFile

def _safeFormat(fmtString, fmtDict):
    """
    Try to format the string C{fmtString} using C{fmtDict} arguments,
    swallowing all errors to always return a string.
    """
    # There's a way we could make this if not safer at least more
    # informative: perhaps some sort of str/repr wrapper objects
    # could be wrapped around the things inside of C{fmtDict}. That way
    # if the event dict contains an object with a bad __repr__, we
    # can only cry about that individual object instead of the
    # entire event dict.
    try:
        text = fmtString % fmtDict
    except KeyboardInterrupt:
        raise
    except:
        try:
            text = ('Invalid format string or unformattable object in log message: %r, %s' % (fmtString, fmtDict))
        except:
            try:
                text = 'UNFORMATTABLE OBJECT WRITTEN TO LOG with fmt %r, MESSAGE LOST' % (fmtString,)
            except:
                text = 'PATHOLOGICAL ERROR IN BOTH FORMAT STRING AND MESSAGE DETAILS, MESSAGE LOST'
    return text


def textFromEventDict(eventDict):
    """
    Extract text from an event dict passed to a log observer. If it cannot
    handle the dict, it returns None.

    The possible keys of eventDict are:
     - C{message}: by default, it holds the final text. It's required, but can
       be empty if either C{isError} or C{format} is provided (the first
       having the priority).
     - C{isError}: boolean indicating the nature of the event.
     - C{failure}: L{failure.Failure} instance, required if the event is an
       error.
     - C{why}: if defined, used as header of the traceback in case of errors.
     - C{format}: string format used in place of C{message} to customize
       the event. It uses all keys present in C{eventDict} to format
       the text.
    Other keys will be used when applying the C{format}, or ignored.
    """
    edm = eventDict['message']
    if not edm:
        if eventDict['isError'] and 'failure' in eventDict:
            text = ((eventDict.get('why') or 'Unhandled Error')
                    + '\n' + eventDict['failure'].getTraceback())
        elif 'format' in eventDict:
            text = _safeFormat(eventDict['format'], eventDict)
        else:
            # we don't know how to log this
            return
    else:
        text = ' '.join(map(reflect.safe_str, edm))
    return text


def formatEvent(event, includeType=False):
  message = textFromEventDict(event)

  timestamp = time.strftime("%d/%m/%Y %H:%M:%S")

  if includeType:
    typeTag = '[%s] ' % event.get('type', 'console')
  else:
    typeTag = ''

  return "%s :: %s%s" % (timestamp, typeTag, message)


def logToStdout():

  def observer(event):
    stdout.write( formatEvent(event, includeType=True) + '\n' )
    stdout.flush()

  startLoggingWithObserver(observer)


def logToDir(logDir):
  consoleLogFile = DailyLogFile('console.log', logDir)
  customLogs = {}

  def observer(event):
    message = formatEvent(event)
    logType = event.get('type')

    if logType is not None and logType not in customLogs:
      customLogs[logType] = DailyLogFile(logType + '.log', logDir)

    logfile = customLogs.get(logType, consoleLogFile)
    logfile.write(message + '\n')
    logfile.flush()

  startLoggingWithObserver(observer)


def cache(message, **context):
  context['type'] = 'cache'
  msg(message, **context)

def creates(message, **context):
  context['type'] = 'creates'
  msg(message, **context)

def updates(message, **context):
  context['type'] = 'updates'
  msg(message, **context)

def listener(message, **context):
  context['type'] = 'listener'
  msg(message, **context)

def relay(message, **context):
  context['type'] = 'relay'
  msg(message, **context)

def aggregator(message, **context):
  context['type'] = 'aggregator'
  msg(message, **context)

def query(message, **context):
  context['type'] = 'query'
  msg(message, **context)
