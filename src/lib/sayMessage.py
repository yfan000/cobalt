from ctypes import pythonapi, py_object, CDLL, c_int, c_char_p, c_void_p, POINTER, Structure
import logging
import sys

__all__ = ["isSayMessageLevel", "sayPlainMessage", "sayFormattedMessage", "sayMessage", "sayCatMessage", "setSaySetMessageFile",
           "setSayMessageLevel", "setSayMessageParams", "MESSAGE_ERROR", "MESSAGE_WARNING", "MESSAGE_INFO", "MESSAGE_DEBUG1",
           "MESSAGE_DEBUG2", "MESSAGE_DEBUG3", "CAT_BP_WO_WIRES", "CAT_MEM", "CAT_PARSE_XML", "CAT_RET_CODE", "CAT_COMM",
           "CAT_DB_ACCESS", "CAT_XML_ACCESS", "CAT_DATA_NOT_FOUND", "CAT_SEQUENCE_ERR", "CAT_BAD_ID", "CAT_DUP_DATA",
           "CAT_BG_INFO", "CAT_BAD_INPUT", "CAT_FREE_ERR", "CAT_GENERAL_ERR"]

MESSAGE_ERROR = 0
MESSAGE_WARNING = 1
MESSAGE_INFO = 2
MESSAGE_DEBUG1 = 3
MESSAGE_DEBUG2 = 4
MESSAGE_DEBUG3 = 5

message_type_t = c_int

CAT_BP_WO_WIRES = 0
CAT_MEM = 1
CAT_PARSE_XML = 2
CAT_RET_CODE = 3
CAT_COMM = 4
CAT_DB_ACCESS = 5
CAT_XML_ACCESS = 6
CAT_DATA_NOT_FOUND = 7
CAT_SEQUENCE_ERR = 8
CAT_BAD_ID = 9
CAT_DUP_DATA = 10
CAT_BG_INFO = 11
CAT_BAD_INPUT = 12
CAT_FREE_ERR = 13
CAT_GENERAL_ERR = 14

cat_message_type_t = c_int

try:
    sm_so = CDLL("libsaymessage.so")
    available = True
except OSError:
    sm_so = None
    available = False

def get_message_type(logging_level):
    if logging_level >= logging.ERROR:
        return MESSAGE_ERROR
    elif logging_level >= logging.WARNING:
        return MESSAGE_WARNING
    elif logging_level >= logging.INFO:
        return MESSAGE_INFO
    elif logging_level >= logging.DEBUG:
        return MESSAGE_DEBUG1
    elif logging_level >= 2:
        return MESSAGE_DEBUG2
    else:
        return MESSAGE_DEBUG3

def _message(format, args):
    if len(args) > 0:
        return format % args
    else:
        return format

if available:
    class FILE (Structure):
        pass
    FILE_p = POINTER(FILE)
    
    PyFile_AsFile = pythonapi.PyFile_AsFile
    PyFile_AsFile.restype = FILE_p
    PyFile_AsFile.argtypes = [py_object]

    c_isSayMessageLevel = sm_so.isSayMessageLevel
    c_isSayMessageLevel.restype = c_int
    c_isSayMessageLevel.argtypes = [message_type_t]

    def isSayMessageLevel(level):
        if c_isSayMessageLevel(c_int(level)) != 0:
            return True
        else:
            return False

    c_sayPlainMessage = sm_so.sayPlainMessage
    c_sayPlainMessage.restype = c_int
    c_sayPlainMessage.argtypes = [FILE_p, c_char_p]

    def sayPlainMessage(fo, format, *args):
        if not isinstance(fo, file):
            raise TypeError("argument 0 must be a 'file' not a '%s'" % (type(fo),))
        fp = PyFile_AsFile(fo)
        rc = c_sayPlainMessage(fp, c_char_p(_message(format, args)))
        if rc < 0:
            raise StandardError("failed to write message to file")
        return rc

    c_sayFormattedMessage = sm_so.sayFormattedMessage
    c_sayFormattedMessage.restype = c_int
    c_sayFormattedMessage.argtypes = [FILE_p, c_void_p, c_int]

    def sayFormattedMessage(fo, buf):
        if not isinstance(fo, file):
            raise TypeError("argument 0 must be a 'file' not a '%s'" % (type(fd),))
        fp = PyFile_AsFile(fo)
        rc = c_sayFormattedMessage(fp, void_p(buf), c_int(len(buf)))
        if rc < 0:
            raise StandardError("failed to write message to file")
        return rc

    c_sayMessage = sm_so.sayMessage
    c_sayMessage.restype = None
    c_sayMessage.argtypes = [c_char_p, message_type_t, c_char_p, c_char_p]

    def sayMessage(component, m_type, curr_func, format, *args):
        c_sayMessage(c_char_p(component), message_type_t(m_type), c_char_p(curr_func), c_char_p(_message(format, args)))

    sayCatMessage = sm_so.sayCatMessage
    sayCatMessage.restype = None
    sayCatMessage.argtypes = [c_char_p, cat_message_type_t]

    setSayMessageFile = sm_so.setSayMessageFile
    setSayMessageFile.restype = None
    setSayMessageFile.argtypes = [c_char_p, c_char_p]

    setSayMessageLevel = sm_so.setSayMessageLevel
    setSayMessageLevel.restype = None
    setSayMessageLevel.argtypes = [c_int]

    # NOTE: declared in header, but missing from shared library
    # 
    # closeSayMessageFile = sm_so.closeSayMessageFile
    # closeSayMessageFile.restype = None
    # closeSayMessageFile.argtypes = []

    c_setSayMessageParams = sm_so.setSayMessageParams
    c_setSayMessageParams.restype = None
    c_setSayMessageParams.argtypes = [FILE_p, c_char_p]

    def setSayMessageParams(fo, level):
        if not isinstance(fo, file):
            raise TypeError("argument 0 must be a 'file' not a '%s'" % (type(fo),))
        fp = PyFile_AsFile(fo)
        c_setSayMessageParams(fp, c_int(level))
else:
    from datetime import datetime

    _message_level = MESSAGE_ERROR

    message_type_text = {
        MESSAGE_ERROR : "ERROR",
        MESSAGE_WARNING : "Warning",
        MESSAGE_INFO : "Info",
        MESSAGE_DEBUG1 : "Debug1",
        MESSAGE_DEBUG2 : "Debug2",
        MESSAGE_DEBUG3 : "Debug3",
    }

    def sayMessage(component, m_type, curr_func, format, *args):
        global _message_level
        if m_type > _message_level:
            return
        if curr_func:
            func = "%s - " % (curr_func,)
        else:
            func = ""
        dt = datetime.today()
        print >>sys.stderr, "<%s.%06d> %s (%s): %s%s" % \
            (dt.strftime("%b %d %H:%M%S"), dt.microsecond, component, message_type_text[m_type], func, _message(format, args))

    def setSayMessageLevel(level):
        global _message_level
        _message_level = level
