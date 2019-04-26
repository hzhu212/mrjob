# -*- coding: utf-8 -*-

import inspect
import json
import pickle


class _KeyCachingProtocol(object):
    """Protocol that caches the last decoded key."""
    _last_key_encoded = None
    _last_key_decoded = None

    def _loads(self, value):
        """Decode a single key/value, and return it."""
        raise NotImplementedError

    def _dumps(self, value):
        """Encode a single key/value, and return it."""
        raise NotImplementedError

    def read(self, line):
        """Decode a line of input.
        :return: A tuple of ``(key, value)``."""

        raw_key, raw_value = line.split(b'\t', 1)

        if raw_key != self._last_key_encoded:
            self._last_key_encoded = raw_key
            self._last_key_decoded = self._loads(raw_key)
        return (self._last_key_decoded, self._loads(raw_value))

    def write(self, key, value):
        """Encode a key and value.
        :return: A line, without trailing newline."""
        return self._dumps(key) + b'\t' + self._dumps(value)


class JSONProtocol(_KeyCachingProtocol):
    def _loads(self, value):
        return json.loads(value)

    def _dumps(self, value):
        if inspect.isgenerator(value):
            value = list(value)
        return json.dumps(value)


class PickleProtocol(_KeyCachingProtocol):
    def _loads(self, value):
        return pickle.loads(value.decode('string_escape'))

    def _dumps(self, value):
        if inspect.isgenerator(value):
            value = list(value)
        return pickle.dumps(value).encode('string_escape')


class TextProtocol(_KeyCachingProtocol):
    def _loads(self, value):
        return value

    def _dumps(self, value):
        if isinstance(value, basestring):
            return value

        try:
            return '\t'.join(str(x) for x in value)
        except Exception as e:
            pass
            # logger.exception(e)

        return str(value)


class TextValueProtocol(object):
    """Read line (without trailing newline) directly into ``value`` (``key``
    is always ``None``). Output ``value`` (bytes) directly, discarding ``key``.

    **This is the default protocol used by jobs to read input on Python 2.**
    """
    def __init__(self):
        self._id = 1

    def read(self, line):
        # give adjacent lines different ids, so they won't be grouped.
        self._id = (self._id + 1) % 2
        return (self._id, line)

    def write(self, key, value):
        if isinstance(value, str):
            return value
        if isinstance(value, unicode):
            return value.encode('utf8')

        try:
            return b'\t'.join(x.encode('utf8') if isinstance(x, unicode) else str(x) for x in value)
        except Exception as e:
            pass

        return str(value)
