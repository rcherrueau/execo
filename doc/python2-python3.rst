**************************************************************
Writing code compatible with python 2.6, 2.7 and 3.2+ in execo
**************************************************************

Here are some guidelines for execo contributors, for writing code
compatible with python 2.6, 2.7 and 3.2+ in execo:

- As always, do *NOT* try to optimize some code for python2 versus
  python3 (eg. by using different codes on python2 and python3),
  unless you are pretty sure that it is useful.

- Use python3 print function, import it from __future__ if necessary
  (anyway, in execo, there should not be much print, since we use the
  logger).

- Do not use modules from the python2 standard library that are
  deprecated or missing from python3. Do not use modules from the
  python3 standard library that are missing in python2.

- Use explicit relative imports
  (https://www.python.org/dev/peps/pep-0328).

- For catching exceptions, use syntax ``except xx as yy:`` instead of
  ``except xx, yy``. For raising exceptions, use syntax ``raise
  xx('yy')`` instead of ``raise xx, 'yy'``.

- Use legacy string interpolation (using ``%``).

- For checking if dict has key, use ``'name' in kwargs`` instead of
  ``kwargs.has_key('name')``.

- Use dict ``keys()``, ``values()``, ``items()`` instead of
  ``iterkeys()``, ``itervalues()``, ``iteritems()``. Beware, they
  return lists under python2, views under python3.

- Use `execo.utils.is_string` to check reliably if object is a string.

- Explicitly use integer division ``//`` or floating point division
  ``/`` where needed. Ensure floating point division is used under
  python2 by casting operands.

- Do not use ``object.__cmp__()``. Do not use the ``cmp`` kwarg from
  ``sort`` or ``sorted``.

- Replace ``obj.next()`` with ``next(obj)``.

- Do not use ``map()``, ``filter()``, ``reduce()``. Use list
  comprehensions instead.

- Do not use dict comprehensions, set comprehensions, set literals.

- Use new style octal literals (eg. ``0o720`` instead of ``0720``).

- Use only new-style classes (deriving from ``object``)

- Use ``!=`` for testing difference, not ``<>``.
    
- When absolutely needed, use version specific sections with this kind
  of code::
  
   if sys.version_info >= (3,):
       import queue, _thread
   else:
       import Queue as queue, thread as _thread

- More informations can be found here:
  https://docs.python.org/3/whatsnew/3.0.html
