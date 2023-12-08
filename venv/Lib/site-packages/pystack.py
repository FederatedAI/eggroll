#!/usr/bin/env python

from __future__ import absolute_import, print_function

import os
import sys
import subprocess
import tempfile
import platform
import functools
import codecs
import locale
import distutils.spawn

import click


FILE_OPEN_COMMAND = r'f = open(\"%s\", \"w\")'
FILE_CLOSE_COMMAND = r'f.close()'

UTILITY_COMMANDS = [
    r'itervalues = lambda d:getattr(d, \"itervalues\", d.values)()',
]

GREENLET_STACK_COMMANDS = [
    r'import gc,greenlet,traceback',
    r'objs=[ob for ob in gc.get_objects() if '
    r'isinstance(ob,greenlet.greenlet) if ob]',
    r'f.write(\"\\nDumping Greenlets....\\n\\n\\n\")',
    r'f.write(\"\\n---------------\\n\\n\".join('
    r'\"\".join(traceback.format_stack(o.gr_frame)) for o in objs))',
]

THREAD_STACK_COMMANDS = [
    r'import traceback,sys',
    r'f.write(\"Dumping Threads....\\n\\n\\n\")',
    r'f.write(\"\\n---------------\\n\\n\".join('
    r'\"\".join(traceback.format_stack(o)) for o in '
    r'itervalues(sys._current_frames())))',
]


def make_gdb_args(pid, command):
    statements = [
        r'call (void *) PyGILState_Ensure()',
        r'call (void) PyRun_SimpleString("exec(r\"\"\"%s\"\"\")")' % command,
        r'call (void) PyGILState_Release((void *) $1)',
    ]
    arguments = [find_debugger('gdb'), '-p', str(pid), '-nx', '-batch']
    arguments.extend(['-iex', 'set debuginfod enabled on'])
    arguments.extend("-eval-command=%s" % s for s in statements)
    return arguments


def make_lldb_args(pid, command):
    statements = [
        r'expr void * $gil = (void *) PyGILState_Ensure()',
        r'expr (void) PyRun_SimpleString("exec(r\"\"\"%s\"\"\")")' % command,
        r'expr (void) PyGILState_Release($gil)',
    ]
    arguments = [find_debugger('lldb'), '-p', str(pid), '--batch']
    for s in statements:
        arguments.extend(['--one-line', s])
    return arguments


def find_debugger(name):
    debugger = distutils.spawn.find_executable(name)
    if not debugger:
        raise DebuggerNotFound(
            'Could not find "%s" in your PATH environment variable' % name)
    return debugger


class DebuggerNotFound(Exception):
    pass


def print_stack(pid, include_greenlet=False, debugger=None, verbose=False):
    """Executes a file in a running Python process."""
    # TextIOWrapper of Python 3 is so strange.
    sys_stdout = getattr(sys.stdout, 'buffer', sys.stdout)
    sys_stderr = getattr(sys.stderr, 'buffer', sys.stderr)

    make_args = make_gdb_args
    environ = dict(os.environ)
    if (
        debugger == 'lldb' or
        (debugger is None and platform.system().lower() == 'darwin')
    ):
        make_args = make_lldb_args
        # fix the PATH environment variable for using built-in Python with lldb
        environ['PATH'] = '/usr/bin:%s' % environ.get('PATH', '')

    tmp_fd, tmp_path = tempfile.mkstemp()
    os.chmod(tmp_path, 0o777)
    commands = []
    commands.append(FILE_OPEN_COMMAND)
    commands.extend(UTILITY_COMMANDS)
    commands.extend(THREAD_STACK_COMMANDS)
    if include_greenlet:
        commands.extend(GREENLET_STACK_COMMANDS)
    commands.append(FILE_CLOSE_COMMAND)
    command = r';'.join(commands)

    args = make_args(pid, command % tmp_path)
    process = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    if verbose:
        sys_stderr.write(b'Standard Output:\n%s\n' % out)
        sys_stderr.write(b'Standard Error:\n%s\n' % err)
        sys_stderr.flush()

    for chunk in iter(functools.partial(os.read, tmp_fd, 1024), b''):
        sys_stdout.write(chunk)
    sys_stdout.write(b'\n')
    sys_stdout.flush()


CONTEXT_SETTINGS = {
    'help_option_names': ['-h', '--help'],
}


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('pid', required=True, type=int)
@click.option('--include-greenlet', default=False, is_flag=True,
              help="Also print greenlet stacks")
@click.option('-d', '--debugger', type=click.Choice(['gdb', 'lldb']))
@click.option('-v', '--verbose', default=False, is_flag=True,
              help="Verbosely print error and warnings")
def cli_main(pid, include_greenlet, debugger, verbose):
    '''Print stack of python process.

    $ pystack <pid>
    '''
    try:
        print_stack(pid, include_greenlet, debugger, verbose)
    except DebuggerNotFound as e:
        click.echo('DebuggerNotFound: %s' % e.args[0], err=True)
        click.get_current_context().exit(1)


def tolerate_missing_locale():
    if codecs.lookup(locale.getpreferredencoding()).name != 'ascii':
        return
    # Dear Click, we really don't need any non-ascii output. Please don't
    # crash yourself because you don't like the unicode design of Python 3.
    # (http://click.pocoo.org/5/python3/#python-3-surrogate-handling)
    os.environ.setdefault('LC_ALL', 'C.UTF-8')
    os.environ.setdefault('LANG', 'C.UTF-8')


def main():
    tolerate_missing_locale()
    cli_main()


if __name__ == '__main__':
    main()
