#pragma once
import os
import socket
import numpy

from distutils.core import setup, Extension
from os.path import join as pjoin
from Cython.Distutils import build_ext


myname = socket.getfqdn(socket.gethostname())
myaddr = socket.gethostbyname(myname)
node1 = 'gpua-node1'
node4 = 'node4'

def find_in_path(name, path):
    """Find a file in a search path"""

    # Adapted fom http://code.activestate.com/recipes/52224
    for dir in path.split(os.pathsep):
        binpath = pjoin(dir, name)
        if os.path.exists(binpath):
            return os.path.abspath(binpath)
    return None

def locate_cuda():
    """Locate the CUDA environment on the system
    Returns a dict with keys 'home', 'nvcc', 'include', and 'lib64'
    and values giving the absolute path to each directory.
    Starts by looking for the CUDAHOME env variable. If not found,
    everything is based on finding 'nvcc' in the PATH.
    """

    # First check if the CUDAHOME env variable is in use
    if 'CUDAHOME' in os.environ:
        home = os.environ['CUDAHOME']
        nvcc = pjoin(home, 'bin', 'nvcc')
    else:
        # Otherwise, search the PATH for NVCC
        nvcc = find_in_path('nvcc', os.environ['PATH'])
        if nvcc is None:
            raise EnvironmentError('The nvcc binary could not be '
                'located in your $PATH. Either add it to your path, '
                'or set $CUDAHOME')
        home = os.path.dirname(os.path.dirname(nvcc))

    cudaconfig = {'home': home, 'nvcc': nvcc,
                  'include': pjoin(home, 'include'),
                  'lib64': pjoin(home, 'lib64')}
    for k, v in iter(cudaconfig.items()):
        if not os.path.exists(v):
            raise EnvironmentError('The CUDA %s path could not be '
                                   'located in %s' % (k, v))

    return cudaconfig

def customize_compiler_for_nvcc(self):
    """Inject deep into distutils to customize how the dispatch
    to gcc/nvcc works.
    If you subclass UnixCCompiler, it's not trivial to get your subclass
    injected in, and still have the right customizations (i.e.
    distutils.sysconfig.customize_compiler) run on it. So instead of going
    the OO route, I have this. Note, it's kindof like a wierd functional
    subclassing going on.
    """

    # Tell the compiler it can processes .cu
    self.src_extensions.append('.cu')

    # Save references to the default compiler_so and _comple methods
    default_compiler_so = self.compiler_so
    super = self._compile

    # Now redefine the _compile method. This gets executed for each
    # object but distutils doesn't have the ability to change compilers
    # based on source extension: we add it.
    def _compile(obj, src, ext, cc_args, extra_postargs, pp_opts):
        if os.path.splitext(src)[1] == '.cu':
            # use the cuda for .cu files
            self.set_executable('compiler_so', CUDA['nvcc'])
            # use only a subset of the extra_postargs, which are 1-1
            # translated from the extra_compile_args in the Extension class
            postargs = extra_postargs['nvcc']
        else:
            postargs = extra_postargs['gcc']

        super(obj, src, ext, cc_args, postargs, pp_opts)
        # Reset the default compiler_so, which we might have changed for cuda
        self.compiler_so = default_compiler_so

    # Inject our redefined _compile method into the class
    self._compile = _compile

class custom_build_ext(build_ext):
    def build_extensions(self):
        #self.compiler.compiler_so.remove('-Wstrict-prototypes')
        #self.compiler.compiler_so.remove('-Wsign-compare')
        #self.compiler.compiler_so.remove('-fPIC')
        #self.compiler.compiler_so.remove('-fwrapv')
        #self.compiler.compiler_so.remove('-Wall')
        customize_compiler_for_nvcc(self.compiler)
        build_ext.build_extensions(self)

CUDA = locate_cuda()


print("+++++++++++++++++++++++",CUDA)


print(myname.strip())


if myname.strip()==node1.strip():
    ext = Extension('roll_paillier_tensor',
                    include_dirs = ['/data/czn/code/Python_eggroll_test_with_gpu',
                                    '/data/czn/code/Python_eggroll_test_with_gpu/cuPaillier',
                                    #CGBN
                                    '/data/czn/code/CGBN-source/include',
                                    '/data/czn/code/CGBN-source/samples/utility',
                                    '/data/czn/code/CGBN-source/samples/sample_01_add',
                                    CUDA['include']],
                    library_dirs = ['/data/czn/code/Python_eggroll_test_with_gpu',
                                   CUDA['lib64']],
                    language = 'c',
                     libraries = ['gmp', 'cudart'],
                    runtime_library_dirs = [CUDA['lib64']],
                    sources=['cuPaillier/eggPaillier.cu', 'paillier.c',
                             'pypaillier_module.c' ,'tools.c'],
                    extra_compile_args= {
                        'gcc': ['-w'],
                        'nvcc': [
                            '-arch=sm_70', '--ptxas-options=-v', '-c',
                            '--compiler-options', "'-fPIC'"
                            ]}
                    )


if  myname.strip()==node4.strip():
    ext = Extension('roll_paillier_tensor',
                    include_dirs = ['/data/home/qijunhuang/czn/code/Python_eggroll_test/include',
                                    '/data/home/qijunhuang/czn/code/Python_eggroll_test_with_gpu/cuPaillier',
                                    #CGBN
                                    '/data/home/qijunhuang/czn/code/CGBN-source/include',
                                    '/data/home/qijunhuang/czn/code/CGBN-source/samples/utility',
                                    '/data/home/qijunhuang/czn/code/CGBN-source/samples/sample_01_add',
                                    CUDA['include']],

                    library_dirs = ['/data/home/qijunhuang/czn/code/Python_eggroll_test/lib', 
                                    CUDA['lib64']],
                    language = 'c',
                    libraries = ['gmp', 'cudart'],
                    runtime_library_dirs = [CUDA['lib64']],
                    sources=['cuPaillier/eggPaillier.cu', 'paillier.c',
                             'pypaillier_module.c' ,'tools.c'],
                    extra_compile_args= {
                        'gcc': ['-w'],
                        'nvcc': [
                            '-arch=sm_70', '--ptxas-options=-v', '-c',
                            '--compiler-options', "'-fPIC'"
                            ]}
                    )

    
#os.environ["CC"] = "nvcc" 
#os.environ["CFLAGS"] = "-arch=sm_70 --ptxas-options=-v -c --compiler-options -fPIC"

setup(name='roll_paillier_tensor',
          author = 'Zhennan Chen',
          version = '0.1',
          ext_modules = [ext],
          cmdclass = {'build_ext': custom_build_ext},
          zip_safe = False
)    


