
import eol_scons

from SCons.Script import Environment, Export


def labjack(env):
    env.Append(LIBS=['LabJackM'])
    # not actually needed
    # env.Append(CPPPATH=['/usr/local/include'])


Export('labjack')


env = Environment(tools=['default', 'buildmode', 'nidas', 'labjack'])
env.Append(CXXFLAGS=['-std=c++11', '-Wno-deprecated', '-fpic', '-fPIC', '-rdynamic'])
env.Append(LINKFLAGS=['-fpic', '-fPIC', '-rdynamic'])

env.Default(env.Program('hotfilm.cc'))

env.SetHelp()
