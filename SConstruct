
import eol_scons

from SCons.Script import Environment, Export


def labjack(env):
    env.Append(LIBS=['LabJackM'])
    # not actually needed
    # env.Append(CPPPATH=['/usr/local/include'])


Export('labjack')


env = Environment(tools=['default', 'buildmode', 'nidas', 'labjack'])
env.Append(CXXFLAGS=['-std=c++11', '-Wno-deprecated', '-fpic', '-fPIC',
                     '-rdynamic'])
env.Append(LINKFLAGS=['-fpic', '-fPIC', '-rdynamic'])

hotfilm = env.Program('hotfilm.cc')
env.Default(hotfilm)

dest = env.Install("$NIDAS_PATH/bin", hotfilm)
env['SETCAP'] = '/sbin/setcap'

# The cap_net_admin prevents some warning messages about calls to
# cap_set_proc() which technically are not needed to set the scheduling
# priority.
setcap = env.Command('setcap', None,
                     f"$SETCAP cap_net_admin,cap_sys_nice=pe {dest[0]}")
env.AlwaysBuild(setcap)

env.Alias('install.root', [dest, setcap])

env.SetHelp()
