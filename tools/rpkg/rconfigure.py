import os
import sys
import shutil
import subprocess

extensions = ['parquet']
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'scripts'))
import package_build

def open_utf8(fpath, flags):
    import sys
    if sys.version_info[0] < 3:
        return open(fpath, flags)
    else:
        return open(fpath, flags, encoding="utf8")

extension_list = ""
for ext in extensions:
    extension_list += ' -DBUILD_{}_EXTENSION'.format(ext.upper())
    extension_list += " -DDUCKDB_BUILD_LIBRARY"
    

# check if we are doing a build from an existing DuckDB installation
if 'DUCKDB_R_BINDIR' in os.environ and 'DUCKDB_R_CFLAGS' in os.environ and 'DUCKDB_R_LIBS' in os.environ:
    existing_duckdb_dir = os.environ['DUCKDB_R_BINDIR']
    compile_flags = os.environ['DUCKDB_R_CFLAGS'].replace('\\', '').replace('  ', ' ')
    libraries = [x for x in os.environ['DUCKDB_R_LIBS'].split(' ') if len(x) > 0]

    # use existing installation: set up Makevars
    with open_utf8(os.path.join('src', 'Makevars.in'), 'r') as f:
        text = f.read()

    compile_flags += package_build.include_flags(extensions)
    compile_flags += extension_list

    # find libraries
    result_libs = package_build.get_libraries(existing_duckdb_dir, libraries, extensions)

    link_flags = ''
    for rlib in result_libs:
        libdir = rlib[0]
        libname = rlib[1]
        if libdir != None:
            link_flags += ' -L' + libdir
        if libname != None:
            link_flags += ' -l' + libname

    text = text.replace('{{ SOURCES }}', '')
    text = text.replace('{{ INCLUDES }}', compile_flags.strip())
    text = text.replace('{{ LINK_FLAGS }}', link_flags.strip())

    # now write it to the output Makevars
    with open_utf8(os.path.join('src', 'Makevars'), 'w+') as f:
        f.write(text)
    exit(0)

if not os.path.isfile(os.path.join('..', '..', 'scripts', 'amalgamation.py')):
    print("Could not find amalgamation script! This script needs to be launched from the subdirectory tools/rpkg")
    exit(1)

target_dir = os.path.join(os.getcwd(), 'src', 'duckdb')

linenr = bool(os.getenv("DUCKDB_R_LINENR", ""))

(source_list, include_list, original_sources) = package_build.build_package(target_dir, extensions, linenr)

# object list, relative paths
script_path = os.path.dirname(os.path.abspath(__file__)).replace('\\','/')
duckdb_sources = [package_build.get_relative_path(os.path.join(script_path, 'src'), x) for x in source_list]
object_list = ' '.join([x.rsplit('.', 1)[0] + '.o' for x in duckdb_sources])

# include list
include_list = ' '.join(['-I' + 'duckdb/' + x for x in include_list])
include_list += ' -Iduckdb'
include_list += extension_list

# read Makevars.in and replace the {{ SOURCES }} and {{ INCLUDES }} macros
with open_utf8(os.path.join('src', 'Makevars.in'), 'r') as f:
    text = f.read()

text = text.replace('{{ SOURCES }}', object_list)
text = text.replace('{{ INCLUDES }}', include_list)
text = text.replace('PKG_LIBS={{ LINK_FLAGS }}', '')

# now write it to the output Makevars
with open_utf8(os.path.join('src', 'Makevars'), 'w+') as f:
    f.write(text)
