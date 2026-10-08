#!/usr/bin/env bash
# Verify every commons-vfs member this inbound calls exists in the commons-vfs the SERVER runs.
#
# We compile against 2.2.0-wso2v13.20 (provided) but MI 4.1.0 ships
# commons-vfs2_2.2.0.wso2v13_18.jar, and that is the one the OSGi bundle resolves against. Anything
# present in .20 but not _18 compiles cleanly and then throws NoSuchMethodError on every call -
# which is exactly how SftpFileSystemConfigBuilder.setConnectTimeout slipped in.
#
#   usage: tools/check-runtime-api.sh <MI_HOME>
set -u
MI_HOME="${1:?usage: $0 <MI_HOME>}"
RT=$(find "$MI_HOME/wso2/components/plugins" -name "commons-vfs2_*.jar" | head -1)
[ -n "$RT" ] || { echo "no commons-vfs jar under $MI_HOME"; exit 2; }
[ -d target/classes ] || { echo "build first: mvn -Dmaven.repo.local=\$HOME/m2_temp3 clean install -Dmaven.test.skip=true"; exit 2; }
echo "runtime jar: $(basename "$RT")"

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
(cd "$WORK" && unzip -q "$RT")

find target/classes -name "*.class" -exec javap -p -c {} + 2>/dev/null \
 | grep -oE "// (Method|InterfaceMethod) org/apache/commons/vfs2/[A-Za-z0-9/$]+\.[A-Za-z0-9_<>]+:\([^)]*\)[^ ]*" \
 | sed -E 's|// (Method\|InterfaceMethod) ||' | sort -u > "$WORK/calls.txt"
echo "commons-vfs members called: $(wc -l < "$WORK/calls.txt" | tr -d ' ')"

WORK="$WORK" python3 - <<'PY'
import os, re, subprocess, collections
work = os.environ['WORK']
by_class = collections.defaultdict(set)
for line in open(f'{work}/calls.txt'):
    m = re.match(r'^(.*)\.([A-Za-z0-9_<>]+):(\(.*\).*)$', line.strip())
    if m:
        by_class[m.group(1)].add((m.group(2), m.group(3)))

def descriptors(dotted, seen):
    """Declared descriptors plus everything inherited - javap lists only declared members.

    JDK supertypes are resolved too, not skipped: FileName extends Comparable<FileName>, so the
    erased compareTo(Object) we call is inherited from java.lang.Comparable and is always present.
    """
    if dotted in seen:
        return set()
    seen.add(dotted)
    r = subprocess.run(['javap','-p','-s','-classpath',work,dotted], capture_output=True, text=True)
    if r.returncode != 0:
        return None
    out = set(re.findall(r'descriptor:\s*(\S+)', r.stdout))
    head = r.stdout.splitlines()[1] if len(r.stdout.splitlines()) > 1 else ''
    for parent in re.findall(r'(?:extends|implements)\s+([\w.$,<> ]+)', head):
        for p in parent.split(','):
            p = re.sub(r'<.*?>', '', p).strip()
            if p:
                inherited = descriptors(p, seen)
                if inherited:
                    out |= inherited
    return out

optional = set()
try:
    for line in open('tools/optional-runtime-api.txt'):
        line = line.strip()
        if line and not line.startswith('#'):
            optional.add(line)
except FileNotFoundError:
    pass

missing, guarded = [], []
for cls, members in sorted(by_class.items()):
    dotted = cls.replace('/', '.')
    present = descriptors(dotted, set())
    if present is None:
        missing.append(f'{dotted}  <- CLASS NOT IN RUNTIME JAR'); continue
    for name, desc in sorted(members):
        if desc not in present:
            entry = f'{dotted}.{name} {desc}'
            (guarded if entry in optional else missing).append(entry)

for g in guarded:
    print('guarded (absent here, handled in code): ' + g)
if missing:
    print('MISSING AT RUNTIME (would throw NoSuchMethodError / NoClassDefFoundError):')
    for m in missing:
        print('  ' + m)
    raise SystemExit(1)
print('OK - every unguarded commons-vfs member we call exists in the runtime jar')
PY
