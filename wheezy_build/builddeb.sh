#!/bin/bash

if [ "$1" == "UNSIGNED" ] ; then
    vagrant ssh -c 'cd execo.git ; dpkg-buildpackage -us -uc'
else
    GPGKEYID="$1"
    gpg --export-secret-key "$GPGKEYID" | vagrant ssh -c 'gpg --import -'
    vagrant ssh -c 'cd execo.git ; debuild --no-tgz-check -b -i -I -k"$GPGKEYID"'
fi
TMPFILE=$(mktemp)
vagrant ssh-config > "$TMPFILE"
mkdir -p packages/
scp -F "$TMPFILE" 'default:python3-execo*' 'default:python-execo*' packages/
rm "$TMPFILE"
