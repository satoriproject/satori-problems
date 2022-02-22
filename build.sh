#!/bin/bash

echo "#!/usr/bin/env python2" > satori-problems
pushd src
find -name "*.pyc" -type f -exec rm {} +
find -name "__pycache__" -type d -exec rmdir {} +
zip -r9 ../src.zip *
popd
cat src.zip >> satori-problems
rm src.zip
chmod 755 satori-problems

rm -rf build-warsztaty
mkdir build-warsztaty
cp -a src/* build-warsztaty/
cp -a warsztaty/* build-warsztaty/
echo "#!/usr/bin/env python2" > satori-problems-warsztaty
pushd build-warsztaty
zip -r9 ../src.zip *
popd
rm -rf build-warsztaty
cat src.zip >> satori-problems-warsztaty
rm src.zip
chmod 755 satori-problems-warsztaty
