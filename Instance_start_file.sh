#!bin/bash
sudo apt update
sudo apt-get update
sudo apt install python3-pip
sudo pip3 install -q -r requirements.txt
git clone https://github.com/Shoonya-Dev/ShoonyaApi-py.git
git clone -b shoonya https://github.com/rajindersingh041/straddle.git
cd ShoonyaApi-py/dist/
pip3 install NorenRestApiPy-0.0.18-py2.py3-none-any.whl
cd ..
cp ./api_helper.py /home/ubuntu/straddle/
